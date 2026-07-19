// Package main is the entry point for the AIEN Scheduler Service.
//
// WHAT THE SCHEDULER DOES
// =======================
// The Scheduler is the "brain" of the control plane. It is responsible for:
// 1. Watching NATS JetStream for new intent submission events (`intents.submitted.*`).
// 2. Fetching intent records from the PostgreSQL database to verify their state.
// 3. Running scheduling algorithms:
//    - Enforcing deadlines: If the intent has expired, reject it.
//    - Choosing execution queues: Directing tasks to NATS queues based on priority/type.
// 4. Updating intent status in the database to `SCHEDULED`.
// 5. Publishing the scheduled task event (`tasks.scheduled.<type>`).
//
// CONCURRENCY & SCALING (NATS Queue Groups)
// ==========================================
// To horizontally scale the control plane, we run multiple Scheduler instances.
// We use a NATS Queue Group ("scheduler-group") so NATS distributes incoming events
// round-robin. Only ONE scheduler instance receives any given event, preventing
// double-scheduling without requiring distributed locks.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aien-platform/aien/shared/config"
	"github.com/aien-platform/aien/shared/db"
	"github.com/aien-platform/aien/shared/logger"
	"github.com/aien-platform/aien/shared/metrics"

	intentv1 "github.com/aien-platform/aien/proto/intent/v1"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

var (
	intentsScheduledTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aien_scheduler_intents_scheduled_total",
			Help: "Total number of intents successfully scheduled.",
		},
		[]string{"type"},
	)
	intentsRejectedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aien_scheduler_intents_rejected_total",
			Help: "Total number of intents rejected (e.g. deadline expired).",
		},
		[]string{"type", "reason"},
	)
	schedulingDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aien_scheduler_scheduling_duration_seconds",
			Help:    "Duration of intent scheduling operations in seconds.",
			Buckets: prometheus.DefBuckets,
		},
	)
)

func init() {
	prometheus.MustRegister(intentsScheduledTotal)
	prometheus.MustRegister(intentsRejectedTotal)
	prometheus.MustRegister(schedulingDuration)
}

func main() {
	log := logger.New("scheduler")
	log.Info("Starting AIEN Scheduler Service...")

	// 1. Load configuration
	databaseURL := config.GetEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/aien?sslmode=disable")
	natsURL := config.GetEnv("NATS_URL", "nats://localhost:4222")
	metricsPort := config.GetEnv("METRICS_PORT", "8084")

	log.Info("Loaded configuration parameters",
		"database_url_configured", databaseURL != "",
		"nats_url", natsURL,
		"metrics_port", metricsPort,
	)

	// Start metrics sidecar server
	metrics.StartServer(metricsPort, log)

	// 2. Connect to PostgreSQL
	dbConn, err := db.ConnectPostgres(databaseURL, 10, 3*time.Second, log)
	if err != nil {
		log.Error("Database connection failed", "error", err)
		os.Exit(1)
	}
	defer dbConn.Close()

	// Start monitoring database pool metrics
	metrics.MonitorDatabasePool(dbConn, "scheduler", 5*time.Second, log)

	// 3. Connect to NATS
	nc, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		log.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Error("Failed to initialize JetStream context", "error", err)
		os.Exit(1)
	}

	// 4. Ensure the TASKS stream exists.
	// This is the stream where the scheduler publishes scheduled tasks for workers to pull.
	streamName := "TASKS"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{"tasks.scheduled.*"},
		Retention: nats.LimitsPolicy,
	})
	if err != nil {
		log.Error("Failed to create NATS TASKS stream", "error", err)
		os.Exit(1)
	}
	log.Info("NATS JetStream stream verified", "stream", streamName)

	// 5. Subscribe to Submitted Intents.
	//
	// QueueSubscribe:
	// - Subject: "intents.submitted.*"
	// - Queue group: "scheduler-group" (round-robin dispatching)
	// - Durable: "scheduler" (resumes where it left off on restart)
	// - ManualAck: We must call m.Ack() once the database write + NATS publish succeed.
	sub, err := js.QueueSubscribe(
		"intents.submitted.*",
		"scheduler-group",
		func(m *nats.Msg) {
			// Process each intent event concurrently or sequentially.
			// For simplicity and sequencing, we handle it sequentially here.
			// At scale, you would pass this to a worker pool (Go channel).
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if err := handleSubmittedIntent(ctx, m, dbConn, js, log); err != nil {
				log.Error("Failed to schedule intent", "subject", m.Subject, "error", err)
				// NATS JetStream will redeliver the message later because we didn't call Ack().
				// We can call Nak() (Negative Ack) to request immediate redelivery.
				m.Nak()
				return
			}

			// Acknowledge the message so NATS knows it's processed.
			m.Ack()
		},
		nats.Durable("scheduler"),
		nats.ManualAck(),
	)
	if err != nil {
		log.Error("Failed to subscribe to intents stream", "error", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	log.Info("Scheduler successfully subscribed to intents.submitted.* events")

	// 6. Graceful Shutdown Signal Handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info("Shutting down Scheduler Service...")
}

// handleSubmittedIntent handles a single incoming intent submission event.
//
// Algorithmic Flow:
// 1. Unmarshal Protobuf message.
// 2. Fetch current status from Postgres (verifies DB state matches event).
// 3. Check deadlines: If deadline is exceeded, transition to FAILED.
// 4. Update status in Database to SCHEDULED.
// 5. Publish task message to tasks.scheduled.<type> subject.
func handleSubmittedIntent(ctx context.Context, m *nats.Msg, dbConn *sql.DB, js nats.JetStreamContext, log *slog.Logger) error {
	start := time.Now()
	defer func() {
		schedulingDuration.Observe(time.Since(start).Seconds())
	}()

	// Step 1: Unmarshal the intent from protobuf binary
	var intent intentv1.Intent
	if err := proto.Unmarshal(m.Data, &intent); err != nil {
		return fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	log.Info("Scheduler processing intent", "id", intent.Id, "type", intent.Type.String())

	// Step 2: Fetch current status and constraints from database.
	// In a concurrent system, DB state is the source of truth; NATS events are notifications.
	var dbStatus string
	var deadline sql.NullTime
	var priority int

	query := "SELECT status, deadline, priority FROM intents WHERE id = $1"
	err := dbConn.QueryRowContext(ctx, query, intent.Id).Scan(&dbStatus, &deadline, &priority)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Warn("Intent not found in database (possibly deleted)", "id", intent.Id)
			return nil // Return nil so NATS discards this orphan message.
		}
		return fmt.Errorf("database query failed: %w", err)
	}

	// If intent is already scheduled, executing, or completed, skip.
	if dbStatus != "INTENT_STATUS_PENDING" {
		if dbStatus == "INTENT_STATUS_SCHEDULED" {
			// If it's already scheduled, we might have crashed before publishing to NATS.
			// Re-publish the NATS task just in case!
			log.Info("Intent already scheduled, re-publishing NATS task to ensure delivery", "id", intent.Id)
			intent.Status = intentv1.IntentStatus_INTENT_STATUS_SCHEDULED
			taskData, err := proto.Marshal(&intent)
			if err == nil {
				targetSubject := fmt.Sprintf("tasks.scheduled.%s", intent.Type.String())
				_, _ = js.PublishMsg(&nats.Msg{
					Subject: targetSubject,
					Data:    taskData,
				}, nats.Context(ctx))
			}
		}
		log.Warn("Intent in non-pending state, skipping scheduling", "id", intent.Id, "status", dbStatus)
		return nil
	}

	// Step 3: Check deadline constraint
	if deadline.Valid && time.Now().After(deadline.Time) {
		log.Warn("Intent deadline exceeded, marking as FAILED", "id", intent.Id, "deadline", deadline.Time)

		// Transition status to FAILED.
		updateQuery := "UPDATE intents SET status = $1, updated_at = NOW() WHERE id = $2"
		_, updateErr := dbConn.ExecContext(ctx, updateQuery, "INTENT_STATUS_FAILED", intent.Id)
		if updateErr != nil {
			return fmt.Errorf("failed to update expired intent status: %w", updateErr)
		}
		intentsRejectedTotal.WithLabelValues(intent.Type.String(), "deadline_exceeded").Inc()
		return nil // Successfully handled (rejected).
	}

	// Step 4: Update status in database to SCHEDULED
	tx, err := dbConn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Safe to defer; no-op if transaction committed.

	updateQuery := "UPDATE intents SET status = $1, updated_at = NOW() WHERE id = $2"
	_, err = tx.ExecContext(ctx, updateQuery, "INTENT_STATUS_SCHEDULED", intent.Id)
	if err != nil {
		return fmt.Errorf("failed to update status to SCHEDULED: %w", err)
	}

	// Commit database transaction first before publishing message to NATS.
	// This prevents the execution worker from receiving the message before the
	// database updates become visible, avoiding a race condition.
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Step 5: Publish scheduled task to NATS
	intent.Status = intentv1.IntentStatus_INTENT_STATUS_SCHEDULED
	taskData, err := proto.Marshal(&intent)
	if err != nil {
		return fmt.Errorf("failed to marshal task data: %w", err)
	}

	targetSubject := fmt.Sprintf("tasks.scheduled.%s", intent.Type.String())

	_, err = js.PublishMsg(&nats.Msg{
		Subject: targetSubject,
		Data:    taskData,
	}, nats.Context(ctx))
	if err != nil {
		return fmt.Errorf("failed to publish scheduled task to NATS: %w", err)
	}

	intentsScheduledTotal.WithLabelValues(intent.Type.String()).Inc()
	log.Info("Successfully scheduled intent", "id", intent.Id, "queue", targetSubject)
	return nil
}
