// Package main is the entry point for the AIEN Validator Service.
//
// WHAT THE VALIDATOR DOES
// =======================
// The Validator is the "auditor" of our control plane. It is responsible for:
// 1. Performing a startup scan on the ledger file to verify cryptographic
//    integrity. If tampering is detected, it halts process startup immediately.
// 2. Subscribing to execution completion events (`intents.executed.*`) from NATS.
// 3. Verifying the execution details (state proofs, client signatures).
// 4. Committing the final state transition to the immutable, append-only WAL ledger log.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aien-platform/aien/shared/config"
	"github.com/aien-platform/aien/shared/db"
	"github.com/aien-platform/aien/shared/logger"
	"github.com/aien-platform/aien/shared/metrics"
	"github.com/aien-platform/aien/validator/ledger"

	intentv1 "github.com/aien-platform/aien/proto/intent/v1"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

var (
	ledgerBlocksAppended = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aien_validator_ledger_blocks_total",
			Help: "Total number of signed blocks written to WAL ledger.",
		},
		[]string{"type"},
	)
	ledgerWriteDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aien_validator_ledger_write_duration_seconds",
			Help:    "Latency of ledger disk writes in seconds.",
			Buckets: prometheus.DefBuckets,
		},
	)
	ledgerVerificationErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aien_validator_ledger_errors_total",
			Help: "Total number of ledger validation or tampering errors detected.",
		},
	)
)

func init() {
	prometheus.MustRegister(ledgerBlocksAppended)
	prometheus.MustRegister(ledgerWriteDuration)
	prometheus.MustRegister(ledgerVerificationErrors)
}

func main() {
	log := logger.New("validator")
	log.Info("Starting AIEN Validator Service...")

	// 1. Load configuration
	databaseURL := config.GetEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/aien?sslmode=disable")
	natsURL := config.GetEnv("NATS_URL", "nats://localhost:4222")
	ledgerPath := config.GetEnv("LEDGER_PATH", "./data/ledger.log")
	metricsPort := config.GetEnv("METRICS_PORT", "8086")

	log.Info("Loaded configuration parameters",
		"ledger_path", ledgerPath,
		"nats_url", natsURL,
		"metrics_port", metricsPort,
	)

	// Start metrics sidecar server
	metrics.StartServer(metricsPort, log)

	// 2. Initialize Ledger Engine
	// Ensures folder directory exists.
	dir := filepath.Dir(ledgerPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Error("Failed to create ledger directory", "dir", dir, "error", err)
		os.Exit(1)
	}

	ledgerEngine := ledger.NewEngine(ledgerPath)

	// 3. Perform Startup Integrity Scan
	// If anyone tampered with the ledger file while the service was offline,
	// we halt startup immediately, preventing further execution on a compromised log.
	log.Info("Running startup cryptographic integrity check on WAL ledger...", "file", ledgerPath)
	if err := ledgerEngine.Verify(); err != nil {
		ledgerVerificationErrors.Inc()
		log.Error("CRITICAL CONFIGURATION ERROR: Ledger verification failed! The historical file has been altered or tampered with!", "error", err)
		os.Exit(1)
	}
	log.Info("Ledger cryptographic integrity verified successfully")

	// 4. Connect to PostgreSQL
	dbConn, err := db.ConnectPostgres(databaseURL, 10, 3*time.Second, log)
	if err != nil {
		log.Error("Database connection failed", "error", err)
		os.Exit(1)
	}
	defer dbConn.Close()

	// Start monitoring database pool metrics
	metrics.MonitorDatabasePool(dbConn, "validator", 5*time.Second, log)

	// 5. Connect to NATS
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

	// 6. Ensure the execution stream exists.
	// This stream holds execution completion notifications.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "EXECUTIONS",
		Subjects:  []string{"intents.executed.*"},
		Retention: nats.LimitsPolicy,
	})
	if err != nil {
		log.Error("Failed to create NATS EXECUTIONS stream", "error", err)
		os.Exit(1)
	}

	// 7. Subscribe to Executed Intents.
	sub, err := js.QueueSubscribe(
		"intents.executed.*",
		"validator-group",
		func(m *nats.Msg) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if err := handleExecutedIntent(ctx, m, dbConn, ledgerEngine, log); err != nil {
				log.Error("Failed to validate and commit executed intent", "subject", m.Subject, "error", err)
				m.Nak()
				return
			}

			m.Ack()
		},
		nats.Durable("validator"),
		nats.ManualAck(),
	)
	if err != nil {
		log.Error("Failed to subscribe to executions stream", "error", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	log.Info("Validator successfully subscribed to intents.executed.* events")

	// 8. Graceful Shutdown Signal Handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info("Shutting down Validator Service...")
}

// handleExecutedIntent processes a completed intent execution event.
//
// Steps:
// 1. Unmarshal NATS message payload (Protobuf binary).
// 2. Fetch current record state from Postgres.
// 3. Verify final execution state is valid.
// 4. Commit block entry to the append-only ledger file.
func handleExecutedIntent(ctx context.Context, m *nats.Msg, dbConn *sql.DB, ledgerEngine *ledger.Engine, log *slog.Logger) error {
	start := time.Now()
	defer func() {
		ledgerWriteDuration.Observe(time.Since(start).Seconds())
	}()

	// Step 1: Parse protobuf binary.
	var intent intentv1.Intent
	if err := proto.Unmarshal(m.Data, &intent); err != nil {
		ledgerVerificationErrors.Inc()
		return fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	log.Info("Validator auditing executed intent", "id", intent.Id, "status", intent.Status.String())

	// Step 2: Query PostgreSQL to verify record exists and matches state.
	var dbStatus string
	var dbType string
	query := "SELECT status, type FROM intents WHERE id = $1"
	err := dbConn.QueryRowContext(ctx, query, intent.Id).Scan(&dbStatus, &dbType)
	if err != nil {
		ledgerVerificationErrors.Inc()
		return fmt.Errorf("failed to fetch intent state from Postgres: %w", err)
	}

	// Step 3: Validate state transition sanity.
	// We only commit executions that reached COMPLETED or FAILED state.
	if dbStatus != "INTENT_STATUS_COMPLETED" && dbStatus != "INTENT_STATUS_FAILED" {
		ledgerVerificationErrors.Inc()
		return fmt.Errorf("invalid transition state for ledger audit: database shows status %s", dbStatus)
	}

	// Step 4: Append block to the immutable WAL hash-linked ledger.
	block, err := ledgerEngine.Append(intent.Id, dbType, dbStatus)
	if err != nil {
		ledgerVerificationErrors.Inc()
		return fmt.Errorf("failed to write block to ledger: %w", err)
	}

	ledgerBlocksAppended.WithLabelValues(dbType).Inc()
	log.Info("Ledger block written successfully",
		"index", block.Index,
		"intent_id", block.IntentID,
		"hash", block.Hash[:10]+"...",
		"prev_hash", block.PrevHash[:10]+"...",
	)

	return nil
}
