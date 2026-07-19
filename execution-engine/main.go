// Package main is the entry point for the AIEN Execution Engine (Worker Daemon).
//
// WHAT THE EXECUTION ENGINE DOES
// =============================
// The Execution Engine is the "muscle" of the platform. It is responsible for:
// 1. Pulling scheduled tasks from NATS JetStream (`tasks.scheduled.*`).
// 2. Setting the intent status in the database to `EXECUTING` (acquiring lock).
// 3. Parsing the intent payload and executing it:
//    - For TRANSFER: Execute transfer rules and log the balance adjustments.
//    - For COMPUTE: Execute CPU/IO tasks.
// 4. Writing execution logs and updating final status (`COMPLETED` or `FAILED`).
// 5. Acknowledging (Ack) the task in NATS JetStream.
//
// GO CONCURRENCY PATTERN: Worker Pool
// ===================================
// Rather than processing tasks sequentially, the engine spawns multiple concurrent
// worker goroutines (configured by WORKER_POOL_SIZE). They share the same NATS
// subscription, letting Go's scheduler and NATS manage high-throughput processing.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aien-platform/aien/shared/config"
	"github.com/aien-platform/aien/shared/db"
	"github.com/aien-platform/aien/shared/logger"
	"github.com/aien-platform/aien/shared/metrics"

	intentv1 "github.com/aien-platform/aien/proto/intent/v1"
	walletv1 "github.com/aien-platform/aien/proto/wallet/v1"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

var (
	tasksExecutedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aien_execution_engine_tasks_executed_total",
			Help: "Total number of tasks executed by the engine.",
		},
		[]string{"type", "status"},
	)
	taskExecutionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aien_execution_engine_task_duration_seconds",
			Help:    "Latency of task executions in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)
	workerThreadsActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "aien_execution_engine_active_workers",
			Help: "Number of worker threads currently processing a task.",
		},
	)
)

func init() {
	prometheus.MustRegister(tasksExecutedTotal)
	prometheus.MustRegister(taskExecutionDuration)
	prometheus.MustRegister(workerThreadsActive)
}

func main() {
	log := logger.New("execution-engine")
	log.Info("Starting AIEN Execution Engine...")

	// 1. Load configuration
	databaseURL := config.GetEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/aien?sslmode=disable")
	natsURL := config.GetEnv("NATS_URL", "nats://localhost:4222")
	poolSizeStr := config.GetEnv("WORKER_POOL_SIZE", "3")
	metricsPort := config.GetEnv("METRICS_PORT", "8085")

	poolSize, err := strconv.Atoi(poolSizeStr)
	if err != nil || poolSize <= 0 {
		poolSize = 3
	}

	log.Info("Loaded configuration parameters",
		"pool_size", poolSize,
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
	metrics.MonitorDatabasePool(dbConn, "execution_engine", 5*time.Second, log)

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

	log.Info("Worker pool initialized", "concurrency", poolSize)

	// Connect to Wallet Service via secure gRPC over mTLS
	walletServiceAddr := config.GetEnv("WALLET_SERVICE_ADDR", "localhost:50052")
	caCertPath := config.GetEnv("TLS_CA_CERT", "")
	clientCertPath := config.GetEnv("TLS_CLIENT_CERT", "")
	clientKeyPath := config.GetEnv("TLS_CLIENT_KEY", "")

	var clientOpts []grpc.DialOption

	if caCertPath != "" && clientCertPath != "" && clientKeyPath != "" {
		log.Info("Configuring Wallet gRPC client with Mutual TLS (mTLS)...", "addr", walletServiceAddr)

		clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
		if err != nil {
			log.Error("Failed to load client TLS key pair for Wallet client", "cert", clientCertPath, "key", clientKeyPath, "error", err)
			os.Exit(1)
		}

		caCertBytes, err := os.ReadFile(caCertPath)
		if err != nil {
			log.Error("Failed to read CA cert file for Wallet client", "path", caCertPath, "error", err)
			os.Exit(1)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCertBytes) {
			log.Error("Failed to append CA certificate to pool for Wallet client")
			os.Exit(1)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      caCertPool,
			ServerName:   "wallet", // Matches CN/SAN in regenerated server cert
		}

		creds := credentials.NewTLS(tlsConfig)
		clientOpts = append(clientOpts, grpc.WithTransportCredentials(creds))
		log.Info("mTLS successfully configured for Wallet gRPC client.")
	} else {
		log.Warn("TLS environment variables not set; starting insecure Wallet gRPC client.")
		clientOpts = append(clientOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	walletConn, err := grpc.NewClient(walletServiceAddr, clientOpts...)
	if err != nil {
		log.Error("Failed to connect to Wallet Service", "addr", walletServiceAddr, "error", err)
		os.Exit(1)
	}
	defer walletConn.Close()

	walletClient := walletv1.NewWalletServiceClient(walletConn)
	log.Info("Connected to Wallet Service", "addr", walletServiceAddr)

	// 4. Create channels to distribute tasks to workers.
	// We use a buffered channel matching the worker pool size.
	taskChan := make(chan *nats.Msg, poolSize)

	// Spawning worker goroutines.
	for i := 1; i <= poolSize; i++ {
		go startWorker(i, taskChan, dbConn, js, walletClient, log)
	}

	// 5. Subscribe to Scheduled Tasks using a Queue Group.
	// NATS handles load balancing; only one worker instance gets each task.
	sub, err := js.QueueSubscribe(
		"tasks.scheduled.*",
		"execution-engine-group",
		func(m *nats.Msg) {
			// Send message to worker pool channel.
			// This blocks if all workers are currently busy,
			// causing NATS to backpressure naturally.
			taskChan <- m
		},
		nats.Durable("execution-engine"),
		nats.ManualAck(),
	)
	if err != nil {
		log.Error("Failed to subscribe to tasks stream", "error", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	log.Info("Execution Engine successfully subscribed to tasks.scheduled.* events")

	// 6. Graceful Shutdown Signal Handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info("Shutting down Execution Engine...")
	close(taskChan) // Workers terminate when channel closes.
}

// startWorker runs a continuous loop executing tasks sent on the taskChan.
func startWorker(workerID int, taskChan <-chan *nats.Msg, dbConn *sql.DB, js nats.JetStreamContext, walletClient walletv1.WalletServiceClient, log *slog.Logger) {
	wLog := log.With("worker_id", workerID)
	wLog.Debug("Worker started")

	for msg := range taskChan {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		
		// Process task
		err := executeTask(ctx, msg, dbConn, js, walletClient, wLog)
		cancel()

		if err != nil {
			wLog.Error("Task execution failed", "subject", msg.Subject, "error", err)
			// Negative ACK tells NATS to redeliver this task.
			msg.Nak()
		} else {
			// Acknowledge task has been executed successfully.
			msg.Ack()
		}
	}
	wLog.Debug("Worker stopped")
}

// executeTask parses the NATS message, updates DB state, runs task logic, and commits updates.
func executeTask(ctx context.Context, m *nats.Msg, dbConn *sql.DB, js nats.JetStreamContext, walletClient walletv1.WalletServiceClient, log *slog.Logger) error {
	workerThreadsActive.Inc()
	defer workerThreadsActive.Dec()

	// Step 1: Unmarshal task payload
	var intent intentv1.Intent
	if err := proto.Unmarshal(m.Data, &intent); err != nil {
		return fmt.Errorf("failed to unmarshal task payload: %w", err)
	}

	start := time.Now()
	defer func() {
		taskExecutionDuration.WithLabelValues(intent.Type.String()).Observe(time.Since(start).Seconds())
	}()

	log.Info("Worker starting task execution", "id", intent.Id, "type", intent.Type.String())

	// Step 2: Transition DB status to EXECUTING.
	// This acts as a state lock: other operations can see this intent is active.
	updateQuery := "UPDATE intents SET status = $1, updated_at = NOW() WHERE id = $2 AND status = $3"
	res, err := dbConn.ExecContext(ctx, updateQuery, "INTENT_STATUS_EXECUTING", intent.Id, "INTENT_STATUS_SCHEDULED")
	if err != nil {
		return fmt.Errorf("failed to transition status to EXECUTING: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil || rows == 0 {
		log.Warn("Intent was already claimed or updated, skipping", "id", intent.Id)
		return nil // Return nil so NATS marks this message as acknowledged.
	}

	// Step 3: Transition/Execute transaction using wallet client
	executionErr := runExecutionSimulation(ctx, &intent, walletClient, log)

	// Step 4: Write result to database
	finalStatus := "INTENT_STATUS_COMPLETED"
	var protoFinalStatus intentv1.IntentStatus = intentv1.IntentStatus_INTENT_STATUS_COMPLETED
	if executionErr != nil {
		log.Error("Simulation execution failed", "id", intent.Id, "error", executionErr)
		finalStatus = "INTENT_STATUS_FAILED"
		protoFinalStatus = intentv1.IntentStatus_INTENT_STATUS_FAILED
	}

	finalUpdateQuery := "UPDATE intents SET status = $1, updated_at = NOW() WHERE id = $2"
	_, err = dbConn.ExecContext(ctx, finalUpdateQuery, finalStatus, intent.Id)
	if err != nil {
		return fmt.Errorf("failed to write final status: %w", err)
	}

	// Step 5: Publish execution completed event to NATS
	// This triggers the Validator microservice to write to the WAL Ledger.
	intent.Status = protoFinalStatus
	executedData, err := proto.Marshal(&intent)
	if err != nil {
		log.Error("Failed to marshal executed intent to protobuf", "id", intent.Id, "error", err)
	} else {
		targetSubject := fmt.Sprintf("intents.executed.%s", intent.Type.String())
		_, pubErr := js.PublishMsg(&nats.Msg{
			Subject: targetSubject,
			Data:    executedData,
		}, nats.Context(ctx))
		if pubErr != nil {
			log.Error("Failed to publish executed intent event to NATS", "id", intent.Id, "error", pubErr)
			// Note: We don't rollback the database transaction here since status is already written.
			// The auditing/validation step is decoupled. In a perfect setup, we'd handle this as a separate transaction.
		} else {
			log.Debug("Published execution completed event to NATS", "id", intent.Id, "subject", targetSubject)
		}
	}

	tasksExecutedTotal.WithLabelValues(intent.Type.String(), finalStatus).Inc()
	log.Info("Worker finished task execution", "id", intent.Id, "final_status", finalStatus)
	return nil
}

// runExecutionSimulation simulates execution work based on the intent category.
func runExecutionSimulation(ctx context.Context, intent *intentv1.Intent, walletClient walletv1.WalletServiceClient, log *slog.Logger) error {
	switch intent.Type {
	case intentv1.IntentType_INTENT_TYPE_TRANSFER:
		// Parse transfer details (expect JSON payload).
		var transfer struct {
			From   string  `json:"from"`
			To     string  `json:"to"`
			Amount float64 `json:"amount"`
		}
		if err := json.Unmarshal(intent.Payload, &transfer); err != nil {
			return fmt.Errorf("failed to parse transfer payload as JSON: %w", err)
		}

		log.Info("💰 [TRANSFER EXECUTION] Invoking Wallet Service gRPC Transfer",
			"from", transfer.From,
			"to", transfer.To,
			"amount", transfer.Amount,
			"reference_id", intent.Id,
		)

		// Invoke secure gRPC mTLS transfer
		_, err := walletClient.Transfer(ctx, &walletv1.TransferRequest{
			FromAccountId: transfer.From,
			ToAccountId:   transfer.To,
			Amount:        transfer.Amount,
			ReferenceId:   intent.Id,
		})
		if err != nil {
			return fmt.Errorf("wallet transfer failed: %w", err)
		}

		log.Info("💰 [TRANSFER EXECUTION] Wallet transfer completed successfully", "id", intent.Id)

	case intentv1.IntentType_INTENT_TYPE_COMPUTE:
		log.Info("🖥️ [COMPUTE EXECUTION] Running intensive computation job...", "payload", string(intent.Payload))
		// Simulate heavy mathematical calculation latency.
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}

	default:
		log.Warn("Unknown intent type for execution, executing as no-op", "type", intent.Type.String())
	}

	return nil
}
