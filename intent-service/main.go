// Package main is the entry point for the AIEN Intent Service.
//
// WHAT THIS SERVICE DOES
// ======================
// The Intent Service is the core microservice responsible for:
// 1. Receiving intents via gRPC from the API Gateway.
// 2. Validating intent structure and constraints.
// 3. Persisting intents to PostgreSQL.
// 4. Publishing events to NATS JetStream for asynchronous execution.
// 5. Returning intent status to callers.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aien-platform/aien/intent-service/handler"
	"github.com/aien-platform/aien/intent-service/outbox"
	"github.com/aien-platform/aien/intent-service/store"
	"github.com/aien-platform/aien/shared/config"
	"github.com/aien-platform/aien/shared/db"
	"github.com/aien-platform/aien/shared/logger"
	"github.com/aien-platform/aien/shared/metrics"

	intentv1 "github.com/aien-platform/aien/proto/intent/v1"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

var (
	grpcRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aien_intent_service_grpc_requests_total",
			Help: "Total number of gRPC requests handled by Intent Service.",
		},
		[]string{"method", "status"},
	)
	grpcRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aien_intent_service_grpc_request_duration_seconds",
			Help:    "Duration of gRPC requests in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)
)

func init() {
	prometheus.MustRegister(grpcRequestsTotal)
	prometheus.MustRegister(grpcRequestDuration)
}

// metricsInterceptor is a gRPC UnaryServerInterceptor that collects execution metrics.
func metricsInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	duration := time.Since(start)

	statusStr := "OK"
	if err != nil {
		if st, ok := status.FromError(err); ok {
			statusStr = st.Code().String()
		} else {
			statusStr = "UNKNOWN"
		}
	}

	grpcRequestsTotal.WithLabelValues(info.FullMethod, statusStr).Inc()
	grpcRequestDuration.WithLabelValues(info.FullMethod).Observe(duration.Seconds())

	return resp, err
}

func main() {
	// 1. Initialize structured logger.
	log := logger.New("intent-service")
	log.Info("Starting Intent Service in Milestone 2 environment...")

	// 2. Load configurations from environment variables.
	grpcPort := config.GetEnv("INTENT_SERVICE_PORT", ":50051")
	metricsPort := config.GetEnv("METRICS_PORT", "8082")
	databaseURL := config.GetEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/aien?sslmode=disable")
	natsURL := config.GetEnv("NATS_URL", "nats://localhost:4222")
	schemaPath := config.GetEnv("SCHEMA_PATH", "store/schema.sql")

	log.Info("Loaded configuration parameters",
		"port", grpcPort,
		"metrics_port", metricsPort,
		"db_configured", databaseURL != "",
		"nats_url", natsURL,
	)

	// Start metrics sidecar server
	metrics.StartServer(metricsPort, log)

	// 3. Initialize PostgreSQL connection pool with connection retry logic.
	//
	// Retries: 10 times, waiting 3 seconds between each.
	// This gives Postgres 30 seconds to wake up inside Docker Compose.
	dbConn, err := db.ConnectPostgres(databaseURL, 10, 3*time.Second, log)
	if err != nil {
		log.Error("Database connection failed", "error", err)
		os.Exit(1)
	}
	defer dbConn.Close()

	// Start monitoring database pool metrics
	metrics.MonitorDatabasePool(dbConn, "intent_service", 5*time.Second, log)

	// 4. Initialize PostgresStore and execute automatic DDL schema migrations.
	postgresStore := store.NewPostgresStore(dbConn)
	if err := postgresStore.Migrate(schemaPath); err != nil {
		log.Error("Failed to apply database migrations", "error", err)
		os.Exit(1)
	}
	log.Info("Database migrations applied successfully")

	// 5. Connect to NATS JetStream event broker.
	//
	// Connection Options:
	// - nats.MaxReconnects: Retry connection indefinitely if NATS drops.
	// - nats.ReconnectWait: Wait 2s between reconnection attempts.
	log.Info("Connecting to NATS event broker...", "url", natsURL)
	nc, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		log.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	// 6. Create JetStream context.
	// JetStream is a built-in engine inside NATS adding log-based streaming,
	// message persistence, and consumer tracking (like Kafka).
	js, err := nc.JetStream()
	if err != nil {
		log.Error("Failed to initialize JetStream context", "error", err)
		os.Exit(1)
	}

	// 7. Ensure NATS stream for intents exists.
	// We bind the stream to subjects matching "intents.submitted.*" (e.g. intents.submitted.TRANSFER).
	streamName := "INTENTS"
	streamConfig := &nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{"intents.submitted.*"},
		// RetentionPolicy: nats.LimitsPolicy ensures old messages are pruned
		// based on size or age limits.
		Retention: nats.LimitsPolicy,
	}

	// AddStream will create the stream or update it if already exists.
	_, err = js.AddStream(streamConfig)
	if err != nil {
		log.Error("Failed to create NATS JetStream stream", "stream", streamName, "error", err)
		os.Exit(1)
	}
	log.Info("NATS JetStream stream initialized", "stream", streamName)

	// 8. Initialize and start the Transactional Outbox Worker.
	outboxWorker := outbox.NewWorker(dbConn, js, log, 250*time.Millisecond, 50, 5)
	outboxWorker.Start()

	// 9. Initialize gRPC Handler with dependencies (decoupled from direct publishing).
	intentHandler := handler.New(postgresStore, log)

	// 10. Start TCP listener for gRPC.
	listener, err := net.Listen("tcp", grpcPort)
	if err != nil {
		log.Error("Failed to listen for TCP", "port", grpcPort, "error", err)
		outboxWorker.Stop()
		os.Exit(1)
	}

	// Load TLS credentials if environment variables are provided
	caCertPath := config.GetEnv("TLS_CA_CERT", "")
	serverCertPath := config.GetEnv("TLS_SERVER_CERT", "")
	serverKeyPath := config.GetEnv("TLS_SERVER_KEY", "")

	var serverOpts []grpc.ServerOption
	serverOpts = append(serverOpts, grpc.UnaryInterceptor(metricsInterceptor))

	if caCertPath != "" && serverCertPath != "" && serverKeyPath != "" {
		log.Info("Configuring gRPC server with Mutual TLS (mTLS)...")
		
		// Load server certificate and key
		serverCert, err := tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
		if err != nil {
			log.Error("Failed to load server TLS key pair", "cert", serverCertPath, "key", serverKeyPath, "error", err)
			outboxWorker.Stop()
			os.Exit(1)
		}

		// Load CA cert to verify client certs
		caCertBytes, err := os.ReadFile(caCertPath)
		if err != nil {
			log.Error("Failed to read CA cert file", "path", caCertPath, "error", err)
			outboxWorker.Stop()
			os.Exit(1)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCertBytes) {
			log.Error("Failed to append CA certificate to pool")
			outboxWorker.Stop()
			os.Exit(1)
		}

		// Configure TLS to require client certificates signed by our CA
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    caCertPool,
		}

		creds := credentials.NewTLS(tlsConfig)
		serverOpts = append(serverOpts, grpc.Creds(creds))
		log.Info("mTLS successfully configured for gRPC server.")
	} else {
		log.Warn("TLS environment variables not set; starting insecure gRPC server.")
	}

	// 11. Create and configure gRPC Server.
	grpcServer := grpc.NewServer(serverOpts...)
	intentv1.RegisterIntentServiceServer(grpcServer, intentHandler)
	reflection.Register(grpcServer)

	// 12. Graceful Shutdown Signal Handler.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Info("Received shutdown signal", "signal", sig.String())
		grpcServer.GracefulStop()
		outboxWorker.Stop()
	}()

	// 13. Start serving traffic.
	log.Info("gRPC server is running", "port", grpcPort)
	if err := grpcServer.Serve(listener); err != nil {
		log.Error("gRPC server failed", "error", err)
		os.Exit(1)
	}

	log.Info("Intent Service stopped gracefully")
}
