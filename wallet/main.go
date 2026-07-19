package main

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	walletv1 "github.com/aien-platform/aien/proto/wallet/v1"
	"github.com/aien-platform/aien/shared/config"
	"github.com/aien-platform/aien/shared/db"
	"github.com/aien-platform/aien/shared/logger"
	"github.com/aien-platform/aien/wallet/handler"
	"github.com/aien-platform/aien/wallet/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

func main() {
	log := logger.New("wallet-service")
	log.Info("Starting Wallet Service...")

	// 1. Load Configurations
	port := config.GetEnv("WALLET_SERVICE_PORT", ":50052")
	databaseURL := config.GetEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/aien?sslmode=disable")
	schemaPath := config.GetEnv("SCHEMA_PATH", "store/schema.sql")

	log.Info("Loaded configuration parameters",
		"port", port,
		"db_configured", databaseURL != "",
		"schema_path", schemaPath,
	)

	// 2. Connect to PostgreSQL with retry logic
	dbConn, err := db.ConnectPostgres(databaseURL, 10, 3*time.Second, log)
	if err != nil {
		log.Error("Database connection failed", "error", err)
		os.Exit(1)
	}
	defer dbConn.Close()

	// 3. Initialize PostgresStore and execute migrations
	postgresStore := store.NewPostgresStore(dbConn)
	if err := postgresStore.Migrate(schemaPath); err != nil {
		log.Error("Failed to apply database migrations", "error", err)
		os.Exit(1)
	}
	log.Info("Database migrations applied successfully")

	// 4. Set up TCP listener
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Error("Failed to listen on port", "port", port, "error", err)
		os.Exit(1)
	}

	// 5. Configure gRPC mTLS Server Options
	caCertPath := config.GetEnv("TLS_CA_CERT", "")
	serverCertPath := config.GetEnv("TLS_SERVER_CERT", "")
	serverKeyPath := config.GetEnv("TLS_SERVER_KEY", "")

	var serverOpts []grpc.ServerOption

	if caCertPath != "" && serverCertPath != "" && serverKeyPath != "" {
		log.Info("Configuring gRPC server with Mutual TLS (mTLS)...")

		serverCert, err := tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
		if err != nil {
			log.Error("Failed to load server TLS key pair", "cert", serverCertPath, "key", serverKeyPath, "error", err)
			os.Exit(1)
		}

		caCertBytes, err := os.ReadFile(caCertPath)
		if err != nil {
			log.Error("Failed to read CA cert file", "path", caCertPath, "error", err)
			os.Exit(1)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCertBytes) {
			log.Error("Failed to append CA certificate to pool")
			os.Exit(1)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    caCertPool,
		}

		creds := credentials.NewTLS(tlsConfig)
		serverOpts = append(serverOpts, grpc.Creds(creds))
		log.Info("mTLS successfully configured for Wallet gRPC server.")
	} else {
		log.Warn("TLS environment variables not set; starting insecure Wallet gRPC server.")
	}

	// 6. Create and Register gRPC Server
	grpcServer := grpc.NewServer(serverOpts...)
	walletHandler := handler.New(postgresStore, log)
	walletv1.RegisterWalletServiceServer(grpcServer, walletHandler)
	reflection.Register(grpcServer)

	// 7. Graceful Shutdown Signal Handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Info("Received shutdown signal", "signal", sig.String())
		grpcServer.GracefulStop()
	}()

	// 8. Start serving
	log.Info("Wallet gRPC server is running", "port", port)
	if err := grpcServer.Serve(listener); err != nil {
		log.Error("gRPC server failed", "error", err)
		os.Exit(1)
	}

	log.Info("Wallet Service stopped gracefully")
}
