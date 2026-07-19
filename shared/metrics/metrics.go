package metrics

import (
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartServer spins up a background HTTP server on the specified port
// exposing the standard `/metrics` endpoint for Prometheus scraping.
func StartServer(port string, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("Starting metrics HTTP server", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("Metrics HTTP server failed", "error", err)
		}
	}()
}

// MonitorDatabasePool registers database stats collectors to export
// Go's standard sql.DBStats metrics to Prometheus.
func MonitorDatabasePool(db *sql.DB, serviceName string, interval time.Duration, log *slog.Logger) {
	// Define Prometheus Gauges for connection pool statistics
	dbOpenConnections := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: fmt.Sprintf("aien_%s_db_connections_open", serviceName),
		Help: "The number of established connections both in use and idle.",
	})
	dbInUseConnections := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: fmt.Sprintf("aien_%s_db_connections_in_use", serviceName),
		Help: "The number of connections currently in use.",
	})
	dbIdleConnections := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: fmt.Sprintf("aien_%s_db_connections_idle", serviceName),
		Help: "The number of idle connections.",
	})
	dbWaitCount := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: fmt.Sprintf("aien_%s_db_connections_wait_count_total", serviceName),
		Help: "The total number of connections waited for.",
	})

	// Register collectors
	prometheus.MustRegister(dbOpenConnections)
	prometheus.MustRegister(dbInUseConnections)
	prometheus.MustRegister(dbIdleConnections)
	prometheus.MustRegister(dbWaitCount)

	// Spawn collector tick loop
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			stats := db.Stats()
			dbOpenConnections.Set(float64(stats.OpenConnections))
			dbInUseConnections.Set(float64(stats.InUse))
			dbIdleConnections.Set(float64(stats.Idle))
			dbWaitCount.Set(float64(stats.WaitCount))
		}
	}()
	log.Info("Database connection pool metrics monitoring started", "service", serviceName)
}
