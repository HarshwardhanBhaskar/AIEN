// Package logger provides a thin, structured logging wrapper for AIEN services.
//
// WHY A CUSTOM LOGGER PACKAGE?
// ============================
// Go's standard library "log" package outputs unstructured text like:
//   2024/01/15 10:30:00 Starting server on port 8081
//
// In a distributed system with dozens of services, unstructured logs are
// nearly impossible to search, filter, or correlate. Structured logging
// outputs key-value pairs that tools like Grafana Loki, ELK, or even
// simple grep can parse:
//   level=INFO service=gateway msg="Starting server" port=8081
//
// WHY slog?
// =========
// Go 1.21+ includes "log/slog" in the standard library. Before slog,
// teams used third-party libraries like Zap (Uber) or Logrus. slog
// provides a standard interface that's:
// 1. Zero-dependency (stdlib)
// 2. High-performance (allocation-free in hot paths)
// 3. Pluggable (swap text/JSON handlers without changing call sites)
//
// DESIGN DECISION: We start with slog's TextHandler for human-readable
// development output. In production, we'll switch to JSONHandler so log
// aggregation tools can parse the output without custom parsers.
package logger

import (
	"log/slog"
	"os"
)

// New creates a structured logger for the given service.
//
// The service name is automatically attached to every log entry,
// making it trivial to filter logs in a multi-service environment:
//   grep "service=intent-service" combined.log
//
// Parameters:
//   - service: the name of the microservice (e.g., "gateway", "intent-service").
//
// Returns:
//   - *slog.Logger: a configured structured logger.
func New(service string) *slog.Logger {
	// TextHandler outputs human-readable structured logs.
	// JSONHandler would be used for production (machine-parseable).
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		// DEBUG level in development; INFO in production.
		Level: slog.LevelDebug,
	})

	// .With() attaches constant fields to every log entry from this logger.
	// This means every line will include service=<name> automatically.
	return slog.New(handler).With("service", service)
}
