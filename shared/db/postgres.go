// Package db provides helper utilities for database connection management.
//
// WHY RETRY DATABASE CONNECTIONS?
// ===============================
// In a distributed containerized system, containers are started in parallel.
// Even though Docker Compose allows specifying dependencies (using "depends_on"),
// Postgres might take 5-10 seconds to initialize its internal storage, write lock
// files, and start listening on port 5432.
//
// If a Go service tries to connect immediately, the connection fails. If the service
// simply exits, the container crashes. To prevent this, we write a retry loop
// with linear backoff. The service will wait for the database to become ready
// instead of crashing.
//
// WHY database/sql AND NOT AN ORM?
// ================================
// Object-Relational Mappers (ORMs) like GORM or Ent are popular in Go, but:
// 1. Complexity: They hide SQL, making it harder for a learning engineer to understand
//    exactly what queries are run, what indexes are used, and how transactions work.
// 2. Performance: ORMs introduce runtime reflection overhead and complex generated queries.
// 3. Control: For high-performance systems (like an execution ledger), we need exact
//    control over queries, locks (e.g., SELECT FOR UPDATE), and transactions.
//
// Go's standard library "database/sql" is extremely powerful, safe (prevents SQL injection
// via prepared statements), and idiomatic. We pair it with a driver for PostgreSQL ("lib/pq").
package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	// The driver registers itself with database/sql using its init() function.
	// We import it with a blank identifier ("_") because we don't call its
	// functions directly; we only use database/sql APIs.
	_ "github.com/lib/pq"
)

// ConnectPostgres initializes a connection pool to a PostgreSQL instance.
//
// It attempts to ping the database, retrying up to maxRetries times with
// the specified interval.
//
// Parameters:
//   - dsn: Data Source Name connection string (e.g., "postgres://user:pass@host:5432/db?sslmode=disable").
//   - maxRetries: number of times to retry if the connection fails.
//   - retryInterval: time to wait between retries.
//   - log: structured logger.
//
// Returns:
//   - *sql.DB: the active database connection pool.
//   - error: if the database could not be reached after all retries.
func ConnectPostgres(dsn string, maxRetries int, retryInterval time.Duration, log *slog.Logger) (*sql.DB, error) {
	var db *sql.DB
	var err error

	for i := 1; i <= maxRetries; i++ {
		log.Info("Attempting to connect to PostgreSQL...", "attempt", i, "max_attempts", maxRetries)

		// sql.Open doesn't actually establish a connection to the database.
		// It just validates the DSN format and initializes the driver structure.
		db, err = sql.Open("postgres", dsn)
		if err == nil {
			// To verify the database is actually reachable and accepting connections,
			// we must Ping it.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = db.PingContext(ctx)
			cancel()

			if err == nil {
				log.Info("Successfully connected to PostgreSQL")
				
				// Configure Connection Pool settings.
				//
				// MaxOpenConns: Limits concurrent connections to avoid overwhelming Postgres.
				// MaxIdleConns: Keeps a pool of idle connections open for reuse, avoiding TCP handshake costs.
				// ConnMaxLifetime: Closes old connections to release database memory resources.
				db.SetMaxOpenConns(10)
				db.SetMaxIdleConns(5)
				db.SetConnMaxLifetime(5 * time.Minute)

				return db, nil
			}

			// Close the connection pool if ping failed to prevent descriptor/socket leak
			db.Close()
		}

		log.Warn("PostgreSQL connection attempt failed", "attempt", i, "error", err.Error())
		if i < maxRetries {
			time.Sleep(retryInterval)
		}
	}

	return nil, fmt.Errorf("failed to connect to PostgreSQL after %d attempts: %w", maxRetries, err)
}
