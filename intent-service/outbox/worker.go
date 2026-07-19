package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// OutboxEvent represents a single event fetched from the database outbox table.
type OutboxEvent struct {
	ID         string
	Topic      string
	Payload    []byte
	RetryCount int
}

// OutboxWorker manages the background polling loop that processes outbox events.
type OutboxWorker struct {
	db           *sql.DB
	js           nats.JetStreamContext
	logger       *slog.Logger
	pollInterval time.Duration
	batchSize    int
	maxRetries   int
	stopChan     chan struct{}
	wg           sync.WaitGroup
}

// NewWorker creates a new OutboxWorker instance.
func NewWorker(db *sql.DB, js nats.JetStreamContext, logger *slog.Logger, pollInterval time.Duration, batchSize int, maxRetries int) *OutboxWorker {
	if pollInterval <= 0 {
		pollInterval = 250 * time.Millisecond
	}
	if batchSize <= 0 {
		batchSize = 50
	}
	if maxRetries <= 0 {
		maxRetries = 5
	}
	return &OutboxWorker{
		db:           db,
		js:           js,
		logger:       logger.With("component", "outbox-worker"),
		pollInterval: pollInterval,
		batchSize:    batchSize,
		maxRetries:   maxRetries,
		stopChan:     make(chan struct{}),
	}
}

// Start spawns the worker polling loop in a background goroutine.
func (w *OutboxWorker) Start() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.logger.Info("Outbox worker daemon started", "poll_interval", w.pollInterval, "batch_size", w.batchSize)

		for {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			if err := w.processBatch(ctx); err != nil {
				w.logger.Error("Error processing outbox batch", "error", err)
			}
			cancel()

			select {
			case <-w.stopChan:
				w.logger.Info("Outbox worker loop stopping...")
				return
			case <-time.After(w.pollInterval):
			}
		}
	}()
}

// Stop signals the worker loop to shutdown and waits for it to exit gracefully.
func (w *OutboxWorker) Stop() {
	close(w.stopChan)
	w.wg.Wait()
	w.logger.Info("Outbox worker daemon stopped successfully")
}

// processBatch polls the database for events, publishes them to NATS, and updates/deletes them.
func (w *OutboxWorker) processBatch(ctx context.Context) error {
	// 1. Claim pending outbox events inside a transaction
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin batch transaction: %w", err)
	}
	defer tx.Rollback()

	// Select PENDING events OR events stuck in PROCESSING for more than 1 minute.
	// We use FOR UPDATE SKIP LOCKED to support safe parallel consumption by multiple replicas.
	query := `
		SELECT id, topic, payload, retry_count
		FROM outbox_events
		WHERE status = 'PENDING' OR (status = 'PROCESSING' AND updated_at < $1)
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`
	stuckThreshold := time.Now().Add(-1 * time.Minute)
	rows, err := tx.QueryContext(ctx, query, stuckThreshold, w.batchSize)
	if err != nil {
		return fmt.Errorf("failed to query outbox events: %w", err)
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		var ev OutboxEvent
		if err := rows.Scan(&ev.ID, &ev.Topic, &ev.Payload, &ev.RetryCount); err != nil {
			return fmt.Errorf("failed to scan outbox row: %w", err)
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("outbox rows error: %w", err)
	}

	if len(events) == 0 {
		return nil // No events to process
	}

	w.logger.Debug("Claimed outbox events for publishing", "count", len(events))

	// 2. Transition claimed events status to PROCESSING so they are skipped by other workers
	updateQuery := `
		UPDATE outbox_events
		SET status = 'PROCESSING', updated_at = NOW()
		WHERE id = $1
	`
	for _, ev := range events {
		_, err := tx.ExecContext(ctx, updateQuery, ev.ID)
		if err != nil {
			return fmt.Errorf("failed to update outbox event to PROCESSING: %w", err)
		}
	}

	// Commit database transaction to release row locks and save the status state.
	// This ensures we do not block database connection pools while calling the NATS broker network.
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit claim transaction: %w", err)
	}

	// 3. Process and publish events to NATS JetStream
	for _, ev := range events {
		w.logger.Debug("Publishing event from outbox", "id", ev.ID, "topic", ev.Topic)

		// Decouple publishing timeout so we fail fast (2s) instead of blocking the worker thread
		pubCtx, pubCancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := w.js.PublishMsg(&nats.Msg{
			Subject: ev.Topic,
			Data:    ev.Payload,
		}, nats.Context(pubCtx))
		pubCancel()

		if err != nil {
			w.logger.Error("Failed to publish event to NATS JetStream", "id", ev.ID, "topic", ev.Topic, "error", err)

			newStatus := "PENDING"
			if ev.RetryCount >= w.maxRetries {
				newStatus = "FAILED"
				w.logger.Error("Max retry attempts reached for outbox event; marked as FAILED", "id", ev.ID, "topic", ev.Topic)
			}

			// Use a fresh context for database updates to ensure recovery succeeds even if parent context timed out
			dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
			if updateErr := w.updateEventStatus(dbCtx, ev.ID, newStatus, ev.RetryCount+1); updateErr != nil {
				w.logger.Error("Failed to update status on publish failure", "id", ev.ID, "error", updateErr)
			}
			dbCancel()
		} else {
			w.logger.Debug("Published event to NATS; deleting from outbox table", "id", ev.ID)

			// Use a fresh context for database delete to prevent block on parent context timeouts
			dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
			if deleteErr := w.deleteEvent(dbCtx, ev.ID); deleteErr != nil {
				w.logger.Error("Failed to delete event after success publish", "id", ev.ID, "error", deleteErr)
			}
			dbCancel()
		}
	}

	return nil
}

func (w *OutboxWorker) updateEventStatus(ctx context.Context, id string, status string, retryCount int) error {
	query := `
		UPDATE outbox_events
		SET status = $1, retry_count = $2, updated_at = NOW()
		WHERE id = $3
	`
	_, err := w.db.ExecContext(ctx, query, status, retryCount, id)
	return err
}

func (w *OutboxWorker) deleteEvent(ctx context.Context, id string) error {
	query := `
		DELETE FROM outbox_events
		WHERE id = $1
	`
	_, err := w.db.ExecContext(ctx, query, id)
	return err
}
