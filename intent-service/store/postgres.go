// Package store provides storage backends for intents.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aien-platform/aien/shared/ids"

	intentv1 "github.com/aien-platform/aien/proto/intent/v1"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PostgresStore implements the Store interface using PostgreSQL.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore creates a new PostgresStore using the provided db connection pool.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// Migrate executes the schema.sql file to initialize database tables.
//
// WHY AUTOMATIC MIGRATIONS ON STARTUP?
// ====================================
// For a production system, you usually use migrations run by a CI/CD pipeline
// or dedicated schema management system (like Liquibase or golang-migrate).
// But for development, having the application verify and create its own tables
// simplifies local setup dramatically.
func (p *PostgresStore) Migrate(schemaPath string) error {
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("failed to read schema file: %w", err)
	}

	_, err = p.db.Exec(string(schemaBytes))
	if err != nil {
		return fmt.Errorf("failed to execute schema: %w", err)
	}

	return nil
}

// Create inserts a new intent into PostgreSQL.
//
// Prepared Statements:
// database/sql automatically prepares statements behind the scenes when we use
// ExecContext or QueryContext with arguments ($1, $2, etc.). This protects
// the application against SQL injection attacks by escaping inputs.
func (p *PostgresStore) Create(ctx context.Context, req *intentv1.SubmitIntentRequest) (*intentv1.Intent, error) {
	// Generate UUID v7 time-ordered ID.
	id := ids.NewIntentID()
	now := time.Now()

	// Default status when submitted is PENDING.
	statusStr := intentv1.IntentStatus_INTENT_STATUS_PENDING.String()
	typeStr := req.Type.String()

	// Extract constraints if they exist.
	var priority int32 = 5
	var maxRetries int32 = 3
	var idempotent bool = false
	var deadline sql.NullTime

	if req.Constraints != nil {
		priority = req.Constraints.Priority
		maxRetries = req.Constraints.MaxRetries
		idempotent = req.Constraints.Idempotent
		if req.Constraints.Deadline != nil {
			deadline = sql.NullTime{
				Time:  req.Constraints.Deadline.AsTime(),
				Valid: true,
			}
		}
	}

	// Begin SQL Transaction to guarantee atomic double-write
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Safe to defer; no-op if transaction committed.

	queryIntents := `
		INSERT INTO intents (
			id, type, status, submitter_id, payload,
			priority, max_retries, idempotent, deadline, signature,
			submitter_public_key, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12, $13
		)
	`

	_, err = tx.ExecContext(
		ctx, queryIntents,
		id, typeStr, statusStr, req.SubmitterId, req.Payload,
		priority, maxRetries, idempotent, deadline, req.Signature,
		req.SubmitterPublicKey, now, now,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert intent: %w", err)
	}

	// Build the created model to serialize.
	intent := &intentv1.Intent{
		Id:                 id,
		Type:               req.Type,
		Status:             intentv1.IntentStatus_INTENT_STATUS_PENDING,
		SubmitterId:        req.SubmitterId,
		Payload:            req.Payload,
		Constraints: &intentv1.IntentConstraints{
			Priority:   priority,
			MaxRetries: maxRetries,
			Idempotent: idempotent,
		},
		Signature:          req.Signature,
		SubmitterPublicKey: req.SubmitterPublicKey,
		CreatedAt:          timestamppb.New(now),
		UpdatedAt:          timestamppb.New(now),
	}

	if deadline.Valid {
		intent.Constraints.Deadline = timestamppb.New(deadline.Time)
	}

	// Serialize intent protobuf binary for outbox payload
	eventData, err := proto.Marshal(intent)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal outbox event payload: %w", err)
	}

	outboxQuery := `
		INSERT INTO outbox_events (
			id, topic, payload, status, retry_count, created_at, updated_at
		) VALUES (
			$1, $2, $3, 'PENDING', 0, $4, $4
		)
	`
	outboxID := ids.NewIntentID()

	_, err = tx.ExecContext(
		ctx, outboxQuery,
		outboxID, fmt.Sprintf("intents.submitted.%s", typeStr), eventData, now,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert outbox event: %w", err)
	}

	// Commit Transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return intent, nil
}

// Get retrieves a single intent by ID.
//
// Row Scanning:
// Scan parses database values into Go types. Database types like TIMESTAMP
// WITH TIME ZONE map to Go time.Time. We use helper mappings for nullable columns.
func (p *PostgresStore) Get(ctx context.Context, id string) (*intentv1.Intent, error) {
	query := `
		SELECT 
			id, type, status, submitter_id, payload,
			priority, max_retries, idempotent, deadline, signature,
			submitter_public_key, created_at, updated_at
		FROM intents
		WHERE id = $1
	`

	row := p.db.QueryRowContext(ctx, query, id)

	var typeStr, statusStr string
	var intentId string
	var deadline sql.NullTime
	var createdAt, updatedAt time.Time

	intent := &intentv1.Intent{
		Constraints: &intentv1.IntentConstraints{},
	}

	err := row.Scan(
		&intentId, &typeStr, &statusStr, &intent.SubmitterId, &intent.Payload,
		&intent.Constraints.Priority, &intent.Constraints.MaxRetries, &intent.Constraints.Idempotent,
		&deadline, &intent.Signature, &intent.SubmitterPublicKey, &createdAt, &updatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("intent not found: %s", id)
		}
		return nil, fmt.Errorf("failed to scan intent: %w", err)
	}

	intent.Id = intentId
	intent.Type = parseIntentType(typeStr)
	intent.Status = parseIntentStatus(statusStr)
	intent.CreatedAt = timestamppb.New(createdAt)
	intent.UpdatedAt = timestamppb.New(updatedAt)

	if deadline.Valid {
		intent.Constraints.Deadline = timestamppb.New(deadline.Time)
	}

	return intent, nil
}

// List queries and returns a paginated list of intents.
//
// SQL PAGINATION TECHNIQUE: Keysets (Cursor-based)
// ===============================================
// In MemoryStore, we used simple offset-based pagination.
// For databases, offset pagination (LIMIT X OFFSET Y) is inefficient:
// PostgreSQL must scan and count Y rows before discarding them, leading to
// O(N) performance as pages grow deeper.
//
// For this PostgreSQL store, we implement cursor-based keyset pagination:
//   WHERE created_at < $pageToken (or id < $pageToken)
// This lets Postgres jump straight to the records using the index, keeping
// pagination performance O(log N) regardless of offset depth.
//
// For Phase 1, to maintain compatibility with list token format, we will implement
// filtering by created_at timestamp passed in the token.
func (p *PostgresStore) List(ctx context.Context, statusFilter intentv1.IntentStatus, submitterFilter string, pageSize int, pageToken string) ([]*intentv1.Intent, string, int, error) {
	if pageSize <= 0 {
		pageSize = 20
	}

	// Step 1: Query total matching count.
	// This helps the UI render total pages or list statistics.
	countQuery := "SELECT COUNT(*) FROM intents WHERE 1=1"
	var countArgs []interface{}
	argCount := 1

	if statusFilter != intentv1.IntentStatus_INTENT_STATUS_UNSPECIFIED {
		countQuery += fmt.Sprintf(" AND status = $%d", argCount)
		countArgs = append(countArgs, statusFilter.String())
		argCount++
	}
	if submitterFilter != "" {
		countQuery += fmt.Sprintf(" AND submitter_id = $%d", argCount)
		countArgs = append(countArgs, submitterFilter)
		argCount++
	}

	var totalCount int
	err := p.db.QueryRowContext(ctx, countQuery, countArgs...).Scan(&totalCount)
	if err != nil {
		return nil, "", 0, fmt.Errorf("failed to count intents: %w", err)
	}

	// Step 2: Fetch paginated intents.
	query := `
		SELECT 
			id, type, status, submitter_id, payload,
			priority, max_retries, idempotent, deadline, signature,
			submitter_public_key, created_at, updated_at
		FROM intents
		WHERE 1=1
	`
	var queryArgs []interface{}
	argIdx := 1

	if statusFilter != intentv1.IntentStatus_INTENT_STATUS_UNSPECIFIED {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		queryArgs = append(queryArgs, statusFilter.String())
		argIdx++
	}
	if submitterFilter != "" {
		query += fmt.Sprintf(" AND submitter_id = $%d", argIdx)
		queryArgs = append(queryArgs, submitterFilter)
		argIdx++
	}

	// Keyset Pagination Filter.
	// If a page token (timestamp) is passed, we only fetch records older than that timestamp.
	if pageToken != "" {
		cursorTime, parseErr := time.Parse(time.RFC3339Nano, pageToken)
		if parseErr == nil {
			query += fmt.Sprintf(" AND created_at < $%d", argIdx)
			queryArgs = append(queryArgs, cursorTime)
			argIdx++
		}
	}

	// Sort order is descending (newest first).
	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d", argIdx)
	queryArgs = append(queryArgs, pageSize)

	rows, err := p.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, "", 0, fmt.Errorf("failed to query intents: %w", err)
	}
	defer rows.Close()

	var results []*intentv1.Intent
	var lastCreatedAt time.Time

	for rows.Next() {
		var typeStr, statusStr string
		var intentId string
		var deadline sql.NullTime
		var createdAt, updatedAt time.Time

		intent := &intentv1.Intent{
			Constraints: &intentv1.IntentConstraints{},
		}

		err = rows.Scan(
			&intentId, &typeStr, &statusStr, &intent.SubmitterId, &intent.Payload,
			&intent.Constraints.Priority, &intent.Constraints.MaxRetries, &intent.Constraints.Idempotent,
			&deadline, &intent.Signature, &intent.SubmitterPublicKey, &createdAt, &updatedAt,
		)
		if err != nil {
			return nil, "", 0, fmt.Errorf("failed to scan row: %w", err)
		}

		intent.Id = intentId
		intent.Type = parseIntentType(typeStr)
		intent.Status = parseIntentStatus(statusStr)
		intent.CreatedAt = timestamppb.New(createdAt)
		intent.UpdatedAt = timestamppb.New(updatedAt)
		lastCreatedAt = createdAt

		if deadline.Valid {
			intent.Constraints.Deadline = timestamppb.New(deadline.Time)
		}

		results = append(results, intent)
	}

	if err = rows.Err(); err != nil {
		return nil, "", 0, fmt.Errorf("rows iteration error: %w", err)
	}

	// Generate next page token (last item's timestamp).
	nextPageToken := ""
	if len(results) == pageSize && !lastCreatedAt.IsZero() {
		nextPageToken = lastCreatedAt.Format(time.RFC3339Nano)
	}

	return results, nextPageToken, totalCount, nil
}

// Helpers to map string representations from database back to protobuf enums.

func parseIntentType(s string) intentv1.IntentType {
	switch s {
	case "INTENT_TYPE_TRANSFER":
		return intentv1.IntentType_INTENT_TYPE_TRANSFER
	case "INTENT_TYPE_COMPUTE":
		return intentv1.IntentType_INTENT_TYPE_COMPUTE
	case "INTENT_TYPE_QUERY":
		return intentv1.IntentType_INTENT_TYPE_QUERY
	case "INTENT_TYPE_SCHEDULED":
		return intentv1.IntentType_INTENT_TYPE_SCHEDULED
	default:
		return intentv1.IntentType_INTENT_TYPE_UNSPECIFIED
	}
}

func parseIntentStatus(s string) intentv1.IntentStatus {
	switch s {
	case "INTENT_STATUS_PENDING":
		return intentv1.IntentStatus_INTENT_STATUS_PENDING
	case "INTENT_STATUS_VALIDATED":
		return intentv1.IntentStatus_INTENT_STATUS_VALIDATED
	case "INTENT_STATUS_SCHEDULED":
		return intentv1.IntentStatus_INTENT_STATUS_SCHEDULED
	case "INTENT_STATUS_EXECUTING":
		return intentv1.IntentStatus_INTENT_STATUS_EXECUTING
	case "INTENT_STATUS_COMPLETED":
		return intentv1.IntentStatus_INTENT_STATUS_COMPLETED
	case "INTENT_STATUS_FAILED":
		return intentv1.IntentStatus_INTENT_STATUS_FAILED
	case "INTENT_STATUS_REJECTED":
		return intentv1.IntentStatus_INTENT_STATUS_REJECTED
	case "INTENT_STATUS_CANCELLED":
		return intentv1.IntentStatus_INTENT_STATUS_CANCELLED
	default:
		return intentv1.IntentStatus_INTENT_STATUS_UNSPECIFIED
	}
}
