package store

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// PostgresStore implements database interactions for the Wallet Service.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore instantiates a new database repository.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// Migrate executes the SQL schema statements in the given schemaPath.
func (s *PostgresStore) Migrate(schemaPath string) error {
	file, err := os.Open(schemaPath)
	if err != nil {
		return fmt.Errorf("failed to open schema file: %w", err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read schema file: %w", err)
	}

	_, err = s.db.Exec(string(content))
	if err != nil {
		return fmt.Errorf("failed to execute schema migration: %w", err)
	}

	return nil
}

// GetBalance retrieves the active balance for the specified account.
func (s *PostgresStore) GetBalance(ctx context.Context, accountID string) (float64, error) {
	var balance float64
	query := "SELECT balance FROM wallet_accounts WHERE id = $1"
	err := s.db.QueryRowContext(ctx, query, accountID).Scan(&balance)
	if err == sql.ErrNoRows {
		return 0.0, fmt.Errorf("account not found: %s", accountID)
	}
	if err != nil {
		return 0.0, fmt.Errorf("failed to query balance: %w", err)
	}
	return balance, nil
}

// Credit adds funds to an account, creating the account if it does not exist.
func (s *PostgresStore) Credit(ctx context.Context, accountID string, amount float64, referenceID string) (float64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// Check for duplicate reference ID
	var exists bool
	err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM wallet_transactions WHERE reference_id = $1)", referenceID).Scan(&exists)
	if err != nil {
		return 0, err
	}
	if exists {
		return 0, fmt.Errorf("transaction with reference_id %s already processed", referenceID)
	}

	// Create account if not exists
	_, err = tx.ExecContext(ctx, "INSERT INTO wallet_accounts (id, balance) VALUES ($1, 0.0) ON CONFLICT (id) DO NOTHING", accountID)
	if err != nil {
		return 0, err
	}

	// Lock and update
	var currentBalance float64
	err = tx.QueryRowContext(ctx, "SELECT balance FROM wallet_accounts WHERE id = $1 FOR UPDATE", accountID).Scan(&currentBalance)
	if err != nil {
		return 0, err
	}

	newBalance := currentBalance + amount
	_, err = tx.ExecContext(ctx, "UPDATE wallet_accounts SET balance = $1, updated_at = NOW() WHERE id = $2", newBalance, accountID)
	if err != nil {
		return 0, err
	}

	// Log transaction audit
	txID := uuid.New()
	_, err = tx.ExecContext(ctx, 
		"INSERT INTO wallet_transactions (id, from_account, to_account, amount, reference_id) VALUES ($1, NULL, $2, $3, $4)",
		txID, accountID, amount, referenceID,
	)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return newBalance, nil
}

// Debit subtracts funds from an account.
func (s *PostgresStore) Debit(ctx context.Context, accountID string, amount float64, referenceID string) (float64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// Check for duplicate reference ID
	var exists bool
	err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM wallet_transactions WHERE reference_id = $1)", referenceID).Scan(&exists)
	if err != nil {
		return 0, err
	}
	if exists {
		return 0, fmt.Errorf("transaction with reference_id %s already processed", referenceID)
	}

	// Lock and query balance
	var currentBalance float64
	err = tx.QueryRowContext(ctx, "SELECT balance FROM wallet_accounts WHERE id = $1 FOR UPDATE", accountID).Scan(&currentBalance)
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("account not found: %s", accountID)
	}
	if err != nil {
		return 0, err
	}

	if currentBalance < amount {
		return 0, fmt.Errorf("insufficient funds: available %.4f, required %.4f", currentBalance, amount)
	}

	newBalance := currentBalance - amount
	_, err = tx.ExecContext(ctx, "UPDATE wallet_accounts SET balance = $1, updated_at = NOW() WHERE id = $2", newBalance, accountID)
	if err != nil {
		return 0, err
	}

	// Log transaction audit
	txID := uuid.New()
	_, err = tx.ExecContext(ctx, 
		"INSERT INTO wallet_transactions (id, from_account, to_account, amount, reference_id) VALUES ($1, $2, NULL, $3, $4)",
		txID, accountID, amount, referenceID,
	)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return newBalance, nil
}

// Transfer atomically transfers funds from one account to another.
// Prevents deadlocks by enforcing deterministic alphabetical lock ordering.
func (s *PostgresStore) Transfer(ctx context.Context, fromAccount, toAccount string, amount float64, referenceID string) (float64, float64, error) {
	if fromAccount == toAccount {
		return 0, 0, fmt.Errorf("source and destination accounts must be different")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()

	// Check for duplicate reference ID (Idempotency check)
	var exists bool
	err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM wallet_transactions WHERE reference_id = $1)", referenceID).Scan(&exists)
	if err != nil {
		return 0, 0, err
	}
	if exists {
		return 0, 0, fmt.Errorf("transaction with reference_id %s already processed", referenceID)
	}

	// Create recipient account if not exists
	_, err = tx.ExecContext(ctx, "INSERT INTO wallet_accounts (id, balance) VALUES ($1, 0.0) ON CONFLICT (id) DO NOTHING", toAccount)
	if err != nil {
		return 0, 0, err
	}

	// Lock rows deterministically to prevent deadlocks
	firstLock := fromAccount
	secondLock := toAccount
	if fromAccount > toAccount {
		firstLock = toAccount
		secondLock = fromAccount
	}

	locks := make(map[string]float64)

	var bal1 float64
	err = tx.QueryRowContext(ctx, "SELECT balance FROM wallet_accounts WHERE id = $1 FOR UPDATE", firstLock).Scan(&bal1)
	if err == sql.ErrNoRows {
		return 0, 0, fmt.Errorf("account not found: %s", firstLock)
	}
	if err != nil {
		return 0, 0, err
	}
	locks[firstLock] = bal1

	var bal2 float64
	err = tx.QueryRowContext(ctx, "SELECT balance FROM wallet_accounts WHERE id = $1 FOR UPDATE", secondLock).Scan(&bal2)
	if err == sql.ErrNoRows {
		return 0, 0, fmt.Errorf("account not found: %s", secondLock)
	}
	if err != nil {
		return 0, 0, err
	}
	locks[secondLock] = bal2

	// Verify sender balance
	fromBalance := locks[fromAccount]
	toBalance := locks[toAccount]

	if fromBalance < amount {
		return 0, 0, fmt.Errorf("insufficient funds: sender balance is %.4f, required %.4f", fromBalance, amount)
	}

	fromNewBalance := fromBalance - amount
	toNewBalance := toBalance + amount

	// Update source
	_, err = tx.ExecContext(ctx, "UPDATE wallet_accounts SET balance = $1, updated_at = NOW() WHERE id = $2", fromNewBalance, fromAccount)
	if err != nil {
		return 0, 0, err
	}

	// Update destination
	_, err = tx.ExecContext(ctx, "UPDATE wallet_accounts SET balance = $1, updated_at = NOW() WHERE id = $2", toNewBalance, toAccount)
	if err != nil {
		return 0, 0, err
	}

	// Log transaction audit
	txID := uuid.New()
	_, err = tx.ExecContext(ctx, 
		"INSERT INTO wallet_transactions (id, from_account, to_account, amount, reference_id) VALUES ($1, $2, $3, $4, $5)",
		txID, fromAccount, toAccount, amount, referenceID,
	)
	if err != nil {
		return 0, 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}

	return fromNewBalance, toNewBalance, nil
}
