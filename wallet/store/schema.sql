-- schema.sql
-- Wallet Service Database Schema

CREATE TABLE IF NOT EXISTS wallet_accounts (
    id VARCHAR(100) PRIMARY KEY,
    balance NUMERIC(20, 8) NOT NULL DEFAULT 0.0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wallet_transactions (
    id UUID PRIMARY KEY,
    from_account VARCHAR(100),
    to_account VARCHAR(100),
    amount NUMERIC(20, 8) NOT NULL,
    reference_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index for reference_id lookup to guarantee idempotency and audit logs
CREATE UNIQUE INDEX IF NOT EXISTS idx_wallet_transactions_ref ON wallet_transactions (reference_id);

-- Pre-seed test accounts for visual validation
INSERT INTO wallet_accounts (id, balance)
VALUES 
    ('user-alice-001', 1000.00000000),
    ('user-bob-001', 100.00000000)
ON CONFLICT (id) DO NOTHING;
