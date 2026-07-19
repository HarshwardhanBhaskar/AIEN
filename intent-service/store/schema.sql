-- schema.sql
--
-- AIEN Database Schema Definition
--
-- DESIGN DECISIONS:
-- =================
-- 1. Identifiers (id):
--    We use the UUID type. In PostgreSQL, UUID is stored as a 128-bit
--    binary integer, which is far more efficient than storing it as a
--    36-character string. Since we use UUID v7, these keys are sequential,
--    dramatically reducing index fragmentation and write amplification.
--
-- 2. Timestamps:
--    We use TIMESTAMP WITH TIME ZONE (timestamptz). Never use TIMESTAMP
--    WITHOUT TIME ZONE in distributed systems. Without time zone, the
--    database assumes local system time, which causes major bugs when
--    servers run in different regions or Docker containers run on UTC.
--
-- 3. Payload:
--    We use BYTEA (byte array). While PostgreSQL supports JSONB,
--    the payload structure of an intent is schema-less and determined
--    by its IntentType. Storing it as binary bytes avoids parsing it
--    inside the database, preserving microsecond execution times.
--
-- 4. Status and Type:
--    We store these as VARCHAR(50) rather than PostgreSQL ENUMs.
--    Why? While ENUMs are type-safe database-side, they are hard to alter.
--    Adding a new enum value (e.g. INTENT_TYPE_NEW) requires running
--    ALTER TYPE statements which can lock tables in production. Varchar
--    with application-side verification is more flexible for updates.

CREATE TABLE IF NOT EXISTS intents (
    id UUID PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    submitter_id VARCHAR(100) NOT NULL,
    payload BYTEA NOT NULL,
    priority INTEGER NOT NULL DEFAULT 5,
    max_retries INTEGER NOT NULL DEFAULT 3,
    idempotent BOOLEAN NOT NULL DEFAULT FALSE,
    deadline TIMESTAMP WITH TIME ZONE,
    signature VARCHAR(130),
    submitter_public_key VARCHAR(70),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Schema migration additions for existing Postgres installations
ALTER TABLE intents ADD COLUMN IF NOT EXISTS submitter_public_key VARCHAR(70);
ALTER TABLE intents ALTER COLUMN signature TYPE VARCHAR(130);

-- =============================================================================
-- INDEXES
-- =============================================================================
-- Why create indexes?
-- By default, queries like "SELECT * FROM intents WHERE submitter_id = 'alice'"
-- require a Full Table Scan (Postgres scans every row). An index creates a
-- B-Tree structure allowing lookup in O(log N) time.
--
-- Index 1: Filter intents by submitter (used by GET /intents?submitter_id=X)
CREATE INDEX IF NOT EXISTS idx_intents_submitter_id ON intents (submitter_id);

-- Index 2: Filter by status (used by the Scheduler to find PENDING/VALIDATED intents)
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents (status);

-- Index 3: Sort by creation time (used for pagination and audit trails)
CREATE INDEX IF NOT EXISTS idx_intents_created_at ON intents (created_at DESC);

-- =============================================================================
-- OUTBOX PATTERN TABLE
-- =============================================================================
-- We store events to be published to NATS in this table. An atomic database
-- transaction writes to both the intents table and this outbox table.
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    topic VARCHAR(100) NOT NULL,
    payload BYTEA NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index for background poller to quickly fetch PENDING / PROCESSING events
CREATE INDEX IF NOT EXISTS idx_outbox_events_status ON outbox_events (status);

