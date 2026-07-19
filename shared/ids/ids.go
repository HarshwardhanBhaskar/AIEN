// Package ids provides unique identifier generation for AIEN entities.
//
// WHY UUID v7?
// ============
// There are several UUID versions, each with different properties:
//
// - UUID v4 (Random): 122 random bits. Completely random, no ordering.
//   Problem: When stored in a B-tree index (PostgreSQL, RocksDB), random
//   UUIDs cause excessive page splits because new entries scatter across
//   the tree. This destroys write performance at scale.
//
// - UUID v1 (Timestamp + MAC): Time-ordered, but leaks the machine's
//   MAC address (privacy concern) and can collide across machines.
//
// - UUID v7 (Timestamp + Random): Introduced in RFC 9562 (2024).
//   The first 48 bits are a Unix millisecond timestamp, followed by
//   74 random bits. This gives us:
//   1. Time-ordering: New UUIDs are always "greater than" old ones.
//   2. B-tree friendly: Sequential inserts into sorted indexes.
//   3. Sortable: No need for a separate created_at column to sort by time.
//   4. Privacy: No machine-identifying information.
//   5. Collision resistance: 74 random bits = negligible collision probability.
//
// TRADE-OFF: UUID v7 leaks approximate creation time from the ID itself.
// For AIEN this is acceptable because our intents are timestamped anyway.
// Systems that require unlinkable IDs (e.g., anonymous voting) would
// choose UUID v4 instead.
package ids

import (
	"github.com/google/uuid"
)

// NewIntentID generates a new UUID v7 for an intent.
//
// UUID v7 format (128 bits):
//   ┌──────────────────┬────┬──────────────────────────────┐
//   │ 48-bit timestamp │ ver│ 74-bit random                │
//   │ (ms since epoch) │ =7 │                              │
//   └──────────────────┴────┴──────────────────────────────┘
//
// Returns:
//   - string: the UUID v7 as a standard hyphenated string
//     (e.g., "01912345-6789-7abc-8def-0123456789ab").
func NewIntentID() string {
	return uuid.Must(uuid.NewV7()).String()
}
