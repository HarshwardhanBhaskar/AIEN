// Package store provides storage backends for intents.
//
// ARCHITECTURAL PATTERN: Repository Pattern
// ==========================================
// We define a Store INTERFACE, not a concrete implementation.
// This is a core Clean Architecture principle:
//
//   Business Logic (handlers) → depends on → Interface (Store)
//                                              ↑
//                                   Concrete Implementation
//                                   (MemoryStore now, PostgreSQL later)
//
// WHY?
// 1. Testability: Unit tests use a mock store. No database needed.
// 2. Swappability: We can swap MemoryStore for PostgresStore without
//    changing a single line in the handler code.
// 3. Dependency Inversion: High-level modules (handlers) don't depend
//    on low-level modules (database drivers). Both depend on abstractions.
//
// This is the "D" in SOLID (Dependency Inversion Principle).
package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aien-platform/aien/shared/ids"

	intentv1 "github.com/aien-platform/aien/proto/intent/v1"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// Store defines the interface for intent persistence.
//
// Any storage backend (memory, PostgreSQL, RocksDB) must implement
// these methods. The handler code programs against this interface,
// never against a concrete type.
type Store interface {
	// Create persists a new intent and returns its assigned ID.
	Create(ctx context.Context, req *intentv1.SubmitIntentRequest) (*intentv1.Intent, error)

	// Get retrieves an intent by ID. Returns nil if not found.
	Get(ctx context.Context, id string) (*intentv1.Intent, error)

	// List returns intents matching the given filters with pagination.
	List(ctx context.Context, statusFilter intentv1.IntentStatus, submitterFilter string, pageSize int, pageToken string) ([]*intentv1.Intent, string, int, error)
}

// MemoryStore is an in-memory implementation of the Store interface.
//
// WHY START WITH IN-MEMORY?
// =========================
// 1. Zero external dependencies: No database to install or configure.
// 2. Fast iteration: We can build and test the entire gRPC flow
//    without worrying about SQL schemas, migrations, or connection pools.
// 3. Correctness first: Get the business logic right before adding
//    persistence complexity.
//
// TRADE-OFFS:
// - Data is lost when the process restarts.
// - No concurrent query optimization (we lock the entire map).
// - No pagination efficiency (we iterate all entries).
//
// All of these are acceptable for Phase 1. We'll replace this with
// PostgreSQL in Phase 2.
type MemoryStore struct {
	// mu protects concurrent access to the intents map.
	//
	// WHY sync.RWMutex instead of sync.Mutex?
	// RWMutex allows multiple concurrent readers (Get, List) but
	// exclusive access for writers (Create). In a read-heavy system
	// like ours (many status checks, few submissions), this
	// significantly reduces lock contention.
	mu sync.RWMutex

	// intents maps intent ID → intent.
	// Using a map gives O(1) lookups by ID.
	intents map[string]*intentv1.Intent

	// ordered maintains insertion order for List pagination.
	// Maps are unordered in Go, so we need a separate slice
	// to support chronological listing.
	ordered []string
}

// NewMemoryStore creates a new in-memory intent store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		intents: make(map[string]*intentv1.Intent),
		ordered: make([]string, 0),
	}
}

// Create persists a new intent in memory.
//
// It generates a UUID v7 ID, sets the initial status to PENDING,
// and records timestamps. The caller (gRPC handler) never needs
// to know that this is an in-memory implementation.
func (m *MemoryStore) Create(ctx context.Context, req *intentv1.SubmitIntentRequest) (*intentv1.Intent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := timestamppb.New(time.Now())

	intent := &intentv1.Intent{
		Id:          ids.NewIntentID(),
		Type:        req.Type,
		Status:      intentv1.IntentStatus_INTENT_STATUS_PENDING,
		SubmitterId: req.SubmitterId,
		Payload:     req.Payload,
		Constraints: req.Constraints,
		Signature:          req.Signature,
		SubmitterPublicKey: req.SubmitterPublicKey,
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	m.intents[intent.Id] = intent
	m.ordered = append(m.ordered, intent.Id)

	return intent, nil
}

// Get retrieves a single intent by its ID.
//
// Uses a read lock (RLock) so multiple goroutines can read
// concurrently without blocking each other.
func (m *MemoryStore) Get(ctx context.Context, id string) (*intentv1.Intent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	intent, exists := m.intents[id]
	if !exists {
		return nil, fmt.Errorf("intent not found: %s", id)
	}

	return intent, nil
}

// List returns a filtered, paginated list of intents.
//
// PAGINATION STRATEGY: Offset-based using the ordered slice index.
// The page_token is the string index of the next item to return.
//
// WHY NOT CURSOR-BASED PAGINATION?
// Cursor-based (using the last seen ID) is more robust for concurrent
// writes but requires sorted storage. For an in-memory map, offset
// pagination is simpler and sufficient. We'll switch to cursor-based
// when we move to PostgreSQL.
func (m *MemoryStore) List(ctx context.Context, statusFilter intentv1.IntentStatus, submitterFilter string, pageSize int, pageToken string) ([]*intentv1.Intent, string, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if pageSize <= 0 {
		pageSize = 20 // Default page size.
	}

	// Determine starting offset from page token.
	startIdx := 0
	if pageToken != "" {
		fmt.Sscanf(pageToken, "%d", &startIdx)
	}

	// Collect matching intents.
	var results []*intentv1.Intent
	totalMatching := 0

	for i := 0; i < len(m.ordered); i++ {
		intent := m.intents[m.ordered[i]]

		// Apply filters.
		if statusFilter != intentv1.IntentStatus_INTENT_STATUS_UNSPECIFIED && intent.Status != statusFilter {
			continue
		}
		if submitterFilter != "" && intent.SubmitterId != submitterFilter {
			continue
		}

		totalMatching++

		// Skip items before the page offset.
		if totalMatching <= startIdx {
			continue
		}

		// Collect until page is full.
		if len(results) < pageSize {
			results = append(results, intent)
		}
	}

	// Generate next page token.
	nextPageToken := ""
	if startIdx+len(results) < totalMatching {
		nextPageToken = fmt.Sprintf("%d", startIdx+len(results))
	}

	return results, nextPageToken, totalMatching, nil
}
