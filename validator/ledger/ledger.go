// Package ledger implements a persistent, immutable, hash-linked log storage.
//
// WHY IMMUTABLE LEDGER WITH HASH CHAINS?
// ======================================
// Traditional databases allow records to be updated or deleted. An administrator
// or an attacker with write access to PostgreSQL can alter execution histories.
//
// To prevent this, we write state transitions to an append-only log file on disk.
// Every entry (Block) is cryptographically linked to the previous entry using a
// SHA-256 hash. This forms a "Hash Chain" (the core concept behind blockchains
// and Git commit history).
//
// Block N Hash = SHA256(Block N Data + Block N-1 Hash)
//
// If someone alters a single byte in Block 5, its hash changes. Consequently,
// Block 6's PrevHash no longer matches, causing a validation failure. To cover
// their tracks, they would have to recompute the hashes for every subsequent block,
// which is easily detected.
//
// FILE FORMAT: JSON Lines (JSONL)
// ===============================
// Each block is serialized as a single line of JSON, followed by a newline (\n).
// This is a standard production format for write-ahead logs (WAL) because:
// 1. Simplicity: Easy to read, write, and parse line-by-line without loading the
//    entire file into memory.
// 2. Resilience: If the process crashes mid-write, only the last line is corrupted,
//    which can be detected and truncated.
package ledger

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Block represents a single immutable record in our ledger.
type Block struct {
	Index     uint64 `json:"index"`
	Timestamp string `json:"timestamp"`
	IntentID  string `json:"intent_id"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	PrevHash  string `json:"prev_hash"`
	Hash      string `json:"hash"`
}

// Engine manages reading, writing, and validating the ledger file.
type Engine struct {
	mu       sync.Mutex
	filePath string
}

// NewEngine creates a new Ledger Engine pointing to the specified file.
func NewEngine(filePath string) *Engine {
	return &Engine{
		filePath: filePath,
	}
}

// Append logs a new state transition to the ledger.
//
// It automatically retrieves the previous block's hash, computes the new hash,
// writes the JSON record to disk, and runs file.Sync() to guarantee durability.
func (e *Engine) Append(intentID, intentType, status string) (*Block, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Get the last block in the log to construct the new block.
	lastBlock, err := e.readLastBlock()
	if err != nil {
		return nil, fmt.Errorf("failed to read last block: %w", err)
	}

	var index uint64 = 0
	prevHash := "0000000000000000000000000000000000000000000000000000000000000000" // Genesis Hash

	if lastBlock != nil {
		index = lastBlock.Index + 1
		prevHash = lastBlock.Hash
	}

	block := &Block{
		Index:     index,
		Timestamp: time.Now().Format(time.RFC3339),
		IntentID:  intentID,
		Type:      intentType,
		Status:    status,
		PrevHash:  prevHash,
	}

	// 2. Compute the cryptographic hash for this block.
	block.Hash = calculateHash(block)

	// 3. Open the file in Append mode (or create it if it doesn't exist).
	// O_APPEND: Windows and Linux guarantee atomic appends at the OS level.
	// O_CREATE: Create file if missing.
	// O_WRONLY: Open for writing only.
	file, err := os.OpenFile(e.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open ledger file: %w", err)
	}
	defer file.Close()

	// 4. Marshal to JSON and write.
	data, err := json.Marshal(block)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize block: %w", err)
	}

	if _, err := file.Write(append(data, '\n')); err != nil {
		return nil, fmt.Errorf("failed to write block to file: %w", err)
	}

	// 5. Force the OS to flush its memory write buffers to physical storage.
	// This is the "D" (Durability) in ACID. Without Sync(), a power failure
	// could lose recently appended ledger blocks.
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync ledger file: %w", err)
	}

	return block, nil
}

// Verify scans the entire ledger file, recalculates hashes, and validates
// the cryptographic hash chain. Returns an error detailing the exact index
// where tampering was detected.
func (e *Engine) Verify() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	file, err := os.Open(e.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // An empty/non-existent ledger is technically valid.
		}
		return fmt.Errorf("failed to open ledger file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var expectedPrevHash = "0000000000000000000000000000000000000000000000000000000000000000"
	var expectedIndex uint64 = 0

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var block Block
		if err := json.Unmarshal(line, &block); err != nil {
			return fmt.Errorf("failed to parse ledger line: %w", err)
		}

		// Validate chronological index sequencing.
		if block.Index != expectedIndex {
			return fmt.Errorf("ledger sequence break at index %d: expected index %d", block.Index, expectedIndex)
		}

		// Validate PrevHash links.
		if block.PrevHash != expectedPrevHash {
			return fmt.Errorf("ledger chain broken at index %d: expected prev_hash %s, got %s", block.Index, expectedPrevHash, block.PrevHash)
		}

		// Recalculate hash and verify integrity.
		recalculatedHash := calculateHash(&block)
		if block.Hash != recalculatedHash {
			return fmt.Errorf("ledger data tampered at index %d: block hash %s does not match recalculated hash %s", block.Index, block.Hash, recalculatedHash)
		}

		// Set markers for next iteration.
		expectedPrevHash = block.Hash
		expectedIndex++
	}

	return scanner.Err()
}

// readLastBlock helper scans the file to locate the last complete block.
func (e *Engine) readLastBlock() (*Block, error) {
	file, err := os.Open(e.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No file yet.
		}
		return nil, err
	}
	defer file.Close()

	var lastBlock *Block
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var block Block
		if err := json.Unmarshal(line, &block); err == nil {
			lastBlock = &block
		}
	}

	return lastBlock, scanner.Err()
}

// calculateHash computes the SHA-256 hash over a block's fields.
func calculateHash(b *Block) string {
	// Construct hash input string.
	// Index + Timestamp + IntentID + Type + Status + PrevHash
	input := fmt.Sprintf("%d%s%s%s%s%s", b.Index, b.Timestamp, b.IntentID, b.Type, b.Status, b.PrevHash)

	h := sha256.New()
	h.Write([]byte(input))
	return hex.EncodeToString(h.Sum(nil))
}
