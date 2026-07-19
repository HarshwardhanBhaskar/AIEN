// Package ledger_test contains unit tests for the ledger WAL engine.
package ledger_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aien-platform/aien/validator/ledger"
)

// setupTestLedger creates a temporary file path for testing the ledger.
func setupTestLedger(t *testing.T) (string, func()) {
	dir, err := os.MkdirTemp("", "ledger-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	filePath := filepath.Join(dir, "test_ledger.log")

	cleanup := func() {
		os.RemoveAll(dir)
	}

	return filePath, cleanup
}

// TestLedger_AppendAndVerify verifies that we can write blocks to the ledger
// and they pass chain validation.
func TestLedger_AppendAndVerify(t *testing.T) {
	path, cleanup := setupTestLedger(t)
	defer cleanup()

	engine := ledger.NewEngine(path)

	// Append 3 blocks.
	intents := []struct {
		id, typ, status string
	}{
		{"intent-1", "TRANSFER", "COMPLETED"},
		{"intent-2", "COMPUTE", "COMPLETED"},
		{"intent-3", "TRANSFER", "FAILED"},
	}

	for i, it := range intents {
		block, err := engine.Append(it.id, it.typ, it.status)
		if err != nil {
			t.Fatalf("failed to append block %d: %v", i, err)
		}

		if block.Index != uint64(i) {
			t.Errorf("block %d: expected index %d, got %d", i, i, block.Index)
		}
	}

	// Verify should succeed.
	if err := engine.Verify(); err != nil {
		t.Errorf("expected verification to succeed, got: %v", err)
	}
}

// TestLedger_TamperDetection verifies that modifying a single byte in the
// ledger log file causes Verify() to fail.
func TestLedger_TamperDetection(t *testing.T) {
	path, cleanup := setupTestLedger(t)
	defer cleanup()

	engine := ledger.NewEngine(path)

	// Append 3 blocks.
	_, _ = engine.Append("intent-1", "TRANSFER", "COMPLETED")
	_, _ = engine.Append("intent-2", "COMPUTE", "COMPLETED")
	_, _ = engine.Append("intent-3", "TRANSFER", "FAILED")

	// Read file contents.
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read ledger file: %v", err)
	}

	// Tamper with the content: replace a character in the payload.
	// We'll replace the first occurrence of "intent-2" with "intent-X".
	contentStr := string(content)
	if !strings.Contains(contentStr, "intent-2") {
		t.Fatal("expected content to contain intent-2")
	}

	tamperedContent := strings.Replace(contentStr, "intent-2", "intent-X", 1)

	// Write back the tampered content.
	err = os.WriteFile(path, []byte(tamperedContent), 0644)
	if err != nil {
		t.Fatalf("failed to write tampered content to file: %v", err)
	}

	// Verify should now FAIL.
	err = engine.Verify()
	if err == nil {
		t.Fatal("expected verification to fail on tampered content")
	}

	t.Logf("Verification failed as expected: %v", err)
}

// TestLedger_BrokenLinkDetection verifies that deleting a block or altering
// the index sequence breaks verification.
func TestLedger_BrokenLinkDetection(t *testing.T) {
	path, cleanup := setupTestLedger(t)
	defer cleanup()

	engine := ledger.NewEngine(path)

	// Append 3 blocks.
	_, _ = engine.Append("intent-1", "TRANSFER", "COMPLETED")
	_, _ = engine.Append("intent-2", "COMPUTE", "COMPLETED")
	_, _ = engine.Append("intent-3", "TRANSFER", "FAILED")

	// Read lines.
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read ledger file: %v", err)
	}

	lines := strings.Split(string(content), "\n")
	// Delete the second line (index 1).
	// lines: [index0, index1, index2, empty]
	modifiedLines := append(lines[:1], lines[2:]...)
	modifiedContent := strings.Join(modifiedLines, "\n")

	err = os.WriteFile(path, []byte(modifiedContent), 0644)
	if err != nil {
		t.Fatalf("failed to write modified content to file: %v", err)
	}

	// Verify should now FAIL because index 2's prev_hash won't match index 0's hash,
	// and the index sequence goes 0 -> 2 (break in index sequence).
	err = engine.Verify()
	if err == nil {
		t.Fatal("expected verification to fail on deleted block line")
	}

	t.Logf("Verification failed as expected: %v", err)
}
