// Package handler_test contains unit tests for the IntentHandler.
package handler_test

import (
	"context"
	"testing"

	"github.com/aien-platform/aien/intent-service/handler"
	"github.com/aien-platform/aien/intent-service/store"
	"github.com/aien-platform/aien/shared/crypto"
	"github.com/aien-platform/aien/shared/logger"

	intentv1 "github.com/aien-platform/aien/proto/intent/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newTestHandler creates a handler with an in-memory store for testing.
func newTestHandler() *handler.IntentHandler {
	log := logger.New("test")
	memStore := store.NewMemoryStore()
	return handler.New(memStore, log)
}

// newSignedRequest is a test helper that creates an Ed25519-signed gRPC request payload.
func newSignedRequest(typ intentv1.IntentType, submitterId string, payload []byte) *intentv1.SubmitIntentRequest {
	pubKey, privKey, _ := crypto.GenerateKeyPair()
	signBytes := crypto.GetSignBytes(typ.String(), submitterId, payload)
	sig, _ := crypto.Sign(signBytes, privKey)

	return &intentv1.SubmitIntentRequest{
		Type:               typ,
		SubmitterId:        submitterId,
		Payload:            payload,
		Signature:          sig,
		SubmitterPublicKey: pubKey,
	}
}

// =============================================================================
// SubmitIntent Tests
// =============================================================================

// TestSubmitIntent_Success verifies that a valid signed intent is created
// and returns a UUID v7 ID with PENDING status.
func TestSubmitIntent_Success(t *testing.T) {
	h := newTestHandler()

	req := newSignedRequest(
		intentv1.IntentType_INTENT_TYPE_TRANSFER,
		"user-alice-001",
		[]byte(`{"from":"alice","to":"bob","amount":100}`),
	)
	req.Constraints = &intentv1.IntentConstraints{
		Priority:   7,
		MaxRetries: 3,
		Idempotent: true,
	}

	resp, err := h.SubmitIntent(context.Background(), req)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify ID is non-empty (UUID v7 format).
	if resp.IntentId == "" {
		t.Fatal("expected non-empty intent ID")
	}

	// Verify initial status is PENDING.
	if resp.Status != intentv1.IntentStatus_INTENT_STATUS_PENDING {
		t.Errorf("expected PENDING status, got: %s", resp.Status.String())
	}

	// Verify timestamp is set.
	if resp.CreatedAt == nil {
		t.Fatal("expected created_at to be set")
	}
}

// TestSubmitIntent_InvalidSignature verifies that altering the payload
// signature leads to Unauthenticated error.
func TestSubmitIntent_InvalidSignature(t *testing.T) {
	h := newTestHandler()

	req := newSignedRequest(
		intentv1.IntentType_INTENT_TYPE_TRANSFER,
		"user-alice-001",
		[]byte(`{"from":"alice","to":"bob","amount":100}`),
	)
	// Tamper with signature string
	req.Signature = "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"

	_, err := h.SubmitIntent(context.Background(), req)
	if err == nil {
		t.Fatal("expected signature verification to fail")
	}

	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated status, got: %s", st.Code().String())
	}
}

// TestSubmitIntent_MissingType verifies that submitting an intent
// without a type returns InvalidArgument.
func TestSubmitIntent_MissingType(t *testing.T) {
	h := newTestHandler()

	req := newSignedRequest(
		intentv1.IntentType_INTENT_TYPE_UNSPECIFIED,
		"user-alice-001",
		[]byte("test"),
	)

	_, err := h.SubmitIntent(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for missing type")
	}

	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got: %s", st.Code().String())
	}
}

// TestSubmitIntent_MissingSubmitter verifies that submitting an intent
// without a submitter_id returns InvalidArgument.
func TestSubmitIntent_MissingSubmitter(t *testing.T) {
	h := newTestHandler()

	req := newSignedRequest(
		intentv1.IntentType_INTENT_TYPE_COMPUTE,
		"",
		[]byte("test"),
	)

	_, err := h.SubmitIntent(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for missing submitter_id")
	}

	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got: %s", st.Code().String())
	}
}

// =============================================================================
// GetIntent Tests
// =============================================================================

// TestGetIntent_Success verifies that we can retrieve a previously
// submitted intent by its ID.
func TestGetIntent_Success(t *testing.T) {
	h := newTestHandler()

	// First, create an intent.
	req := newSignedRequest(
		intentv1.IntentType_INTENT_TYPE_QUERY,
		"user-carol-003",
		[]byte(`{"query":"SELECT * FROM nodes"}`),
	)

	submitResp, err := h.SubmitIntent(context.Background(), req)
	if err != nil {
		t.Fatalf("setup: failed to submit intent: %v", err)
	}

	// Now retrieve it.
	getResp, err := h.GetIntent(context.Background(), &intentv1.GetIntentRequest{
		IntentId: submitResp.IntentId,
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	intent := getResp.Intent
	if intent.Id != submitResp.IntentId {
		t.Errorf("expected ID %s, got %s", submitResp.IntentId, intent.Id)
	}
	if intent.Type != intentv1.IntentType_INTENT_TYPE_QUERY {
		t.Errorf("expected QUERY type, got: %s", intent.Type.String())
	}
	if intent.SubmitterId != "user-carol-003" {
		t.Errorf("expected submitter_id user-carol-003, got: %s", intent.SubmitterId)
	}
	if intent.SubmitterPublicKey != req.SubmitterPublicKey {
		t.Errorf("expected public key %s, got %s", req.SubmitterPublicKey, intent.SubmitterPublicKey)
	}
}

// TestGetIntent_NotFound verifies that requesting a non-existent intent
// returns NotFound.
func TestGetIntent_NotFound(t *testing.T) {
	h := newTestHandler()

	_, err := h.GetIntent(context.Background(), &intentv1.GetIntentRequest{
		IntentId: "non-existent-id",
	})

	if err == nil {
		t.Fatal("expected error for non-existent intent")
	}

	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got: %s", st.Code().String())
	}
}

// TestGetIntent_EmptyID verifies that an empty intent_id returns
// InvalidArgument.
func TestGetIntent_EmptyID(t *testing.T) {
	h := newTestHandler()

	_, err := h.GetIntent(context.Background(), &intentv1.GetIntentRequest{
		IntentId: "",
	})

	if err == nil {
		t.Fatal("expected error for empty intent_id")
	}

	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got: %s", st.Code().String())
	}
}

// =============================================================================
// ListIntents Tests
// =============================================================================

// TestListIntents_Empty verifies that listing intents on an empty store
// returns an empty list with total_count 0.
func TestListIntents_Empty(t *testing.T) {
	h := newTestHandler()

	resp, err := h.ListIntents(context.Background(), &intentv1.ListIntentsRequest{})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(resp.Intents) != 0 {
		t.Errorf("expected 0 intents, got: %d", len(resp.Intents))
	}
	if resp.TotalCount != 0 {
		t.Errorf("expected total_count 0, got: %d", resp.TotalCount)
	}
}

// TestListIntents_Multiple verifies that multiple submitted intents
// are returned in order.
func TestListIntents_Multiple(t *testing.T) {
	h := newTestHandler()

	// Submit 3 intents.
	types := []intentv1.IntentType{
		intentv1.IntentType_INTENT_TYPE_TRANSFER,
		intentv1.IntentType_INTENT_TYPE_COMPUTE,
		intentv1.IntentType_INTENT_TYPE_QUERY,
	}
	for i, typ := range types {
		req := newSignedRequest(typ, "user-test", []byte("test"))
		_, err := h.SubmitIntent(context.Background(), req)
		if err != nil {
			t.Fatalf("setup: failed to submit intent %d: %v", i, err)
		}
	}

	// List all.
	resp, err := h.ListIntents(context.Background(), &intentv1.ListIntentsRequest{})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(resp.Intents) != 3 {
		t.Errorf("expected 3 intents, got: %d", len(resp.Intents))
	}
	if resp.TotalCount != 3 {
		t.Errorf("expected total_count 3, got: %d", resp.TotalCount)
	}

	// Verify order: TRANSFER, COMPUTE, QUERY.
	for i, typ := range types {
		if resp.Intents[i].Type != typ {
			t.Errorf("intent[%d]: expected type %s, got %s", i, typ.String(), resp.Intents[i].Type.String())
		}
	}
}

// TestListIntents_FilterBySubmitter verifies that the submitter_id
// filter works correctly.
func TestListIntents_FilterBySubmitter(t *testing.T) {
	h := newTestHandler()

	// Submit intents from two different submitters.
	for _, sub := range []string{"alice", "bob", "alice"} {
		req := newSignedRequest(intentv1.IntentType_INTENT_TYPE_TRANSFER, sub, []byte("test"))
		_, err := h.SubmitIntent(context.Background(), req)
		if err != nil {
			t.Fatalf("setup: failed to submit: %v", err)
		}
	}

	// Filter by alice.
	resp, err := h.ListIntents(context.Background(), &intentv1.ListIntentsRequest{
		SubmitterIdFilter: "alice",
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if resp.TotalCount != 2 {
		t.Errorf("expected 2 intents from alice, got: %d", resp.TotalCount)
	}
	for _, intent := range resp.Intents {
		if intent.SubmitterId != "alice" {
			t.Errorf("expected submitter alice, got: %s", intent.SubmitterId)
		}
	}
}
