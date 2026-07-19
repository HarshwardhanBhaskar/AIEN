// Package handler defines the gRPC interface and event dispatching.
package handler

import (
	"context"
	"fmt"
	"log/slog"

	intentv1 "github.com/aien-platform/aien/proto/intent/v1"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// EventPublisher defines the interface for publishing intent lifecycle events.
//
// WHY DECOUPLING WITH AN INTERFACE?
// =================================
// The Intent Service handler should only care about dispatching events, not the
// underlying message broker technology.
// 1. Swappability: We can swap NATS for Apache Kafka, AWS SNS/SQS, or RabbitMQ
//    by implementing this interface.
// 2. Unit Testing: We don't want unit tests to require a running NATS broker.
//    We can pass a mock/no-op publisher to handler.New() in tests.
type EventPublisher interface {
	PublishIntentSubmitted(ctx context.Context, intent *intentv1.Intent) error
}

// NatsPublisher implements the EventPublisher interface using NATS JetStream.
type NatsPublisher struct {
	js  nats.JetStreamContext
	log *slog.Logger
}

// NewNatsPublisher creates a new event publisher bound to NATS JetStream.
func NewNatsPublisher(js nats.JetStreamContext, log *slog.Logger) *NatsPublisher {
	return &NatsPublisher{
		js:  js,
		log: log,
	}
}

// PublishIntentSubmitted marshals the intent and publishes it to the NATS JetStream subject.
//
// SERIALIZATION DECISION: Protobuf Binary
// =======================================
// We serialize the message using proto.Marshal(intent) into raw binary bytes.
// While JSON is easier to debug when inspecting the queue, Protobuf binary is:
// 1. Smaller: Saves bandwidth and disk storage in NATS.
// 2. Faster: Negligible serialization CPU overhead.
// 3. Schema-locked: Ensures strict schema compatibilities between publisher
//    (intent-service) and subscriber (scheduler).
func (n *NatsPublisher) PublishIntentSubmitted(ctx context.Context, intent *intentv1.Intent) error {
	// Serialize protobuf message to binary.
	data, err := proto.Marshal(intent)
	if err != nil {
		return fmt.Errorf("failed to marshal intent to protobuf: %w", err)
	}

	// Publish to NATS JetStream.
	// Subject pattern: intents.submitted.<type>
	// This allows subscribers to filter by action type if they wish (e.g. intents.submitted.TRANSFER).
	subject := fmt.Sprintf("intents.submitted.%s", intent.Type.String())

	n.log.Debug("Publishing event to NATS JetStream", "subject", subject, "intent_id", intent.Id)

	_, err = n.js.PublishMsg(&nats.Msg{
		Subject: subject,
		Data:    data,
	}, nats.Context(ctx))
	if err != nil {
		return fmt.Errorf("failed to publish to NATS JetStream: %w", err)
	}

	return nil
}

// NoOpPublisher is a no-op implementation of EventPublisher.
// Used for unit tests so we don't need a NATS broker running.
type NoOpPublisher struct{}

func (n NoOpPublisher) PublishIntentSubmitted(ctx context.Context, intent *intentv1.Intent) error {
	return nil
}
