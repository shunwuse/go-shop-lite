package main

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// OrderCreatedSubject carries one message per successful order.
const OrderCreatedSubject = "order.created"

// OrderStream persists OrderCreatedSubject so a restarted consumer
// re-receives without loss.
const OrderStream = "ORDERS"

// OrderCreated is the payload of OrderCreatedSubject.
type OrderCreated struct {
	OrderID         string `json:"order_id"`
	CreatedAtUnixMs int64  `json:"created_at_unix_ms"`
}

// Publisher emits order events. The fake in server_test.go and the
// NATS publisher below both satisfy it, so CreateOrder never knows
// which backend it talks to.
type Publisher interface {
	PublishOrderCreated(ctx context.Context, event OrderCreated) error
}

// EnsureOrderStream creates the durable stream once; later calls update
// nothing. Both publisher and consumer call it, so either side can boot
// first without depending on the other.
func EnsureOrderStream(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     OrderStream,
		Subjects: []string{OrderCreatedSubject},
	})

	return err
}

// NATSPublisher publishes through JetStream, so every announcement is
// persisted before Publish returns. A consumer that was down at publish
// time still receives it after restart.
type NATSPublisher struct {
	js jetstream.JetStream
}

// NewNATSPublisher dials addr and returns a Publisher.
func NewNATSPublisher(addr string) (*NATSPublisher, error) {
	conn, err := nats.Connect(addr)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(conn)
	if err != nil {
		return nil, err
	}
	if err := EnsureOrderStream(context.Background(), js); err != nil {
		return nil, err
	}

	return &NATSPublisher{js: js}, nil
}

func (p *NATSPublisher) PublishOrderCreated(ctx context.Context, event OrderCreated) error {
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}

	_, err = p.js.Publish(ctx, OrderCreatedSubject, raw)
	return err
}
