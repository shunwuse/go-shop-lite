package main

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
)

// OrderCreatedSubject carries one message per successful order.
const OrderCreatedSubject = "order.created"

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

// NATSPublisher publishes to a live NATS server. Delivery past the
// Publish call is the server's job; durability (streams, durable
// consumers) is set up on the consumer side in a later pack.
type NATSPublisher struct {
	conn *nats.Conn
}

// NewNATSPublisher dials addr and returns a Publisher.
func NewNATSPublisher(addr string) (*NATSPublisher, error) {
	conn, err := nats.Connect(addr)
	if err != nil {
		return nil, err
	}
	return &NATSPublisher{conn: conn}, nil
}

func (p *NATSPublisher) PublishOrderCreated(ctx context.Context, event OrderCreated) error {
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.conn.Publish(OrderCreatedSubject, raw)
}
