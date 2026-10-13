package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NotifyDurable is the named subscription position. The server keeps one
// cursor per durable name, so a restarted consumer resumes where the last
// acknowledged message left off instead of starting over or missing any.
const NotifyDurable = "notify-orders"

// decodeOrderCreated parses one consumed payload. Separated from delivery
// so it is unit-testable without a server.
func decodeOrderCreated(raw []byte) (orderID string, createdAtUnixMs int64, err error) {
	var event struct {
		OrderID         string `json:"order_id"`
		CreatedAtUnixMs int64  `json:"created_at_unix_ms"`
	}

	if err := json.Unmarshal(raw, &event); err != nil {
		return "", 0, err
	}

	return event.OrderID, event.CreatedAtUnixMs, nil
}

// run subscribes durably and logs every order until ctx ends. Each message
// is acknowledged only after handling, so a crash before the ack leaves it
// pending and the next boot re-receives it: at-least-once, never lost.
func run(ctx context.Context, js jetstream.JetStream) error {
	cons, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Durable:   NotifyDurable,
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return err
	}

	_, err = cons.Consume(func(msg jetstream.Msg) {
		id, at, err := decodeOrderCreated(msg.Data())
		if err != nil {
			log.Printf("notify: bad payload: %v", err)
			_ = msg.Ack()
			return
		}
		log.Printf("notify: order.created order_id=%s created_at_unix_ms=%d", id, at)
		_ = msg.Ack()
	})
	if err != nil {
		return err
	}

	<-ctx.Done()

	return ctx.Err()
}

func main() {
	addr := os.Getenv("NATS_ADDR")
	if addr == "" {
		addr = "nats://localhost:4222"
	}

	conn, err := nats.Connect(addr)
	if err != nil {
		log.Fatalf("dial nats: %v", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	if err := run(context.Background(), js); err != nil {
		log.Printf("notify: %v", err)
	}
}
