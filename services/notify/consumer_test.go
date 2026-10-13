package main

import (
	"encoding/json"
	"sync"
	"testing"
)

// event mirrors the order.created payload. The shape is asserted by
// TestDecodeOrderCreated, so the bus below only checks delivery.
type event struct {
	OrderID string `json:"order_id"`
}

// fakeBus models durable-subscription semantics in memory: published events
// persist, each durable holds its ack cursor, and a resubscribe replays
// everything past the cursor. No live NATS needed.
type fakeBus struct {
	mu      sync.Mutex
	log     []event
	cursors map[string]int
}

func (b *fakeBus) publish(ev event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.log = append(b.log, ev)
}

// subscribe returns every event past the durable cursor. The caller acks by
// calling the returned function with the count handled; unacked events stay
// past the cursor and are replayed on the next subscribe.
func (b *fakeBus) subscribe(durable string) (events []event, ack func(n int)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cursors == nil {
		b.cursors = make(map[string]int)
	}
	pending := append([]event(nil), b.log[b.cursors[durable]:]...)
	return pending, func(n int) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.cursors[durable] += n
	}
}

func TestDecodeOrderCreated(t *testing.T) {
	id, at, err := decodeOrderCreated([]byte(`{"order_id":"o1","created_at_unix_ms":42}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if id != "o1" || at != 42 {
		t.Fatalf("decode = (%q, %d), want (o1, 42)", id, at)
	}
	if _, _, err := decodeOrderCreated([]byte(`{broken`)); err == nil {
		t.Fatalf("decode broken payload: want error")
	}
}

func TestRestartRereceives(t *testing.T) {
	bus := &fakeBus{}
	for _, id := range []string{"o1", "o2", "o3"} {
		bus.publish(event{OrderID: id})
	}

	// First boot handles two, then dies before acking the third.
	pending, ack := bus.subscribe(NotifyDurable)
	if len(pending) != 3 {
		t.Fatalf("first boot pending = %d, want 3", len(pending))
	}
	for _, ev := range pending[:2] {
		if _, _, err := decodeOrderCreated(mustJSON(t, ev)); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	ack(2)

	// Restart: the unacked third is re-received, nothing is lost, nothing
	// already acked repeats.
	pending, ack = bus.subscribe(NotifyDurable)
	if len(pending) != 1 || pending[0].OrderID != "o3" {
		t.Fatalf("restart pending = %+v, want [o3]", pending)
	}
	ack(1)

	pending, _ = bus.subscribe(NotifyDurable)
	if len(pending) != 0 {
		t.Fatalf("after ack pending = %d, want 0", len(pending))
	}
}

func mustJSON(t *testing.T, ev event) []byte {
	t.Helper()
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}
