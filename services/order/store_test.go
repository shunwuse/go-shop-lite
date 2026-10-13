package main

import (
	"context"
	"errors"
	"os"
	"testing"

	pb "example.com/shop/proto/gen/shop/v1"
)

// conformance runs every Store implementation through the same contract:
// issue numbers, create, read back, flip status, and miss. A backend that
// passes behaves exactly like the memory one.
func conformance(t *testing.T, store Store) {
	t.Helper()
	ctx := context.Background()

	n, err := store.NextID(ctx)
	if err != nil || n <= 0 {
		t.Fatalf("NextID = (%d, %v), want positive", n, err)
	}
	order := Order{
		OrderID:         "o-test",
		CustomerID:      "c1",
		Items:           []*pb.OrderItem{{ProductId: "p1", Qty: 2}},
		Status:          "CREATED",
		CreatedAtUnixMs: 42,
	}
	if err := store.Create(ctx, order); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := store.Get(ctx, "o-test")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.CustomerID != "c1" || got.Status != "CREATED" || len(got.Items) != 1 || got.Items[0].GetQty() != 2 {
		t.Fatalf("Get = %+v, want c1/CREATED/1 item", got)
	}
	if err := store.SetStatus(ctx, "o-test", "CANCELLED"); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	got, err = store.Get(ctx, "o-test")
	if err != nil || got.Status != "CANCELLED" {
		t.Fatalf("Get after flip = (%+v, %v), want CANCELLED", got, err)
	}
	if _, err := store.Get(ctx, "missing"); !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("Get missing err = %v, want ErrOrderNotFound", err)
	}
}

func TestMemoryStore_Conformance(t *testing.T) {
	conformance(t, NewMemoryStore())
}

// TestPGStore_Conformance needs a live order_db and stays skipped
// otherwise, so `go test ./...` never requires a real server:
//
//	TEST_ORDER_DSN="postgres://order_user:pw@localhost:5432/order_db?sslmode=disable" go test ./services/order/ -run TestPGStore -v
func TestPGStore_Conformance(t *testing.T) {
	dsn := os.Getenv("TEST_ORDER_DSN")
	if dsn == "" {
		t.Skip("TEST_ORDER_DSN not set")
	}
	store, err := OpenPGStore(context.Background(), dsn)
	if err != nil {
		t.Fatalf("OpenPGStore: %v", err)
	}
	conformance(t, store)
}
