package main

import (
	"context"
	"errors"
	"os"
	"testing"
)

// conformance runs every Store implementation through the same contract:
// lookup, deduct, replay, short stock, restore, and reseed. A backend that
// passes behaves exactly like the memory one, so swapping engines never
// changes service behavior.
func conformance(t *testing.T, store Store) {
	t.Helper()
	ctx := context.Background()

	if err := store.Seed(ctx, "t1", "test", 10); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	got, err := store.GetProduct(ctx, "t1")
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	if got.GetStock() != 10 {
		t.Fatalf("stock = %d, want 10", got.GetStock())
	}
	if _, err := store.GetProduct(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetProduct missing err = %v, want ErrNotFound", err)
	}

	left, err := store.Deduct(ctx, "t1", 3, "k1")
	if err != nil || left != 7 {
		t.Fatalf("Deduct = (%d, %v), want (7, nil)", left, err)
	}
	left, err = store.Deduct(ctx, "t1", 3, "k1")
	if err != nil || left != 7 {
		t.Fatalf("Deduct replay = (%d, %v), want (7, nil)", left, err)
	}
	if _, err := store.Deduct(ctx, "t1", 99, "k2"); !errors.Is(err, ErrInsufficient) {
		t.Fatalf("Deduct over-draw err = %v, want ErrInsufficient", err)
	}

	left, err = store.Restore(ctx, "t1", 3, "r1")
	if err != nil || left != 10 {
		t.Fatalf("Restore = (%d, %v), want (10, nil)", left, err)
	}
	left, err = store.Restore(ctx, "t1", 3, "r1")
	if err != nil || left != 10 {
		t.Fatalf("Restore replay = (%d, %v), want (10, nil)", left, err)
	}
}

func TestMemoryStore_Conformance(t *testing.T) {
	conformance(t, NewMemoryStore())
}

// TestPGStore_Conformance needs a live product_db and stays skipped
// otherwise, so `go test ./...` never requires a real server:
//
//	TEST_PRODUCT_DSN="postgres://product_user:pw@localhost:5432/product_db?sslmode=disable" go test ./services/product/ -run TestPGStore -v
func TestPGStore_Conformance(t *testing.T) {
	dsn := os.Getenv("TEST_PRODUCT_DSN")
	if dsn == "" {
		t.Skip("TEST_PRODUCT_DSN not set")
	}
	store, err := OpenPGStore(context.Background(), dsn)
	if err != nil {
		t.Fatalf("OpenPGStore: %v", err)
	}
	conformance(t, store)
}
