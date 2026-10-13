package main

import (
	"context"
	"errors"
	"sync"

	pb "example.com/shop/proto/gen/shop/v1"
)

// ErrNotFound means the product id is unknown. ErrInsufficient means the
// request is valid but the stock cannot cover it. Servers map these to
// gRPC codes; the store itself never imports grpc.
var (
	ErrNotFound     = errors.New("product not found")
	ErrInsufficient = errors.New("insufficient stock")
)

// Store is the product persistence boundary. MemoryStore serves tests and
// local runs; PGStore serves product_db. Both obey the same contract, so
// swapping engines never changes service behavior.
type Store interface {
	GetProduct(ctx context.Context, productID string) (*pb.GetProductResponse, error)
	// Deduct subtracts qty exactly once per key, replaying the stored
	// remaining on retry.
	Deduct(ctx context.Context, productID string, qty int32, key string) (remaining int32, err error)
	// Restore adds qty back exactly once per key.
	Restore(ctx context.Context, productID string, qty int32, key string) (remaining int32, err error)
	// Seed inserts or replaces a product. Production never calls it;
	// seed scripts and tests do.
	Seed(ctx context.Context, productID, name string, stock int32) error
}

// MemoryStore keeps the catalog in a mutex-guarded map.
type MemoryStore struct {
	mu         sync.Mutex
	items      map[string]*pb.GetProductResponse
	deductions map[string]int32
	restores   map[string]int32
}

// NewMemoryStore returns a Store seeded with two products for solo testing.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		items: map[string]*pb.GetProductResponse{
			"p1": {ProductId: "p1", Name: "apple", Stock: 10},
			"p2": {ProductId: "p2", Name: "banana", Stock: 5},
		},
		deductions: make(map[string]int32),
		restores:   make(map[string]int32),
	}
}

func (s *MemoryStore) GetProduct(ctx context.Context, productID string) (*pb.GetProductResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[productID]
	if !ok {
		return nil, ErrNotFound
	}

	return &pb.GetProductResponse{
		ProductId: item.GetProductId(),
		Name:      item.GetName(),
		Stock:     item.GetStock(),
	}, nil
}

func (s *MemoryStore) Deduct(ctx context.Context, productID string, qty int32, key string) (int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if prev, ok := s.deductions[key]; ok {
		return prev, nil
	}

	item, ok := s.items[productID]
	if !ok {
		return 0, ErrNotFound
	}

	if item.GetStock() < qty {
		return 0, ErrInsufficient
	}
	item.Stock -= qty

	s.deductions[key] = item.GetStock()

	return item.GetStock(), nil
}

func (s *MemoryStore) Restore(ctx context.Context, productID string, qty int32, key string) (int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if prev, ok := s.restores[key]; ok {
		return prev, nil
	}

	item, ok := s.items[productID]
	if !ok {
		return 0, ErrNotFound
	}
	item.Stock += qty

	s.restores[key] = item.GetStock()

	return item.GetStock(), nil
}

func (s *MemoryStore) Seed(ctx context.Context, productID, name string, stock int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[productID] = &pb.GetProductResponse{ProductId: productID, Name: name, Stock: stock}

	return nil
}
