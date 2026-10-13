package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	pb "example.com/shop/proto/gen/shop/v1"
)

// ErrOrderNotFound means the order id is unknown. Servers map it to
// gRPC NotFound; the store itself never imports grpc.
var ErrOrderNotFound = errors.New("order not found")

// Order is the stored shape: identity plus items. Status moves
// CREATED -> CANCELLED on compensation and never moves back.
type Order struct {
	OrderID         string
	CustomerID      string
	Items           []*pb.OrderItem
	Status          string
	CreatedAtUnixMs int64
}

// Store is the order persistence boundary. MemoryStore serves tests and
// local runs; PGStore serves order_db. Only order tables exist behind it.
type Store interface {
	// NextID issues the next order number. Numbers never repeat.
	NextID(ctx context.Context) (int64, error)
	Create(ctx context.Context, order Order) error
	Get(ctx context.Context, orderID string) (Order, error)
	SetStatus(ctx context.Context, orderID, status string) error
}

// MemoryStore keeps orders in a mutex-guarded map.
type MemoryStore struct {
	mu     sync.Mutex
	orders map[string]Order
	nextID atomic.Int64
}

// NewMemoryStore returns an empty order Store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{orders: make(map[string]Order)}
}

// NextID issues the next order number. Atomic, like the chat event_id:
// numbers never repeat, though write order is not guaranteed.
func (s *MemoryStore) NextID(ctx context.Context) (int64, error) {
	return s.nextID.Add(1), nil
}

func (s *MemoryStore) Create(ctx context.Context, order Order) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.orders[order.OrderID] = order

	return nil
}

func (s *MemoryStore) Get(ctx context.Context, orderID string) (Order, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	order, ok := s.orders[orderID]
	if !ok {
		return Order{}, ErrOrderNotFound
	}

	return order, nil
}

func (s *MemoryStore) SetStatus(ctx context.Context, orderID, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	order, ok := s.orders[orderID]
	if !ok {
		return ErrOrderNotFound
	}

	order.Status = status
	s.orders[orderID] = order

	return nil
}
