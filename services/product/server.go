package main

import (
	"context"
	"sync"

	pb "example.com/shop/proto/gen/shop/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server is the in-memory ProductService implementation.
// Storage is a map until the split-DB phase wires product_db;
// the shape (lookup by product_id, no order fields) already
// enforces the boundary.
type Server struct {
	pb.UnimplementedProductServiceServer
	mu    sync.Mutex
	items map[string]*pb.GetProductResponse
	// deductions records the first result per idempotency key so
	// retries return the stored result instead of deducting again.
	deductions map[string]*pb.DeductStockResponse
	// restores records the first result per restore key so
	// compensation retries never restore twice.
	restores map[string]*pb.RestoreStockResponse
}

// NewServer returns a Server seeded with two products for solo testing.
func NewServer() *Server {
	return &Server{
		items: map[string]*pb.GetProductResponse{
			"p1": {ProductId: "p1", Name: "apple", Stock: 10},
			"p2": {ProductId: "p2", Name: "banana", Stock: 5},
		},
		deductions: make(map[string]*pb.DeductStockResponse),
		restores:   make(map[string]*pb.RestoreStockResponse),
	}
}

func (s *Server) GetProduct(ctx context.Context, request *pb.GetProductRequest) (*pb.GetProductResponse, error) {
	if request.GetProductId() == "" {
		return nil, status.Error(codes.InvalidArgument, "product_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[request.GetProductId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "product not found")
	}
	return &pb.GetProductResponse{
		ProductId: item.GetProductId(),
		Name:      item.GetName(),
		Stock:     item.GetStock(),
	}, nil
}

// DeductStock subtracts qty from the product stock exactly once per
// idempotency key. Retries with a seen key replay the stored result.
func (s *Server) DeductStock(ctx context.Context, request *pb.DeductStockRequest) (*pb.DeductStockResponse, error) {
	if request.GetProductId() == "" || request.GetIdempotencyKey() == "" || request.GetQty() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "product_id, qty (>0) and idempotency_key are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// The server handler already opened a span from the propagated
	// context; tag it so the order -> product link is visible in Jaeger.
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("product.id", request.GetProductId()),
		attribute.Int("deduct.qty", int(request.GetQty())),
	)

	if prev, ok := s.deductions[request.GetIdempotencyKey()]; ok {
		return &pb.DeductStockResponse{
			ProductId: prev.GetProductId(),
			Remaining: prev.GetRemaining(),
		}, nil
	}

	item, ok := s.items[request.GetProductId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "product not found")
	}
	if item.GetStock() < request.GetQty() {
		return nil, status.Error(codes.FailedPrecondition, "insufficient stock")
	}

	item.Stock -= request.GetQty()
	resp := &pb.DeductStockResponse{
		ProductId: item.GetProductId(),
		Remaining: item.GetStock(),
	}
	s.deductions[request.GetIdempotencyKey()] = resp

	return &pb.DeductStockResponse{
		ProductId: resp.GetProductId(),
		Remaining: resp.GetRemaining(),
	}, nil
}

// RestoreStock adds qty back exactly once per restore key. It is the
// Saga compensation counterpart of DeductStock and shares its shape:
// same validation, same replay rule, separate ledger.
func (s *Server) RestoreStock(ctx context.Context, request *pb.RestoreStockRequest) (*pb.RestoreStockResponse, error) {
	if request.GetProductId() == "" || request.GetIdempotencyKey() == "" || request.GetQty() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "product_id, qty (>0) and idempotency_key are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if prev, ok := s.restores[request.GetIdempotencyKey()]; ok {
		return &pb.RestoreStockResponse{
			ProductId: prev.GetProductId(),
			Remaining: prev.GetRemaining(),
		}, nil
	}

	item, ok := s.items[request.GetProductId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "product not found")
	}

	item.Stock += request.GetQty()
	resp := &pb.RestoreStockResponse{
		ProductId: item.GetProductId(),
		Remaining: item.GetStock(),
	}
	s.restores[request.GetIdempotencyKey()] = resp

	return &pb.RestoreStockResponse{
		ProductId: resp.GetProductId(),
		Remaining: resp.GetRemaining(),
	}, nil
}
