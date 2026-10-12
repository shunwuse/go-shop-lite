package main

import (
	"context"
	"sync"

	pb "example.com/shop/proto/gen/shop/v1"
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
}

// NewServer returns a Server seeded with two products for solo testing.
func NewServer() *Server {
	return &Server{
		items: map[string]*pb.GetProductResponse{
			"p1": {ProductId: "p1", Name: "apple", Stock: 10},
			"p2": {ProductId: "p2", Name: "banana", Stock: 5},
		},
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

// DeductStock is a placeholder until the idempotency pack lands.
func (s *Server) DeductStock(ctx context.Context, request *pb.DeductStockRequest) (*pb.DeductStockResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
