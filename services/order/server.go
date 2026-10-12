package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "example.com/shop/proto/gen/shop/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server is the in-memory OrderService implementation.
// It keeps its own orders only and knows nothing about products;
// the Saga pack will wire the product client in.
type Server struct {
	pb.UnimplementedOrderServiceServer
	mu     sync.Mutex
	orders map[string]*pb.GetOrderResponse
	nextID atomic.Int64
}

// NewServer returns an empty order Server.
func NewServer() *Server {
	return &Server{orders: make(map[string]*pb.GetOrderResponse)}
}

func (s *Server) CreateOrder(ctx context.Context, request *pb.CreateOrderRequest) (*pb.CreateOrderResponse, error) {
	if request.GetCustomerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "customer_id is required")
	}
	if len(request.GetItems()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one item is required")
	}
	for _, item := range request.GetItems() {
		if item.GetProductId() == "" || item.GetQty() <= 0 {
			return nil, status.Error(codes.InvalidArgument, "every item needs product_id and qty (>0)")
		}
	}

	id := fmt.Sprintf("o%d", s.nextID.Add(1))
	stored := &pb.GetOrderResponse{
		OrderId:         id,
		CustomerId:      request.GetCustomerId(),
		Items:           request.GetItems(),
		Status:          "CREATED",
		CreatedAtUnixMs: time.Now().UnixMilli(),
	}

	s.mu.Lock()
	s.orders[id] = stored
	s.mu.Unlock()

	return &pb.CreateOrderResponse{
		OrderId:         id,
		CreatedAtUnixMs: stored.GetCreatedAtUnixMs(),
	}, nil
}

func (s *Server) GetOrder(ctx context.Context, request *pb.GetOrderRequest) (*pb.GetOrderResponse, error) {
	if request.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.orders[request.GetOrderId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "order not found")
	}

	return &pb.GetOrderResponse{
		OrderId:         stored.GetOrderId(),
		CustomerId:      stored.GetCustomerId(),
		Items:           stored.GetItems(),
		Status:          stored.GetStatus(),
		CreatedAtUnixMs: stored.GetCreatedAtUnixMs(),
	}, nil
}
