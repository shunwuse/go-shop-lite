package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pb "example.com/shop/proto/gen/shop/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server is the in-memory OrderService implementation.
// It keeps its own orders and deducts stock through the injected
// product client, so tests use bufconn while production dials the network.
type Server struct {
	pb.UnimplementedOrderServiceServer
	mu       sync.Mutex
	orders   map[string]*pb.GetOrderResponse
	nextID   atomic.Int64
	products pb.ProductServiceClient
	// events is nil-tolerant: nil means eventing disabled (local runs
	// without NATS). A publish failure never fails the order.
	events Publisher
}

// NewServer returns an order Server that deducts through products and
// announces success through events (nil disables announcing).
func NewServer(products pb.ProductServiceClient, events Publisher) *Server {
	return &Server{
		orders:   make(map[string]*pb.GetOrderResponse),
		products: products,
		events:   events,
	}
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

	if s.products == nil {
		return nil, status.Error(codes.Unavailable, "product client not wired")
	}

	id := fmt.Sprintf("o%d", s.nextID.Add(1))

	// One span per order attempt, wrapping the whole Saga. The product
	// calls below inherit ctx, so the trace continues order -> product
	// without any manual header handling.
	ctx, span := otel.Tracer("example.com/shop/services/order").Start(ctx, "CreateOrder")
	defer span.End()
	span.SetAttributes(attribute.String("order.id", id))

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

	// Saga: deduct each item under a key derived from the order so a
	// retried CreateOrder replays instead of double-deducting. The
	// caller ctx propagates, carrying the deadline bff -> order -> product.
	var deducted []*pb.OrderItem
	for _, item := range request.GetItems() {
		_, err := s.products.DeductStock(ctx, &pb.DeductStockRequest{
			ProductId:      item.GetProductId(),
			Qty:            item.GetQty(),
			IdempotencyKey: id + ":" + item.GetProductId(),
		})
		if err != nil {
			s.compensate(ctx, id, deducted)
			code := status.Code(err)
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "deduct failed")
			return nil, status.Errorf(code, "create order %s failed on %s: %s", id, item.GetProductId(), status.Convert(err).Message())
		}

		deducted = append(deducted, item)
	}

	// Announce only after every deduction lands. Publish failure is
	// logged, never fatal: the order is already true, the event is
	// a notification, and conflating the two would flip successes.
	if s.events != nil {
		if err := s.events.PublishOrderCreated(ctx, OrderCreated{
			OrderID:         id,
			CreatedAtUnixMs: stored.GetCreatedAtUnixMs(),
		}); err != nil {
			log.Printf("trace_id=%s publish %s %s: %v", span.SpanContext().TraceID(), OrderCreatedSubject, id, err)
		}
	}

	return &pb.CreateOrderResponse{
		OrderId:         id,
		CreatedAtUnixMs: stored.GetCreatedAtUnixMs(),
	}, nil
}

// compensate restores every prior deduction and flips the order to
// CANCELLED. Restore is best-effort: the order ends CANCELLED either
// way, and restore keys make a retried compensation replay safely.
func (s *Server) compensate(ctx context.Context, id string, deducted []*pb.OrderItem) {
	for _, item := range deducted {
		_, _ = s.products.RestoreStock(ctx, &pb.RestoreStockRequest{
			ProductId:      item.GetProductId(),
			Qty:            item.GetQty(),
			IdempotencyKey: id + ":" + item.GetProductId() + ":restore",
		})
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if stored, ok := s.orders[id]; ok {
		stored.Status = "CANCELLED"
	}
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
