package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	pb "example.com/shop/proto/gen/shop/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Retry policy for product calls: transient failures get a few more
// chances under the same idempotency key, so a retry replays instead of
// double-deducting. Permanent failures return immediately.
const (
	deductAttempts = 3
	deductBackoff  = 50 * time.Millisecond
	deductTimeout  = 2 * time.Second
)

// retryable reports whether another attempt could succeed. NotFound,
// InvalidArgument, and FailedPrecondition describe the request or the
// stock, not the network, so retrying is pointless.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// deductOne deducts a single item with bounded retries. The key never
// changes across attempts, so the product ledger stays exact-once no
// matter how many times the network forces a replay.
func (s *Server) deductOne(ctx context.Context, id string, item *pb.OrderItem) error {
	request := &pb.DeductStockRequest{
		ProductId:      item.GetProductId(),
		Qty:            item.GetQty(),
		IdempotencyKey: id + ":" + item.GetProductId(),
	}

	var err error
	for attempt := range deductAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(deductBackoff):
			}
		}
		attemptCtx, cancel := context.WithTimeout(ctx, deductTimeout)
		_, err = s.products.DeductStock(attemptCtx, request)
		cancel()
		if err == nil || !retryable(err) {
			return err
		}
	}

	return err
}

// Server implements OrderService over a Store, deducting stock through the
// injected product client, so tests use bufconn while production dials the
// network.
type Server struct {
	pb.UnimplementedOrderServiceServer
	store    Store
	products pb.ProductServiceClient
	// events is nil-tolerant: nil means eventing disabled (local runs
	// without NATS). A publish failure never fails the order.
	events Publisher
}

// NewServer returns an order Server over the given Store.
func NewServer(store Store, products pb.ProductServiceClient, events Publisher) *Server {
	return &Server{
		store:    store,
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

	n, err := s.store.NextID(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	id := fmt.Sprintf("o%d", n)

	stored := Order{
		OrderID:         id,
		CustomerID:      request.GetCustomerId(),
		Items:           request.GetItems(),
		Status:          "CREATED",
		CreatedAtUnixMs: time.Now().UnixMilli(),
	}

	if err := s.store.Create(ctx, stored); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	// One span per order attempt, wrapping the whole Saga. The product
	// calls below inherit ctx, so the trace continues order -> product
	// without any manual header handling.
	ctx, span := otel.Tracer("example.com/shop/services/order").Start(ctx, "CreateOrder")
	defer span.End()
	span.SetAttributes(attribute.String("order.id", id))

	// Saga: deduct each item under a key derived from the order so a
	// retried CreateOrder replays instead of double-deducting. The
	// caller ctx propagates, carrying the deadline bff -> order -> product.
	var deducted []*pb.OrderItem
	for _, item := range request.GetItems() {
		err := s.deductOne(ctx, id, item)
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
			CreatedAtUnixMs: stored.CreatedAtUnixMs,
		}); err != nil {
			log.Printf("trace_id=%s publish %s %s: %v", span.SpanContext().TraceID(), OrderCreatedSubject, id, err)
		}
	}

	return &pb.CreateOrderResponse{
		OrderId:         id,
		CreatedAtUnixMs: stored.CreatedAtUnixMs,
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

	_ = s.store.SetStatus(ctx, id, "CANCELLED")
}

func (s *Server) GetOrder(ctx context.Context, request *pb.GetOrderRequest) (*pb.GetOrderResponse, error) {
	if request.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	}
	stored, err := s.store.Get(ctx, request.GetOrderId())
	if errors.Is(err, ErrOrderNotFound) {
		return nil, status.Error(codes.NotFound, "order not found")
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	return &pb.GetOrderResponse{
		OrderId:         stored.OrderID,
		CustomerId:      stored.CustomerID,
		Items:           stored.Items,
		Status:          stored.Status,
		CreatedAtUnixMs: stored.CreatedAtUnixMs,
	}, nil
}
