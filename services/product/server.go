package main

import (
	"context"
	"errors"

	pb "example.com/shop/proto/gen/shop/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements ProductService over a Store. Validation and error
// mapping live here; persistence lives behind the interface.
type Server struct {
	pb.UnimplementedProductServiceServer
	store Store
}

// NewServer returns a Server over the given Store.
func NewServer(store Store) *Server {
	return &Server{store: store}
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, "product not found")
	case errors.Is(err, ErrInsufficient):
		return status.Error(codes.FailedPrecondition, "insufficient stock")
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}

func (s *Server) GetProduct(ctx context.Context, request *pb.GetProductRequest) (*pb.GetProductResponse, error) {
	if request.GetProductId() == "" {
		return nil, status.Error(codes.InvalidArgument, "product_id is required")
	}
	item, err := s.store.GetProduct(ctx, request.GetProductId())
	if err != nil {
		return nil, toStatus(err)
	}

	return item, nil
}

// DeductStock subtracts qty from the product stock exactly once per
// idempotency key. Retries with a seen key replay the stored result.
func (s *Server) DeductStock(ctx context.Context, request *pb.DeductStockRequest) (*pb.DeductStockResponse, error) {
	if request.GetProductId() == "" || request.GetIdempotencyKey() == "" || request.GetQty() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "product_id, qty (>0) and idempotency_key are required")
	}

	// The server handler already opened a span from the propagated
	// context; tag it so the order -> product link is visible in Jaeger.
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("product.id", request.GetProductId()),
		attribute.Int("deduct.qty", int(request.GetQty())),
	)

	remaining, err := s.store.Deduct(ctx, request.GetProductId(), request.GetQty(), request.GetIdempotencyKey())
	if err != nil {
		return nil, toStatus(err)
	}

	return &pb.DeductStockResponse{
		ProductId: request.GetProductId(),
		Remaining: remaining,
	}, nil
}

// RestoreStock adds qty back exactly once per restore key. It is the
// Saga compensation counterpart of DeductStock and shares its shape:
// same validation, same replay rule, separate ledger.
func (s *Server) RestoreStock(ctx context.Context, request *pb.RestoreStockRequest) (*pb.RestoreStockResponse, error) {
	if request.GetProductId() == "" || request.GetIdempotencyKey() == "" || request.GetQty() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "product_id, qty (>0) and idempotency_key are required")
	}

	remaining, err := s.store.Restore(ctx, request.GetProductId(), request.GetQty(), request.GetIdempotencyKey())
	if err != nil {
		return nil, toStatus(err)
	}

	return &pb.RestoreStockResponse{
		ProductId: request.GetProductId(),
		Remaining: remaining,
	}, nil
}
