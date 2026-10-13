package main

import (
	"context"
	"net"
	"sync"
	"testing"

	pb "example.com/shop/proto/gen/shop/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeProducts is a configurable in-memory ProductService for Saga tests.
// Like gateway/fake_test.go in go-realtime-chat, the caller side owns its
// fake: stock sets the starting level, fail forces a code per product.
type fakeProducts struct {
	pb.UnimplementedProductServiceServer
	mu    sync.Mutex
	stock map[string]int32
	fail  map[string]codes.Code
	// seenTrace records the incoming trace ID per DeductStock call,
	// proving the trace propagated order -> product.
	seenTrace []string
}

func (f *fakeProducts) DeductStock(ctx context.Context, request *pb.DeductStockRequest) (*pb.DeductStockResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.seenTrace = append(f.seenTrace, trace.SpanFromContext(ctx).SpanContext().TraceID().String())
	if code, ok := f.fail[request.GetProductId()]; ok {
		return nil, status.Error(code, "forced failure")
	}
	left, ok := f.stock[request.GetProductId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "product not found")
	}
	if left < request.GetQty() {
		return nil, status.Error(codes.FailedPrecondition, "insufficient stock")
	}
	f.stock[request.GetProductId()] = left - request.GetQty()
	return &pb.DeductStockResponse{ProductId: request.GetProductId(), Remaining: left - request.GetQty()}, nil
}

func (f *fakeProducts) RestoreStock(ctx context.Context, request *pb.RestoreStockRequest) (*pb.RestoreStockResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.stock[request.GetProductId()] += request.GetQty()
	return &pb.RestoreStockResponse{ProductId: request.GetProductId(), Remaining: f.stock[request.GetProductId()]}, nil
}

// fakeEvents records announcements and optionally fails them.
type fakeEvents struct {
	mu        sync.Mutex
	published []OrderCreated
	err       error
}

func (f *fakeEvents) PublishOrderCreated(ctx context.Context, event OrderCreated) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, event)
	return nil
}

// dial starts an order Server wired to a fake product backend and a fake
// event bus, and returns the order client plus both fakes for assertions.
func dial(t *testing.T, stock map[string]int32, fail map[string]codes.Code) (pb.OrderServiceClient, *fakeProducts, *fakeEvents) {
	t.Helper()
	fake := &fakeProducts{stock: stock, fail: fail}
	events := &fakeEvents{}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	pb.RegisterProductServiceServer(srv, fake)

	products, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		t.Fatalf("dial product: %v", err)
	}
	t.Cleanup(func() { products.Close() })
	pb.RegisterOrderServiceServer(srv, NewServer(pb.NewProductServiceClient(products), events))

	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		t.Fatalf("dial order: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewOrderServiceClient(conn), fake, events
}

func TestCreate_GetOrder(t *testing.T) {
	c, _, _ := dial(t, map[string]int32{"p1": 10}, nil)
	ctx := context.Background()

	created, err := c.CreateOrder(ctx, &pb.CreateOrderRequest{
		CustomerId: "c1",
		Items:      []*pb.OrderItem{{ProductId: "p1", Qty: 2}},
	})
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	if created.GetOrderId() == "" {
		t.Fatalf("CreateOrder returned empty order_id")
	}

	got, err := c.GetOrder(ctx, &pb.GetOrderRequest{OrderId: created.GetOrderId()})
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if got.GetCustomerId() != "c1" || got.GetStatus() != "CREATED" || len(got.GetItems()) != 1 {
		t.Fatalf("GetOrder = %+v, want c1/CREATED/1 item", got)
	}
}

func TestGetOrder_NotFound(t *testing.T) {
	c, _, _ := dial(t, nil, nil)

	if _, err := c.GetOrder(context.Background(), &pb.GetOrderRequest{OrderId: "o999"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetOrder unknown code = %v, want NotFound", status.Code(err))
	}
}

func TestCreateOrder_Invalid(t *testing.T) {
	c, _, _ := dial(t, nil, nil)
	ctx := context.Background()

	for _, req := range []*pb.CreateOrderRequest{
		{Items: []*pb.OrderItem{{ProductId: "p1", Qty: 1}}},
		{CustomerId: "c1"},
		{CustomerId: "c1", Items: []*pb.OrderItem{{Qty: 1}}},
		{CustomerId: "c1", Items: []*pb.OrderItem{{ProductId: "p1", Qty: 0}}},
	} {
		if _, err := c.CreateOrder(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("CreateOrder %+v code = %v, want InvalidArgument", req, status.Code(err))
		}
	}
}

func TestCreateOrder_SagaOk(t *testing.T) {
	c, fake, _ := dial(t, map[string]int32{"p1": 10, "p2": 5}, nil)
	ctx := context.Background()

	created, err := c.CreateOrder(ctx, &pb.CreateOrderRequest{
		CustomerId: "c1",
		Items:      []*pb.OrderItem{{ProductId: "p1", Qty: 2}, {ProductId: "p2", Qty: 1}},
	})
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}

	got, err := c.GetOrder(ctx, &pb.GetOrderRequest{OrderId: created.GetOrderId()})
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if got.GetStatus() != "CREATED" {
		t.Fatalf("order status = %q, want CREATED", got.GetStatus())
	}
	if fake.stock["p1"] != 8 || fake.stock["p2"] != 4 {
		t.Fatalf("fake stock = %v, want p1:8 p2:4", fake.stock)
	}
}

func TestCreateOrder_Compensate(t *testing.T) {
	c, fake, _ := dial(t, map[string]int32{"p1": 10, "p2": 1}, nil)
	ctx := context.Background()

	_, err := c.CreateOrder(ctx, &pb.CreateOrderRequest{
		CustomerId: "c1",
		Items:      []*pb.OrderItem{{ProductId: "p1", Qty: 2}, {ProductId: "p2", Qty: 5}},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateOrder code = %v, want FailedPrecondition", status.Code(err))
	}

	// Each dial starts a fresh Server, so the failed order is o1.
	got, err := c.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "o1"})
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if got.GetStatus() != "CANCELLED" {
		t.Fatalf("order status = %q, want CANCELLED", got.GetStatus())
	}
	if fake.stock["p1"] != 10 || fake.stock["p2"] != 1 {
		t.Fatalf("fake stock = %v, want p1:10 p2:1 (restored)", fake.stock)
	}
}

func TestCreateOrder_ProductDown(t *testing.T) {
	c, fake, _ := dial(t, map[string]int32{"p1": 10}, map[string]codes.Code{"p1": codes.Unavailable})
	ctx := context.Background()

	if _, err := c.CreateOrder(ctx, &pb.CreateOrderRequest{
		CustomerId: "c1",
		Items:      []*pb.OrderItem{{ProductId: "p1", Qty: 1}},
	}); status.Code(err) != codes.Unavailable {
		t.Fatalf("CreateOrder code = %v, want Unavailable", status.Code(err))
	}

	got, err := c.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "o1"})
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if got.GetStatus() != "CANCELLED" {
		t.Fatalf("order status = %q, want CANCELLED", got.GetStatus())
	}
	if fake.stock["p1"] != 10 {
		t.Fatalf("fake stock p1 = %d, want 10", fake.stock["p1"])
	}
}

func TestCreateOrder_Publishes(t *testing.T) {
	c, _, events := dial(t, map[string]int32{"p1": 10}, nil)
	ctx := context.Background()

	created, err := c.CreateOrder(ctx, &pb.CreateOrderRequest{
		CustomerId: "c1",
		Items:      []*pb.OrderItem{{ProductId: "p1", Qty: 1}},
	})
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	if len(events.published) != 1 {
		t.Fatalf("published = %d, want 1", len(events.published))
	}
	if events.published[0].OrderID != created.GetOrderId() {
		t.Fatalf("published order = %q, want %q", events.published[0].OrderID, created.GetOrderId())
	}
}

func TestCreateOrder_PublishFails(t *testing.T) {
	c, _, events := dial(t, map[string]int32{"p1": 10}, nil)
	events.err = status.Error(codes.Unavailable, "bus down")
	ctx := context.Background()

	created, err := c.CreateOrder(ctx, &pb.CreateOrderRequest{
		CustomerId: "c1",
		Items:      []*pb.OrderItem{{ProductId: "p1", Qty: 1}},
	})
	if err != nil {
		t.Fatalf("CreateOrder with dead bus: %v, want success", err)
	}
	if created.GetOrderId() == "" {
		t.Fatalf("CreateOrder returned empty order_id")
	}
}

func TestCreateOrder_PropagatesTrace(t *testing.T) {
	// Real SDK so spans carry valid IDs; without it every context is
	// a noop and propagation is unobservable.
	provider := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	otel.SetTextMapPropagator(propagation.TraceContext{})

	c, fake, _ := dial(t, map[string]int32{"p1": 10}, nil)

	tracer := otel.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test-create")
	defer span.End()
	want := span.SpanContext().TraceID().String()

	if _, err := c.CreateOrder(ctx, &pb.CreateOrderRequest{
		CustomerId: "c1",
		Items:      []*pb.OrderItem{{ProductId: "p1", Qty: 1}},
	}); err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	if len(fake.seenTrace) != 1 {
		t.Fatalf("fake saw %d calls, want 1", len(fake.seenTrace))
	}
	if fake.seenTrace[0] != want {
		t.Fatalf("product saw trace %q, want %q", fake.seenTrace[0], want)
	}
}
