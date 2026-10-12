package main

import (
	"context"
	"net"
	"sync"
	"testing"

	pb "example.com/shop/proto/gen/shop/v1"
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
}

func (f *fakeProducts) DeductStock(ctx context.Context, request *pb.DeductStockRequest) (*pb.DeductStockResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

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

// dial starts an order Server wired to a fake product backend and returns
// the order client plus the fake for stock assertions.
func dial(t *testing.T, stock map[string]int32, fail map[string]codes.Code) (pb.OrderServiceClient, *fakeProducts) {
	t.Helper()
	fake := &fakeProducts{stock: stock, fail: fail}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterProductServiceServer(srv, fake)

	products, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial product: %v", err)
	}
	t.Cleanup(func() { products.Close() })
	pb.RegisterOrderServiceServer(srv, NewServer(pb.NewProductServiceClient(products)))

	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial order: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewOrderServiceClient(conn), fake
}

func TestCreate_GetOrder(t *testing.T) {
	c, _ := dial(t, map[string]int32{"p1": 10}, nil)
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
	c, _ := dial(t, nil, nil)

	if _, err := c.GetOrder(context.Background(), &pb.GetOrderRequest{OrderId: "o999"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetOrder unknown code = %v, want NotFound", status.Code(err))
	}
}

func TestCreateOrder_Invalid(t *testing.T) {
	c, _ := dial(t, nil, nil)
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
	c, fake := dial(t, map[string]int32{"p1": 10, "p2": 5}, nil)
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
	c, fake := dial(t, map[string]int32{"p1": 10, "p2": 1}, nil)
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
	c, fake := dial(t, map[string]int32{"p1": 10}, map[string]codes.Code{"p1": codes.Unavailable})
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
