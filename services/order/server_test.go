package main

import (
	"context"
	"net"
	"testing"

	pb "example.com/shop/proto/gen/shop/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// dial starts an in-memory gRPC server and returns a client for it.
// bufconn carries the traffic in memory, so no real port is used.
func dial(t *testing.T) pb.OrderServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterOrderServiceServer(srv, NewServer())
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewOrderServiceClient(conn)
}

func TestCreate_GetOrder(t *testing.T) {
	c := dial(t)
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
	c := dial(t)

	if _, err := c.GetOrder(context.Background(), &pb.GetOrderRequest{OrderId: "o999"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetOrder unknown code = %v, want NotFound", status.Code(err))
	}
}

func TestCreateOrder_Invalid(t *testing.T) {
	c := dial(t)
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
