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
func dial(t *testing.T) pb.ProductServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterProductServiceServer(srv, NewServer())
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
	return pb.NewProductServiceClient(conn)
}

func TestGetProduct_Ok(t *testing.T) {
	c := dial(t)

	resp, err := c.GetProduct(context.Background(), &pb.GetProductRequest{ProductId: "p1"})
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	if resp.GetName() != "apple" || resp.GetStock() != 10 {
		t.Fatalf("GetProduct p1 = (%q, %d), want (apple, 10)", resp.GetName(), resp.GetStock())
	}
}

func TestGetProduct_NotFound(t *testing.T) {
	c := dial(t)

	if _, err := c.GetProduct(context.Background(), &pb.GetProductRequest{ProductId: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetProduct unknown code = %v, want NotFound", status.Code(err))
	}
}

func TestGetProduct_Invalid(t *testing.T) {
	c := dial(t)

	if _, err := c.GetProduct(context.Background(), &pb.GetProductRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetProduct empty code = %v, want InvalidArgument", status.Code(err))
	}
}
