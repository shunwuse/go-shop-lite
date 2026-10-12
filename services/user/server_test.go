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
func dial(t *testing.T) pb.UserServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterUserServiceServer(srv, NewServer())
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
	return pb.NewUserServiceClient(conn)
}

func TestGetUser_Ok(t *testing.T) {
	c := dial(t)

	resp, err := c.GetUser(context.Background(), &pb.GetUserRequest{UserId: "u1"})
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if resp.GetName() != "amy" {
		t.Fatalf("GetUser u1 name = %q, want amy", resp.GetName())
	}
}

func TestGetUser_NotFound(t *testing.T) {
	c := dial(t)

	if _, err := c.GetUser(context.Background(), &pb.GetUserRequest{UserId: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetUser unknown code = %v, want NotFound", status.Code(err))
	}
}

func TestGetUser_Invalid(t *testing.T) {
	c := dial(t)

	if _, err := c.GetUser(context.Background(), &pb.GetUserRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetUser empty code = %v, want InvalidArgument", status.Code(err))
	}
}
