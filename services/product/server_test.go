package main

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	pb "example.com/shop/proto/gen/shop/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
)

// dial starts an in-memory gRPC server and returns a client for it.
// bufconn carries the traffic in memory, so no real port is used.
func dial(t *testing.T) pb.ProductServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterProductServiceServer(srv, NewServer(NewMemoryStore()))
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

func TestDeductStock_Ok(t *testing.T) {
	c := dial(t)

	resp, err := c.DeductStock(context.Background(), &pb.DeductStockRequest{ProductId: "p1", Qty: 3, IdempotencyKey: "k-ok"})
	if err != nil {
		t.Fatalf("DeductStock: %v", err)
	}
	if resp.GetRemaining() != 7 {
		t.Fatalf("DeductStock remaining = %d, want 7", resp.GetRemaining())
	}

	got, err := c.GetProduct(context.Background(), &pb.GetProductRequest{ProductId: "p1"})
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	if got.GetStock() != 7 {
		t.Fatalf("stock after deduct = %d, want 7", got.GetStock())
	}
}

func TestDeductStock_Idempotent(t *testing.T) {
	c := dial(t)
	ctx := context.Background()

	first, err := c.DeductStock(ctx, &pb.DeductStockRequest{ProductId: "p1", Qty: 2, IdempotencyKey: "k-retry"})
	if err != nil {
		t.Fatalf("DeductStock: %v", err)
	}
	for i := 0; i < 2; i++ {
		replay, err := c.DeductStock(ctx, &pb.DeductStockRequest{ProductId: "p1", Qty: 2, IdempotencyKey: "k-retry"})
		if err != nil {
			t.Fatalf("DeductStock retry #%d: %v", i, err)
		}
		if replay.GetRemaining() != first.GetRemaining() {
			t.Fatalf("retry #%d remaining = %d, want %d", i, replay.GetRemaining(), first.GetRemaining())
		}
	}

	got, err := c.GetProduct(ctx, &pb.GetProductRequest{ProductId: "p1"})
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	if got.GetStock() != 8 {
		t.Fatalf("stock after retried deduct = %d, want 8", got.GetStock())
	}
}

func TestDeductStock_Insufficient(t *testing.T) {
	c := dial(t)
	ctx := context.Background()

	if _, err := c.DeductStock(ctx, &pb.DeductStockRequest{ProductId: "p2", Qty: 99, IdempotencyKey: "k-fail"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeductStock over-draw code = %v, want FailedPrecondition", status.Code(err))
	}

	got, err := c.GetProduct(ctx, &pb.GetProductRequest{ProductId: "p2"})
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	if got.GetStock() != 5 {
		t.Fatalf("stock after failed deduct = %d, want 5", got.GetStock())
	}
}

func TestDeductStock_NotFound(t *testing.T) {
	c := dial(t)

	if _, err := c.DeductStock(context.Background(), &pb.DeductStockRequest{ProductId: "nope", Qty: 1, IdempotencyKey: "k-missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("DeductStock unknown code = %v, want NotFound", status.Code(err))
	}
}

func TestDeductStock_Invalid(t *testing.T) {
	c := dial(t)
	ctx := context.Background()

	for _, req := range []*pb.DeductStockRequest{
		{Qty: 1, IdempotencyKey: "k-bad"},
		{ProductId: "p1", Qty: 1},
		{ProductId: "p1", Qty: 0, IdempotencyKey: "k-zero"},
	} {
		if _, err := c.DeductStock(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("DeductStock %+v code = %v, want InvalidArgument", req, status.Code(err))
		}
	}
}

func TestRestoreStock_RoundTrip(t *testing.T) {
	c := dial(t)
	ctx := context.Background()

	if _, err := c.DeductStock(ctx, &pb.DeductStockRequest{ProductId: "p1", Qty: 4, IdempotencyKey: "k-rt"}); err != nil {
		t.Fatalf("DeductStock: %v", err)
	}
	resp, err := c.RestoreStock(ctx, &pb.RestoreStockRequest{ProductId: "p1", Qty: 4, IdempotencyKey: "k-rt-back"})
	if err != nil {
		t.Fatalf("RestoreStock: %v", err)
	}
	if resp.GetRemaining() != 10 {
		t.Fatalf("RestoreStock remaining = %d, want 10", resp.GetRemaining())
	}
}

func TestRestoreStock_Idempotent(t *testing.T) {
	c := dial(t)
	ctx := context.Background()

	first, err := c.RestoreStock(ctx, &pb.RestoreStockRequest{ProductId: "p1", Qty: 2, IdempotencyKey: "k-rdup"})
	if err != nil {
		t.Fatalf("RestoreStock: %v", err)
	}
	again, err := c.RestoreStock(ctx, &pb.RestoreStockRequest{ProductId: "p1", Qty: 2, IdempotencyKey: "k-rdup"})
	if err != nil {
		t.Fatalf("RestoreStock retry: %v", err)
	}
	if again.GetRemaining() != first.GetRemaining() {
		t.Fatalf("retry remaining = %d, want %d", again.GetRemaining(), first.GetRemaining())
	}
}

func TestRestoreStock_NotFound(t *testing.T) {
	c := dial(t)

	if _, err := c.RestoreStock(context.Background(), &pb.RestoreStockRequest{ProductId: "nope", Qty: 1, IdempotencyKey: "k-missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("RestoreStock unknown code = %v, want NotFound", status.Code(err))
	}
}

func TestGetProduct_Evolution(t *testing.T) {
	c := dial(t)

	resp, err := c.GetProduct(context.Background(), &pb.GetProductRequest{ProductId: "p1"})
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	// New clients see the added field.
	if resp.GetCategory() != "fruit" {
		t.Fatalf("category = %q, want fruit", resp.GetCategory())
	}
	// Old clients (compiled before field 4 existed) never see it, and
	// the wire does not break them: the old fields arrive intact through
	// a shape that lacks the new one. Field numbers frozen is what makes
	// this true; renumbering would silently corrupt instead.
	raw, err := protojson.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var old struct {
		ProductID string `json:"productId"`
		Name      string `json:"name"`
		Stock     int32  `json:"stock"`
	}
	if err := json.Unmarshal(raw, &old); err != nil {
		t.Fatalf("old client decode: %v", err)
	}
	if old.ProductID != "p1" || old.Name != "apple" || old.Stock != 10 {
		t.Fatalf("old client saw %+v, want p1/apple/10", old)
	}
}
