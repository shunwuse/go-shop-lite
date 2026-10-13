package graph

import (
	"context"
	"net"
	"testing"

	"example.com/shop/bff/graph/model"
	pb "example.com/shop/proto/gen/shop/v1"
	"github.com/99designs/gqlgen/client"
	"github.com/99designs/gqlgen/graphql/handler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeOrders struct {
	pb.UnimplementedOrderServiceServer
}

func (f *fakeOrders) GetOrder(ctx context.Context, request *pb.GetOrderRequest) (*pb.GetOrderResponse, error) {
	if request.GetOrderId() != "o1" {
		return nil, status.Error(codes.NotFound, "order not found")
	}
	return &pb.GetOrderResponse{
		OrderId:    "o1",
		CustomerId: "c1",
		Status:     "CREATED",
		Items: []*pb.OrderItem{
			{ProductId: "p1", Qty: 2},
			{ProductId: "gone", Qty: 1},
		},
	}, nil
}

type fakeProducts struct {
	pb.UnimplementedProductServiceServer
}

func (f *fakeProducts) GetProduct(ctx context.Context, request *pb.GetProductRequest) (*pb.GetProductResponse, error) {
	if request.GetProductId() != "p1" {
		return nil, status.Error(codes.NotFound, "product not found")
	}
	return &pb.GetProductResponse{ProductId: "p1", Name: "apple", Stock: 10}, nil
}

func serve(t *testing.T) *client.Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterOrderServiceServer(srv, &fakeOrders{})
	pb.RegisterProductServiceServer(srv, &fakeProducts{})
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

	return client.New(handler.NewDefaultServer(NewExecutableSchema(Config{
		Resolvers: &Resolver{
			Orders:   pb.NewOrderServiceClient(conn),
			Products: pb.NewProductServiceClient(conn),
		},
	})))
}

func TestOrder_Tree(t *testing.T) {
	c := serve(t)

	var resp struct {
		Order struct {
			ID         string
			CustomerID string `json:"customerId"`
			Status     string
			Items      []struct {
				ProductID string `json:"productId"`
				Qty       int
				Product   *struct {
					ID   string
					Name string
				}
			}
		}
	}
	if err := c.Post(`{ order(id: "o1") { id customerId status items { productId qty product { id name } } } }`, &resp); err != nil {
		t.Fatalf("query: %v", err)
	}
	if resp.Order.ID != "o1" || len(resp.Order.Items) != 2 {
		t.Fatalf("order = %+v, want o1 with 2 items", resp.Order)
	}
	got := resp.Order.Items[0].Product
	if got == nil || got.Name != "apple" {
		t.Fatalf("items[0].product = %+v, want apple", resp.Order.Items[0].Product)
	}
	if resp.Order.Items[1].Product != nil {
		t.Fatalf("items[1].product = %+v, want null", resp.Order.Items[1].Product)
	}
}

func TestOrder_NotFound(t *testing.T) {
	c := serve(t)

	var resp struct {
		Order *model.Order
	}
	err := c.Post(`{ order(id: "missing") { id } }`, &resp)
	if err == nil {
		t.Fatalf("query missing: want error")
	}
}
