package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "example.com/shop/proto/gen/shop/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeOrders struct {
	pb.UnimplementedOrderServiceServer
}

func (f *fakeOrders) CreateOrder(ctx context.Context, request *pb.CreateOrderRequest) (*pb.CreateOrderResponse, error) {
	if request.GetCustomerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "customer_id is required")
	}
	return &pb.CreateOrderResponse{OrderId: "o1"}, nil
}

func (f *fakeOrders) GetOrder(ctx context.Context, request *pb.GetOrderRequest) (*pb.GetOrderResponse, error) {
	if request.GetOrderId() != "o1" {
		return nil, status.Error(codes.NotFound, "order not found")
	}
	return &pb.GetOrderResponse{
		OrderId:    "o1",
		CustomerId: "c1",
		Status:     "CREATED",
		Items:      []*pb.OrderItem{{ProductId: "p1", Qty: 2}},
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

func serveMux(t *testing.T) *http.ServeMux {
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

	rest := &restServer{
		orders:   pb.NewOrderServiceClient(conn),
		products: pb.NewProductServiceClient(conn),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", rest.createOrder)
	mux.HandleFunc("GET /orders/{id}", rest.getOrder)
	mux.HandleFunc("GET /products/{id}", rest.getProduct)
	return mux
}

func TestRest_CreateGetOrder(t *testing.T) {
	mux := serveMux(t)

	created := httptest.NewRecorder()
	mux.ServeHTTP(created, httptest.NewRequest(http.MethodPost, "/orders",
		strings.NewReader(`{"customer_id":"c1","items":[{"product_id":"p1","qty":2}]}`)))
	if created.Code != http.StatusCreated {
		t.Fatalf("POST /orders = %d, want 201", created.Code)
	}
	var body struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &body); err != nil || body.OrderID != "o1" {
		t.Fatalf("POST /orders body = %q, want o1", created.Body.String())
	}

	got := httptest.NewRecorder()
	mux.ServeHTTP(got, httptest.NewRequest(http.MethodGet, "/orders/o1", nil))
	if got.Code != http.StatusOK {
		t.Fatalf("GET /orders/o1 = %d, want 200", got.Code)
	}
	if !strings.Contains(got.Body.String(), `"status":"CREATED"`) {
		t.Fatalf("GET /orders/o1 body = %s, want CREATED", got.Body.String())
	}
}

func TestRest_StatusCodes(t *testing.T) {
	mux := serveMux(t)

	for path, want := range map[string]int{
		"/orders/missing":   http.StatusNotFound,
		"/products/missing": http.StatusNotFound,
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != want {
			t.Fatalf("GET %s = %d, want %d", path, rec.Code, want)
		}
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/orders",
		strings.NewReader(`{"customer_id":"","items":[]}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST /orders empty = %d, want 400", rec.Code)
	}
}
