package main

import (
	"encoding/json"
	"net/http"

	pb "example.com/shop/proto/gen/shop/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// restServer speaks resource-style REST over the same gRPC clients the
// GraphQL resolvers use. One endpoint returns one resource on purpose:
// clients needing the whole tree make 1+N calls, which is exactly the
// contrast the GraphQL door exists to beat. No assembly across services
// here, no business logic either.
type restServer struct {
	orders   pb.OrderServiceClient
	products pb.ProductServiceClient
}

func restStatus(err error) int {
	switch status.Code(err) {
	case codes.NotFound:
		return http.StatusNotFound
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.FailedPrecondition:
		return http.StatusConflict
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func writeError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(restStatus(err))
	_ = json.NewEncoder(w).Encode(map[string]string{"error": status.Convert(err).Message()})
}

// POST /orders {"customer_id": "...", "items": [{"product_id": "...", "qty": n}]}
func (s *restServer) createOrder(w http.ResponseWriter, r *http.Request) {
	var body struct {
		CustomerID string `json:"customer_id"`
		Items      []struct {
			ProductID string `json:"product_id"`
			Qty       int32  `json:"qty"`
		} `json:"items"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	items := make([]*pb.OrderItem, 0, len(body.Items))
	for _, item := range body.Items {
		items = append(items, &pb.OrderItem{ProductId: item.ProductID, Qty: item.Qty})
	}

	created, err := s.orders.CreateOrder(r.Context(), &pb.CreateOrderRequest{
		CustomerId: body.CustomerID,
		Items:      items,
	})
	if err != nil {
		writeError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(map[string]string{"order_id": created.GetOrderId()})
}

// GET /orders/{id} returns the order only, without product details.
// Need names? Call GET /products/{id} per item: that is the 1+N.
func (s *restServer) getOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	got, err := s.orders.GetOrder(r.Context(), &pb.GetOrderRequest{OrderId: id})
	if err != nil {
		writeError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	raw, err := protojson.Marshal(got)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(raw)
}

// GET /products/{id} returns one product.
func (s *restServer) getProduct(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	got, err := s.products.GetProduct(r.Context(), &pb.GetProductRequest{ProductId: id})
	if err != nil {
		writeError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	raw, err := protojson.Marshal(got)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(raw)
}
