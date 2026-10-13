package graph

import (
	pb "example.com/shop/proto/gen/shop/v1"
)

// Resolver holds the three gRPC clients. No business logic lives here or
// in any resolver: dial, translate, assemble, nothing else.
type Resolver struct {
	Orders   pb.OrderServiceClient
	Products pb.ProductServiceClient
	Users    pb.UserServiceClient
}
