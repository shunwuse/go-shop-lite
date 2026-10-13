package main

import (
	"context"
	"flag"
	"log"
	"os"

	pb "example.com/shop/proto/gen/shop/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// seed verifies the demo baseline is reachable and prints it. Product and
// user creation have no RPC yet, so today it is read-only: the catalog it
// shows is whatever the services were born with. Once the split-DB pack
// lands, this same entrypoint grows real writes and stays idempotent.
func main() {
	productAddr := envOr("PRODUCT_ADDR", "localhost:50052")
	userAddr := envOr("USER_ADDR", "localhost:50053")
	flag.Parse()

	ctx := context.Background()

	products, err := grpc.NewClient(productAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("seed: dial product: %v", err)
	}
	defer products.Close()

	pc := pb.NewProductServiceClient(products)
	for _, id := range []string{"p1", "p2"} {
		got, err := pc.GetProduct(ctx, &pb.GetProductRequest{ProductId: id})
		if err != nil {
			log.Fatalf("seed: product %s: %v", id, err)
		}
		log.Printf("seed: product %s name=%s stock=%d", got.GetProductId(), got.GetName(), got.GetStock())
	}

	users, err := grpc.NewClient(userAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("seed: dial user: %v", err)
	}
	defer users.Close()

	uc := pb.NewUserServiceClient(users)

	got, err := uc.GetUser(ctx, &pb.GetUserRequest{UserId: "u1"})
	if err != nil {
		log.Fatalf("seed: user u1: %v", err)
	}

	log.Printf("seed: user %s name=%s", got.GetUserId(), got.GetName())
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
