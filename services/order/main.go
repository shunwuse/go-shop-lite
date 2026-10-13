package main

import (
	"log"
	"net"
	"os"

	pb "example.com/shop/proto/gen/shop/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	productAddr := os.Getenv("PRODUCT_ADDR")
	if productAddr == "" {
		productAddr = "localhost:50052"
	}

	conn, err := grpc.NewClient(productAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial product: %v", err)
	}

	var events Publisher
	if natsAddr := os.Getenv("NATS_ADDR"); natsAddr != "" {
		publisher, err := NewNATSPublisher(natsAddr)
		if err != nil {
			log.Fatalf("dial nats: %v", err)
		}
		events = publisher
	}

	srv := grpc.NewServer()

	pb.RegisterOrderServiceServer(srv, NewServer(pb.NewProductServiceClient(conn), events))

	log.Fatal(srv.Serve(lis))
}
