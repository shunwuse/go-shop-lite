package main

import (
	"context"
	"log"
	"net"
	"os"

	"example.com/shop/internal/tracing"
	pb "example.com/shop/proto/gen/shop/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx := context.Background()

	shutdown, err := tracing.Init(ctx, os.Getenv("OTEL_ADDR"))
	if err != nil {
		log.Fatalf("tracing: %v", err)
	}
	defer shutdown(ctx)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	productAddr := os.Getenv("PRODUCT_ADDR")
	if productAddr == "" {
		productAddr = "localhost:50052"
	}

	conn, err := grpc.NewClient(productAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
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

	srv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))

	pb.RegisterOrderServiceServer(srv, NewServer(pb.NewProductServiceClient(conn), events))

	log.Fatal(srv.Serve(lis))
}
