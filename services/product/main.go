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
)

func main() {
	ctx := context.Background()

	shutdown, err := tracing.Init(ctx, os.Getenv("OTEL_ADDR"))
	if err != nil {
		log.Fatalf("tracing: %v", err)
	}
	defer shutdown(ctx)

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))

	pb.RegisterProductServiceServer(srv, NewServer())

	log.Fatal(srv.Serve(lis))
}
