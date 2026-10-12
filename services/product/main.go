package main

import (
	"log"
	"net"

	pb "example.com/shop/proto/gen/shop/v1"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()

	pb.RegisterProductServiceServer(srv, NewServer())

	log.Fatal(srv.Serve(lis))
}
