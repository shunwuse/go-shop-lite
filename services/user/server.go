package main

import (
	"context"
	"sync"

	pb "example.com/shop/proto/gen/shop/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server is the in-memory UserService stub. Auth is out of scope;
// it only proves the user boundary exists with the shared error codes.
type Server struct {
	pb.UnimplementedUserServiceServer
	mu    sync.Mutex
	users map[string]*pb.GetUserResponse
}

// NewServer returns a Server seeded with two users for solo testing.
func NewServer() *Server {
	return &Server{
		users: map[string]*pb.GetUserResponse{
			"u1": {UserId: "u1", Name: "amy"},
			"u2": {UserId: "u2", Name: "bob"},
		},
	}
}

func (s *Server) GetUser(ctx context.Context, request *pb.GetUserRequest) (*pb.GetUserResponse, error) {
	if request.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	user, ok := s.users[request.GetUserId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "user not found")
	}

	return &pb.GetUserResponse{
		UserId: user.GetUserId(),
		Name:   user.GetName(),
	}, nil
}
