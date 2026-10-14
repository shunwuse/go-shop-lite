package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"example.com/shop/bff/graph"
	"example.com/shop/internal/tracing"
	pb "example.com/shop/proto/gen/shop/v1"
	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// toGraphQLError passes the gRPC code through with bff context attached.
// Resolvers never translate meaning, only envelopes: NotFound stays
// NotFound, so callers read the same contract over both protocols.
func toGraphQLError(ctx context.Context, err error) *gqlerror.Error {
	_ = ctx

	msg := status.Convert(err).Message()

	var code string

	switch status.Code(err) {
	case codes.NotFound:
		code = "NOT_FOUND"
	case codes.InvalidArgument:
		code = "BAD_USER_INPUT"
	case codes.FailedPrecondition:
		code = "FAILED_PRECONDITION"
	case codes.Unavailable:
		code = "UNAVAILABLE"
	default:
		code = "INTERNAL"
	}

	return &gqlerror.Error{Message: msg, Extensions: map[string]any{"code": code}}
}

func dial(addr string) *grpc.ClientConn {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatalf("bff: dial %s: %v", addr, err)
	}

	return conn
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}

func main() {
	ctx := context.Background()
	shutdown, err := tracing.Init(ctx, os.Getenv("OTEL_ADDR"), "bff")
	if err != nil {
		log.Fatalf("bff: tracing: %v", err)
	}
	defer shutdown(ctx)

	orders := dial(envOr("ORDER_ADDR", "localhost:50051"))
	products := dial(envOr("PRODUCT_ADDR", "localhost:50052"))
	users := dial(envOr("USER_ADDR", "localhost:50053"))

	srv := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{
		Resolvers: &graph.Resolver{
			Orders:   pb.NewOrderServiceClient(orders),
			Products: pb.NewProductServiceClient(products),
			Users:    pb.NewUserServiceClient(users),
		},
	}))

	srv.SetErrorPresenter(toGraphQLError)
	// The trace starts here: everything downstream joins this span.
	srv.AroundResponses(func(ctx context.Context, next graphql.ResponseHandler) *graphql.Response {
		ctx, span := otel.Tracer("example.com/shop/bff").Start(ctx, "graphql",
			trace.WithAttributes(attribute.String("operation", graphql.GetOperationContext(ctx).OperationName)),
		)
		defer span.End()
		return next(ctx)
	})

	http.Handle("/", playground.Handler("GraphQL playground", "/query"))
	http.Handle("/query", srv)

	rest := &restServer{
		orders:   pb.NewOrderServiceClient(orders),
		products: pb.NewProductServiceClient(products),
	}
	http.HandleFunc("POST /orders", rest.createOrder)
	http.HandleFunc("GET /orders/{id}", rest.getOrder)
	http.HandleFunc("GET /products/{id}", rest.getProduct)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
