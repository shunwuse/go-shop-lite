package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"os"

	pb "example.com/shop/proto/gen/shop/v1"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// seed fills product_db with the demo catalog (idempotent upserts) and
// then verifies the baseline over gRPC. SEED_PRODUCT_DSN empty means
// local memory runs: writes are skipped, verification still runs.
func main() {
	productAddr := envOr("PRODUCT_ADDR", "localhost:50052")
	userAddr := envOr("USER_ADDR", "localhost:50053")
	stock := flag.Int("stock", 10000, "stock per demo product")
	flag.Parse()

	ctx := context.Background()

	if dsn := os.Getenv("SEED_PRODUCT_DSN"); dsn != "" {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			log.Fatalf("seed: open product_db: %v", err)
		}
		for _, item := range [][3]any{{"p1", "apple", *stock}, {"p2", "banana", *stock}} {
			if _, err := db.ExecContext(ctx, `INSERT INTO products (product_id, name, stock) VALUES ($1, $2, $3)
ON CONFLICT (product_id) DO UPDATE SET name = EXCLUDED.name, stock = EXCLUDED.stock`,
				item[0], item[1], item[2]); err != nil {
				log.Fatalf("seed: upsert %v: %v", item[0], err)
			}
		}
		db.Close()
		log.Printf("seed: stocked p1/p2 at %d each", *stock)
	}

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
		log.Printf("seed: user u1 unreachable (continuing): %v", err)
		return
	}

	log.Printf("seed: user %s name=%s", got.GetUserId(), got.GetName())
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
