package main

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// crossDB rewrites a DSN to point at another database on the same
// instance, keeping the role. The acceptance is literally this:
// the wrong-DB connection must fail.
func crossDB(dsn, db string) string {
	i := strings.LastIndex(dsn, "/")
	if i < 0 {
		return dsn
	}
	j := strings.Index(dsn[i:], "?")
	if j < 0 {
		return dsn[:i+1] + db
	}
	return dsn[:i+1] + db + dsn[i+j:]
}

// TestCrossDB_Denied proves the product role cannot enter order_db.
// It needs a live instance and skips otherwise:
//
//	TEST_PRODUCT_DSN=... go test ./services/product/ -run TestCrossDB -v
func TestCrossDB_Denied(t *testing.T) {
	dsn := os.Getenv("TEST_PRODUCT_DSN")
	if dsn == "" {
		t.Skip("TEST_PRODUCT_DSN not set")
	}
	db, err := sql.Open("pgx", crossDB(dsn, "order_db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(context.Background()); err == nil {
		t.Fatalf("product role entered order_db: want denial")
	}
}
