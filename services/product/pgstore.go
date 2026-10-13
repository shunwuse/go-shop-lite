package main

import (
	"context"
	"database/sql"
	"errors"

	pb "example.com/shop/proto/gen/shop/v1"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// PGStore persists the catalog in product_db. Only product tables exist
// here; an order table name will simply not resolve, which is the
// boundary made physical.
type PGStore struct {
	db *sql.DB
}

// OpenPGStore connects to product_db and creates its tables. The DSN must
// carry the product role, which has no rights on any other database.
func OpenPGStore(ctx context.Context, dsn string) (*PGStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	schema := `
CREATE TABLE IF NOT EXISTS products (
  product_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  stock INT NOT NULL,
  category TEXT NOT NULL DEFAULT ''
);
-- Evolution: tables created before category existed gain the column here.
ALTER TABLE products ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS deductions (
  idempotency_key TEXT PRIMARY KEY,
  product_id TEXT NOT NULL,
  remaining INT NOT NULL
);
CREATE TABLE IF NOT EXISTS restores (
  idempotency_key TEXT PRIMARY KEY,
  product_id TEXT NOT NULL,
  remaining INT NOT NULL
);`
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, err
	}

	return &PGStore{db: db}, nil
}

func (s *PGStore) GetProduct(ctx context.Context, productID string) (*pb.GetProductResponse, error) {
	var name, category string
	var stock int32

	err := s.db.QueryRowContext(ctx, `SELECT name, stock, category FROM products WHERE product_id = $1`, productID).Scan(&name, &stock, &category)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return &pb.GetProductResponse{ProductId: productID, Name: name, Stock: stock, Category: category}, nil
}

func (s *PGStore) Deduct(ctx context.Context, productID string, qty int32, key string) (int32, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var replay int32
	err = tx.QueryRowContext(ctx, `SELECT remaining FROM deductions WHERE idempotency_key = $1`, key).Scan(&replay)
	if err == nil {
		return replay, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}

	var stock int32
	err = tx.QueryRowContext(ctx, `SELECT stock FROM products WHERE product_id = $1 FOR UPDATE`, productID).Scan(&stock)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}

	if stock < qty {
		return 0, ErrInsufficient
	}
	remaining := stock - qty

	if _, err := tx.ExecContext(ctx, `UPDATE products SET stock = $1 WHERE product_id = $2`, remaining, productID); err != nil {
		return 0, err
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO deductions (idempotency_key, product_id, remaining) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, key, productID, remaining); err != nil {
		return 0, err
	}

	return remaining, tx.Commit()
}

func (s *PGStore) Restore(ctx context.Context, productID string, qty int32, key string) (int32, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var replay int32
	err = tx.QueryRowContext(ctx, `SELECT remaining FROM restores WHERE idempotency_key = $1`, key).Scan(&replay)
	if err == nil {
		return replay, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}

	var stock int32
	err = tx.QueryRowContext(ctx, `SELECT stock FROM products WHERE product_id = $1 FOR UPDATE`, productID).Scan(&stock)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}

	remaining := stock + qty

	if _, err := tx.ExecContext(ctx, `UPDATE products SET stock = $1 WHERE product_id = $2`, remaining, productID); err != nil {
		return 0, err
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO restores (idempotency_key, product_id, remaining) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, key, productID, remaining); err != nil {
		return 0, err
	}

	return remaining, tx.Commit()
}

func (s *PGStore) Seed(ctx context.Context, productID, name string, stock int32) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO products (product_id, name, stock, category) VALUES ($1, $2, $3, 'fruit')
ON CONFLICT (product_id) DO UPDATE SET name = EXCLUDED.name, stock = EXCLUDED.stock`,
		productID, name, stock)

	return err
}
