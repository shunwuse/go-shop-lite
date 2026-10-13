package main

import (
	"context"
	"database/sql"
	"errors"

	pb "example.com/shop/proto/gen/shop/v1"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// PGStore persists orders in order_db. Only order tables exist here;
// a product table name will simply not resolve, which is the boundary
// made physical.
type PGStore struct {
	db *sql.DB
}

// OpenPGStore connects to order_db and creates its tables. The DSN must
// carry the order role, which has no rights on any other database.
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
CREATE TABLE IF NOT EXISTS orders (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at_unix_ms BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS order_items (
  order_id TEXT NOT NULL REFERENCES orders(order_id),
  product_id TEXT NOT NULL,
  qty INT NOT NULL
);
CREATE SEQUENCE IF NOT EXISTS order_seq;`

	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, err
	}

	return &PGStore{db: db}, nil
}

// NextID draws from a database sequence, so every replica and restart
// shares one numbering.
func (s *PGStore) NextID(ctx context.Context) (int64, error) {
	var n int64

	if err := s.db.QueryRowContext(ctx, `SELECT nextval('order_seq')`).Scan(&n); err != nil {
		return 0, err
	}

	return n, nil
}

func (s *PGStore) Create(ctx context.Context, order Order) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `INSERT INTO orders (order_id, customer_id, status, created_at_unix_ms) VALUES ($1, $2, $3, $4)`,
		order.OrderID, order.CustomerID, order.Status, order.CreatedAtUnixMs); err != nil {
		return err
	}

	for _, item := range order.Items {
		if _, err := tx.ExecContext(ctx, `INSERT INTO order_items (order_id, product_id, qty) VALUES ($1, $2, $3)`,
			order.OrderID, item.GetProductId(), item.GetQty()); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *PGStore) Get(ctx context.Context, orderID string) (Order, error) {
	var order Order

	err := s.db.QueryRowContext(ctx, `SELECT order_id, customer_id, status, created_at_unix_ms FROM orders WHERE order_id = $1`, orderID).
		Scan(&order.OrderID, &order.CustomerID, &order.Status, &order.CreatedAtUnixMs)
	if errors.Is(err, sql.ErrNoRows) {
		return Order{}, ErrOrderNotFound
	}
	if err != nil {
		return Order{}, err
	}

	rows, err := s.db.QueryContext(ctx, `SELECT product_id, qty FROM order_items WHERE order_id = $1`, orderID)
	if err != nil {
		return Order{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var item pb.OrderItem
		if err := rows.Scan(&item.ProductId, &item.Qty); err != nil {
			return Order{}, err
		}
		order.Items = append(order.Items, &item)
	}

	return order, rows.Err()
}

func (s *PGStore) SetStatus(ctx context.Context, orderID, status string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE orders SET status = $1 WHERE order_id = $2`, status, orderID)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrOrderNotFound
	}

	return nil
}
