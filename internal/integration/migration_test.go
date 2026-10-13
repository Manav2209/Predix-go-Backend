package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLifecycleMigrationPreservesLegacyRows(t *testing.T) {
	if !dockerReady(t) {
		t.Skip("Docker unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, postgresDSN(startPostgres(t, ctx)))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, name := range []string{"001_create_users.up.sql", "002_create_events_orders.up.sql", "003_create_trading_tables.up.sql"} {
		sql, err := os.ReadFile(filepath.Join("..", "..", "db", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatal(err)
		}
	}
	buyer, seller, event := uuid.New(), uuid.New(), uuid.New()
	for _, user := range []uuid.UUID{buyer, seller} {
		if _, err := pool.Exec(ctx, `INSERT INTO users(id,email,password_hash) VALUES($1,$2,'fixture')`, user, user.String()+"@example.com"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO events(id,title,description,question,thumbnail,expires_at) VALUES($1,'fixture','fixture','fixture','fixture',NOW()+INTERVAL '1 day')`, event); err != nil {
		t.Fatal(err)
	}
	canceled, filled, partial := uuid.New(), uuid.New(), uuid.New()
	for _, row := range []struct {
		id       uuid.UUID
		status   string
		quantity int
	}{{canceled, "CANCELLED", 10}, {filled, "FILLED", 4}, {partial, "PARTIAL", 5}} {
		if _, err := pool.Exec(ctx, `INSERT INTO orders(id,event_id,user_id,order_type,outcome,side,quantity,price,status) VALUES($1,$2,$3,'LIMIT','YES','BUY',$4,0.5,$5)`, row.id, event, buyer, row.quantity, row.status); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO trades(id,event_id,outcome,taker_order_id,maker_order_id,buyer_id,seller_id,taker_side,quantity,price) VALUES($1,$2,'YES',$3,$4,$5,$6,'BUY',2,0.5)`, uuid.New(), event, filled, partial, buyer, seller); err != nil {
		t.Fatal(err)
	}
	sql, err := os.ReadFile(filepath.Join("..", "..", "db", "migrations", "004_order_lifecycle.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(sql)); err != nil {
		t.Fatalf("legacy migration failed: %v", err)
	}
	for _, row := range []struct {
		id              uuid.UUID
		status          string
		fill, remaining int64
	}{{canceled, "CANCELED", 0, 10}, {filled, "FILLED", 4, 0}, {partial, "PARTIAL", 2, 3}} {
		var status string
		var fill, remaining int64
		if err := pool.QueryRow(ctx, `SELECT status,filled_quantity::bigint,remaining_quantity::bigint FROM orders WHERE id=$1`, row.id).Scan(&status, &fill, &remaining); err != nil {
			t.Fatal(err)
		}
		if status != row.status || fill != row.fill || remaining != row.remaining {
			t.Fatalf("migration changed lifecycle incorrectly: %s %d %d", status, fill, remaining)
		}
	}
	var volume int64
	if err := pool.QueryRow(ctx, `SELECT (volume*10000)::bigint FROM events WHERE id=$1`, event).Scan(&volume); err != nil {
		t.Fatal(err)
	}
	if volume != 10000 {
		t.Fatalf("backfilled volume=%d", volume)
	}
	assertBalance(t, ctx, pool, buyer.String(), 1000000)
	assertBalance(t, ctx, pool, seller.String(), 1000000)
	if _, err := pool.Exec(ctx, string(sql)); err != nil {
		t.Fatalf("migration is not safe to retry: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("legacy orders lost: %d %v", count, err)
	}
}
