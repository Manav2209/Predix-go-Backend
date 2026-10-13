// Command migrate checks or atomically applies the missing lifecycle migration
// to the database explicitly configured in the environment or .env file.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"predix/pkg/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	apply := flag.Bool("apply", false, "apply migration 004; default is a read-only check")
	env := flag.String("env-file", ".env", "database configuration file")
	flag.Parse()
	if err := config.LoadEnvFile(*env); err != nil && !os.IsNotExist(err) {
		return err
	}
	if os.Getenv("DATABASE_URL") == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return fmt.Errorf("invalid database configuration")
	}
	defer pool.Close()
	var present int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND ((table_name='orders' AND column_name IN ('filled_quantity','remaining_quantity')) OR (table_name='events' AND column_name='volume'))`).Scan(&present)
	if err != nil {
		return err
	}
	if present == 3 {
		fmt.Println("PASS migration 004 columns are present")
		return nil
	}
	if !*apply {
		return fmt.Errorf("migration 004 is required (%d of 3 lifecycle columns present); apply with go run ./scripts/migrate -apply", present)
	}
	sql, err := os.ReadFile("db/migrations/004_order_lifecycle.up.sql")
	if err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '30s'; SELECT pg_advisory_xact_lock(hashtext('predix:migration:004'))`); err != nil {
		return err
	}
	counts := map[string]int64{}
	for _, table := range []string{"users", "events", "orders", "trades", "positions", "transactions"} {
		var count int64
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil {
			return err
		}
		counts[table] = count
	}
	if _, err := tx.Exec(ctx, string(sql)); err != nil {
		return fmt.Errorf("migration rolled back: %w", err)
	}
	for table, before := range counts {
		var after int64
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&after); err != nil {
			return err
		}
		if before != after {
			return fmt.Errorf("migration changed row count for %s; rolling back", table)
		}
	}
	var invalid int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM orders WHERE filled_quantity < 0 OR filled_quantity > quantity OR remaining_quantity <> quantity - filled_quantity`).Scan(&invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("migration found %d inconsistent order fills; rolling back", invalid)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	fmt.Println("PASS migration 004 applied atomically; existing row counts preserved")
	return nil
}
