// Command smoke runs the real service binaries against an isolated PostgreSQL
// schema and namespaced Redis streams, then checks every registered API route.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func loadEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = value[1 : len(value)-1]
		}
		if _, exists := os.LookupEnv(key); !exists {
			if err := os.Setenv(key, value); err != nil {
				return err
			}
		}
	}
	return scanner.Err()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	envFile := flag.String("env-file", ".env", "configuration file (credentials are never printed)")
	probe := flag.Bool("probe", false, "check dependencies without starting services")
	localPostgres := flag.Bool("local-postgres", true, "use a disposable local PostgreSQL Docker container")
	flag.Parse()
	if err := loadEnv(*envFile); err != nil {
		return err
	}
	if *localPostgres && !*probe {
		dsn, cleanup, err := startLocalPostgres()
		if err != nil {
			return err
		}
		defer cleanup()
		if err := os.Setenv("DATABASE_URL", dsn); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_URL"), MaxRetries: -1})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Redis connection failed: %w", err)
	}
	fmt.Println("PASS Redis connection")
	if client.Exists(ctx, "engine:lease:0").Val() != 0 {
		return fmt.Errorf("an engine already owns partition 0; stop that engine before this isolated smoke run")
	}
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return fmt.Errorf("invalid PostgreSQL connection configuration")
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("PostgreSQL connection failed: %w", err)
	}
	fmt.Println("PASS PostgreSQL connection")
	if *probe {
		return probeSchema(ctx, pool)
	}
	return exercise(pool, client)
}

func probeSchema(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = current_schema()`)
	if err != nil {
		return fmt.Errorf("read-only schema inspection failed: %w", err)
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			return err
		}
		columns[table+"."+column] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var missing []string
	for _, column := range []string{"users.id", "users.balance", "events.id", "events.volume", "orders.id", "orders.filled_quantity", "orders.remaining_quantity", "trades.id", "positions.shares", "transactions.trade_id"} {
		if !columns[column] {
			missing = append(missing, column)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("configured database is missing required schema columns: %s", strings.Join(missing, ", "))
	}
	fmt.Println("PASS configured database has required application schema columns (read-only check)")
	return nil
}
