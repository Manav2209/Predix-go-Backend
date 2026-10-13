package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func startLocalPostgres() (string, func(), error) {
	name := "predix-smoke-postgres-" + uuid.NewString()
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", nil, err
	}
	password := hex.EncodeToString(secret)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "run", "--detach", "--rm", "--name", name, "-p", "127.0.0.1::5432", "-e", "POSTGRES_USER=smoke", "-e", "POSTGRES_PASSWORD="+password, "-e", "POSTGRES_DB=predix_smoke", "postgres:16-alpine")
	hideWindow(cmd)
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", nil, fmt.Errorf("start disposable PostgreSQL: %s", out)
	}
	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(cleanupCtx, "docker", "rm", "--force", name)
		hideWindow(cmd)
		if _, err := cmd.CombinedOutput(); err != nil {
			fmt.Printf("Cleanup required for test container %s\n", name)
		}
	}
	cmd = exec.CommandContext(ctx, "docker", "port", name, "5432/tcp")
	hideWindow(cmd)
	out, err := cmd.Output()
	if err != nil {
		cleanup()
		return "", nil, err
	}
	address := strings.TrimSpace(string(out))
	if _, _, err := net.SplitHostPort(address); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("invalid disposable PostgreSQL port")
	}
	dsn := "postgres://smoke:" + password + "@" + address + "/predix_smoke?sslmode=disable"
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	defer pool.Close()
	for ctx.Err() == nil {
		if pool.Ping(ctx) == nil {
			fmt.Println("START disposable PostgreSQL " + name)
			return dsn, cleanup, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	cleanup()
	return "", nil, fmt.Errorf("disposable PostgreSQL did not become ready")
}
