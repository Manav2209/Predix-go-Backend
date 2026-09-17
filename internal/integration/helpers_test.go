package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"predix/internal/engine"
	"predix/internal/events"
	"predix/internal/partition"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// dockerReady reports whether a usable Docker daemon exists. Testcontainers
// tests self-skip when it does not.
func dockerReady(t *testing.T) bool {
	t.Helper()

	if os.Getenv("TESTCONTAINERS_DISABLE") != "" {
		return false
	}

	cmd := exec.Command("docker", "version", "--format", "{{.Server.Version}}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("docker not ready: %v (%s)", err, strings.TrimSpace(string(out)))
		return false
	}

	return true
}

// startPostgres runs a disposable Postgres 16 and returns its host:port.
func startPostgres(t *testing.T, ctx context.Context) string {
	t.Helper()

	container, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "postgres:16-alpine",
				ExposedPorts: []string{"5432/tcp"},
				Env: map[string]string{
					"POSTGRES_USER":     "test",
					"POSTGRES_PASSWORD": "test",
					"POSTGRES_DB":       "predix",
				},
				WaitingFor: wait.ForAll(
					wait.ForLog("database system is ready to accept connections").
						WithStartupTimeout(90*time.Second),
					wait.ForListeningPort("5432/tcp").
						WithStartupTimeout(90*time.Second),
				),
			},
			Started: true,
		})
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}

	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}

	return fmt.Sprintf("%s:%s", host, port.Port())
}

// startRedis runs a disposable Redis 7 and returns its host:port.
func startRedis(t *testing.T, ctx context.Context) string {
	t.Helper()

	container, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "redis:7-alpine",
				ExposedPorts: []string{"6379/tcp"},
				WaitingFor: wait.ForAll(
					wait.ForLog("Ready to accept connections").
						WithStartupTimeout(90*time.Second),
					wait.ForListeningPort("6379/tcp").
						WithStartupTimeout(90*time.Second),
				),
			},
			Started: true,
		})
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}

	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatal(err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}

	return fmt.Sprintf("%s:%s", host, port.Port())
}

func postgresDSN(addr string) string {
	return "postgres://test:test@" + addr + "/predix?sslmode=disable"
}

// runMigrations applies every *.up.sql migration in order.
func runMigrations(t *testing.T, pgAddr string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, postgresDSN(pgAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	dir, err := filepath.Abs(filepath.Join("..", "..", "db", "migrations"))
	if err != nil {
		t.Fatal(err)
	}

	paths, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil {
		t.Fatal(err)
	}

	sort.Strings(paths)

	for _, p := range paths {
		sqlBytes, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := pool.Exec(ctx, string(sqlBytes)); err != nil {
			t.Fatalf("migration %s: %v", filepath.Base(p), err)
		}
	}
}

// waitFor polls cond until it is true or timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for: %s", what)
}

// postJSON issues a raw JSON POST and returns the response.
func postJSON(t *testing.T, url string, body any, token string) *http.Response {
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	return resp
}

func readBodyString(t *testing.T, resp *http.Response) string {
	t.Helper()

	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	return string(b)
}

// assertBalance compares a user's DB balance, scaled to engine units.
func assertBalance(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID string, want int64) {
	t.Helper()

	var got int64
	if err := pool.QueryRow(ctx,
		`SELECT (balance * 10000)::bigint FROM users WHERE id = $1`, userID,
	).Scan(&got); err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Errorf("user %s balance = %d, want %d", userID, got, want)
	}
}

// currentFencingToken reads the live fencing token for a partition.
func currentFencingToken(ctx context.Context, client *redis.Client, partitionID int) (uint64, error) {
	raw, err := client.Get(ctx, partition.FencingKey(partitionID)).Result()
	if err != nil {
		return 0, err
	}

	var token uint64
	if _, err := fmt.Sscanf(raw, "%d", &token); err != nil {
		return 0, err
	}

	return token, nil
}

// injectTrade publishes a trade_executed envelope directly onto the durable
// event stream, exactly as the engine would.
func injectTrade(ctx context.Context, client *redis.Client, stream string, trade *engine.Trade, token uint64) error {
	data, err := json.Marshal(trade)
	if err != nil {
		return err
	}

	envelope := events.NewEnvelope(events.NewEnvelopeParams{
		Type:         events.EventTradeExecuted,
		Data:         data,
		PartitionID:  0,
		FencingToken: token,
		Sequence:     1,
	})

	envBytes, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	return client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"event": string(envBytes)},
	}).Err()
}

// expectWSEvent waits for an envelope of the given type on a pub/sub channel
// and decodes its data into out.
func expectWSEvent(t *testing.T, msgChan <-chan *redis.Message, wantType events.EventType, out any) error {
	t.Helper()

	timeout := time.After(15 * time.Second)

	for {
		select {
		case msg := <-msgChan:
			var env events.EventEnvelope
			if err := json.Unmarshal([]byte(msg.Payload), &env); err != nil {
				continue
			}

			if env.Type != wantType {
				continue
			}

			if out != nil {
				return json.Unmarshal(env.Data, out)
			}

			return nil

		case <-timeout:
			return errors.New("timed out waiting for WS envelope " + string(wantType))
		}
	}
}
