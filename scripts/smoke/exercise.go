package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"predix/internal/engine"
	"predix/internal/events"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type check struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail,omitempty"`
}

type service struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
	log  *os.File
}

type smoke struct {
	ctx                                                         context.Context
	root, work, schema, namespace, dsn, secret, api, ws, origin string
	apiPort, wsPort                                             int
	ops                                                         map[string]int
	processes                                                   map[string]*service
	db                                                          *pgxpool.Pool
	redis                                                       *redis.Client
	http                                                        *http.Client
	checks                                                      []check
}

func exercise(admin *pgxpool.Pool, client *redis.Client) (runErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(root, "tmp", "smoke"), 0700); err != nil {
		return err
	}
	work, err := os.MkdirTemp(filepath.Join(root, "tmp", "smoke"), "run-")
	if err != nil {
		return err
	}
	id := strings.ReplaceAll(uuid.NewString(), "-", "")
	r := &smoke{ctx: ctx, root: root, work: work, schema: "smoke_" + id, namespace: "smoke:" + id, redis: client, http: &http.Client{Timeout: 10 * time.Second}, ops: map[string]int{}, processes: map[string]*service{}}
	defer func() {
		for name := range r.processes {
			r.stop(name)
		}
		if r.db != nil {
			r.db.Close()
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{r.schema}.Sanitize()+" CASCADE"); err != nil {
			r.record("cleanup temporary schema", false, "schema retained: "+r.schema)
		}
		var cursor uint64
		for {
			keys, next, err := client.Scan(cleanupCtx, cursor, r.namespace+"*", 100).Result()
			if err != nil {
				r.record("cleanup test Redis keys", false, err.Error())
				break
			}
			if len(keys) > 0 {
				if err := client.Del(cleanupCtx, keys...).Err(); err != nil {
					r.record("cleanup test Redis keys", false, err.Error())
					break
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		// Only remove our own lease. Never reset the shared fencing counter.
		_ = client.Eval(cleanupCtx, `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0`, []string{"engine:lease:0"}, r.namespace).Err()
		if runErr != nil {
			r.record("complete smoke run", false, runErr.Error())
		}
		failed := 0
		for _, c := range r.checks {
			if !c.Passed {
				failed++
			}
		}
		report, _ := json.MarshalIndent(map[string]any{"checked_at": time.Now().UTC(), "passed": len(r.checks) - failed, "failed": failed, "checks": r.checks, "notes": []string{"Actual api/engine/db-worker/ws binaries were tested.", "Trade settlement uses isolated fixtures; public API lacks share issuance for a successful SELL match.", "All test service processes and test data are cleaned up after the run."}}, "", "  ")
		path := filepath.Join(work, "report.json")
		if err := os.WriteFile(path, report, 0600); err != nil {
			runErr = err
		}
		fmt.Printf("RESULT passed=%d failed=%d report=%s\n", len(r.checks)-failed, failed, path)
		if failed > 0 && runErr == nil {
			runErr = fmt.Errorf("%d smoke checks failed; see report", failed)
		}
	}()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{r.schema}.Sanitize()); err != nil {
		return fmt.Errorf("create isolated schema: %w", err)
	}
	r.dsn = os.Getenv("DATABASE_URL")
	if u, err := url.Parse(r.dsn); err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		q := u.Query()
		q.Set("search_path", r.schema)
		u.RawQuery = q.Encode()
		r.dsn = u.String()
	} else {
		r.dsn += " search_path=" + r.schema
	}
	r.db, err = pgxpool.New(ctx, r.dsn)
	if err != nil {
		return fmt.Errorf("configure isolated schema")
	}
	var actualSchema string
	if err := r.db.QueryRow(ctx, "SELECT current_schema()").Scan(&actualSchema); err != nil {
		return err
	}
	if actualSchema != r.schema {
		return fmt.Errorf("database connection did not preserve isolated search_path; refusing to touch existing tables")
	}
	paths, err := filepath.Glob(filepath.Join(root, "db", "migrations", "*.up.sql"))
	if err != nil {
		return err
	}
	sort.Strings(paths)
	for _, path := range paths {
		sql, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if _, err := r.db.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", filepath.Base(path), err)
		}
	}
	r.record("all four migrations on real PostgreSQL", true, "isolated schema")
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return err
	}
	r.secret = hex.EncodeToString(secret)
	r.apiPort, err = freePort(3000)
	if err != nil {
		return err
	}
	r.wsPort, err = freePort(8080)
	if err != nil {
		return err
	}
	r.api = fmt.Sprintf("http://127.0.0.1:%d", r.apiPort)
	r.ws = fmt.Sprintf("ws://127.0.0.1:%d/ws", r.wsPort)
	r.origin = r.api
	for i, name := range []string{"api", "engine", "db-worker", "ws"} {
		r.ops[name], err = freePort(9090 + i)
		if err != nil {
			return err
		}
		if err := r.start(name); err != nil {
			return err
		}
	}
	for _, name := range []string{"api", "engine", "db-worker", "ws"} {
		if !r.await("service ready: "+name, 20*time.Second, func() bool { return r.status(r.opsURL(name)+"/health/ready") == 200 }) {
			return fmt.Errorf("%s did not become ready; inspect %s.log", name, name)
		}
		for _, path := range []string{"/health/live", "/metrics"} {
			r.record(name+" "+path, r.status(r.opsURL(name)+path) == 200, "expected HTTP 200")
		}
	}
	return r.apis()
}

func freePort(preferred int) (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(preferred))
	if err != nil {
		l, err = net.Listen("tcp", "127.0.0.1:0")
	}
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func (r *smoke) start(name string) error {
	path := filepath.Join(r.root, "tmp", "smoke", "bin", name)
	if _, err := os.Stat(path + ".exe"); err == nil {
		path += ".exe"
	}
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("build service binaries first: go build -o tmp/smoke/bin/ ./cmd/api ./cmd/engine ./cmd/db-worker ./cmd/ws")
	}
	dir := filepath.Join(r.work, name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	log, err := os.OpenFile(filepath.Join(r.work, name+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	cmd := exec.Command(path)
	cmd.Dir = dir
	cmd.Stdout = log
	cmd.Stderr = log
	hideWindow(cmd)
	env := map[string]string{}
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		env[key] = value
	}
	for key, value := range map[string]string{
		"DATABASE_URL": r.dsn, "JWT_SECRET": r.secret, "API_PORT": strconv.Itoa(r.apiPort), "WS_PORT": strconv.Itoa(r.wsPort),
		"OPS_HOST": "127.0.0.1", "OPS_PORT": strconv.Itoa(r.ops[name]), "ENGINE_ID": r.namespace,
		"PARTITION_COUNT": "1", "LEASE_TTL": "2s", "LEASE_RENEW_INTERVAL": "500ms", "COMMAND_STREAM_PREFIX": r.namespace + ":commands",
		"DB_STREAM": r.namespace + ":events", "DB_GROUP": r.namespace + ":db-workers", "WS_STREAM": r.namespace + ":ws",
		"WS_ALLOWED_ORIGINS": r.origin, "WS_ALLOW_QUERY_TOKEN": "false", "GIN_MODE": "release",
	} {
		env[key] = value
	}
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	if err := cmd.Start(); err != nil {
		log.Close()
		return err
	}
	s := &service{cmd: cmd, done: make(chan struct{}), log: log}
	r.processes[name] = s
	go func() { s.err = cmd.Wait(); close(s.done) }()
	fmt.Printf("START %s PID=%d\n", name, cmd.Process.Pid)
	return nil
}

func (r *smoke) stop(name string) {
	s := r.processes[name]
	if s == nil {
		return
	}
	select {
	case <-s.done:
	default:
		_ = s.cmd.Process.Kill()
		<-s.done
	}
	_ = s.log.Close()
	delete(r.processes, name)
}

func (r *smoke) record(name string, passed bool, detail string) {
	r.checks = append(r.checks, check{Name: name, Passed: passed, Detail: detail})
	status := "PASS"
	if !passed {
		status = "FAIL"
	}
	fmt.Printf("%s %s %s\n", status, name, detail)
}

func (r *smoke) opsURL(name string) string { return fmt.Sprintf("http://127.0.0.1:%d", r.ops[name]) }
func (r *smoke) status(address string) int {
	resp, err := r.http.Get(address)
	if err != nil {
		return 0
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

func (r *smoke) await(name string, timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && r.ctx.Err() == nil {
		if fn() {
			r.record(name, true, "")
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	r.record(name, false, "timed out")
	return false
}

func (r *smoke) request(name, method, path, token string, body any, want int) map[string]any {
	var data []byte
	if body != nil {
		data, _ = json.Marshal(body)
	}
	req, err := http.NewRequestWithContext(r.ctx, method, r.api+path, bytes.NewReader(data))
	if err != nil {
		r.record(name, false, err.Error())
		return nil
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := r.http.Do(req)
	if err != nil {
		r.record(name, false, err.Error())
		return nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	r.record(name, resp.StatusCode == want, fmt.Sprintf("%s %s got=%d want=%d", method, path, resp.StatusCode, want))
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}

func object(v any) map[string]any { m, _ := v.(map[string]any); return m }
func text(v any) string           { s, _ := v.(string); return s }
func list(v any) []any            { l, _ := v.([]any); return l }

func (r *smoke) signup(label string) (string, string, error) {
	email := label + "-" + uuid.NewString() + "@example.com"
	body := map[string]any{"email": email, "password": "smoke-password-123"}
	created := r.request(label+" signup", "POST", "/auth/signup", "", body, 201)
	id := text(created["id"])
	r.request("duplicate signup", "POST", "/auth/signup", "", body, 409)
	r.request("wrong password", "POST", "/auth/signin", "", map[string]any{"email": email, "password": "wrong"}, 401)
	signed := r.request(label+" signin", "POST", "/auth/signin", "", body, 200)
	token := text(signed["token"])
	if _, err := uuid.Parse(id); err != nil || token == "" {
		return "", "", fmt.Errorf("signup/signin response missing valid user ID or JWT")
	}
	return id, token, nil
}

func (r *smoke) createEvent(token, title string, expiry time.Time, want int) string {
	out := r.request(title, "POST", "/event", token, map[string]any{"title": title, "description": "Isolated smoke test", "question": "Will this test pass?", "imageurl": "https://example.com/test.png", "expiresAt": expiry.UTC().Format(time.RFC3339)}, want)
	data := object(out["data"])
	id := text(data["id"])
	if title == "main event" {
		r.record("event response uses API DTO field id", id != "", "")
	}
	if id == "" {
		id = text(data["ID"])
	}
	return id
}

func order(event, id, side, outcome string, quantity int, price float64) map[string]any {
	return map[string]any{"eventId": event, "orderId": id, "side": side, "outcome": outcome, "orderType": "LIMIT", "quantity": quantity, "price": price}
}

func (r *smoke) rowExists(id string) bool {
	var n int
	err := r.db.QueryRow(r.ctx, "SELECT count(*) FROM orders WHERE id=$1", id).Scan(&n)
	return err == nil && n == 1
}

func (r *smoke) apis() error {
	r.request("root", "GET", "/", "", nil, 200)
	r.request("ping", "GET", "/ping", "", nil, 200)
	for _, path := range []string{"/health/live", "/health/ready", "/metrics"} {
		r.request("ops absent from public API", "GET", path, "", nil, 404)
	}
	for _, route := range []struct{ method, path string }{{"GET", "/me"}, {"GET", "/balances"}, {"GET", "/position"}, {"GET", "/events"}, {"GET", "/event/" + uuid.NewString()}, {"POST", "/event"}, {"POST", "/order"}, {"DELETE", "/order/" + uuid.NewString()}, {"GET", "/orderbook/" + uuid.NewString()}, {"GET", "/orderbook/" + uuid.NewString() + "/depth"}} {
		r.request("authentication required", route.method, route.path, "", nil, 401)
	}
	r.request("invalid bearer token", "GET", "/me", "invalid", nil, 401)
	r.request("invalid signup", "POST", "/auth/signup", "", map[string]any{"email": "invalid", "password": "123"}, 400)
	r.request("invalid signin payload", "POST", "/auth/signin", "", map[string]any{}, 400)
	alice, aliceToken, err := r.signup("alice")
	if err != nil {
		return err
	}
	bob, bobToken, err := r.signup("bob")
	if err != nil {
		return err
	}
	me := r.request("current user", "GET", "/me", aliceToken, nil, 200)
	r.record("me identifies authenticated user", text(me["user_id"]) == alice, "")
	for _, path := range []string{"/balances", "/position"} {
		out := r.request("account endpoint status", "GET", path, aliceToken, nil, 200)
		if path == "/balances" {
			r.record("starting cash is returned by balances API", object(out["data"])["balance"] == float64(100) && text(object(out["data"])["userId"]) == alice, "")
		} else {
			_, isArray := out["data"].([]any)
			r.record("empty positions returned as array", isArray && len(list(out["data"])) == 0, "")
		}
	}
	r.request("empty event list", "GET", "/events", aliceToken, nil, 200)
	r.request("missing event", "GET", "/event/"+uuid.NewString(), aliceToken, nil, 404)
	r.request("malformed event ID", "GET", "/event/not-a-uuid", aliceToken, nil, 400)
	r.request("invalid event payload", "POST", "/event", aliceToken, map[string]any{}, 400)
	event := r.createEvent(aliceToken, "main event", time.Now().Add(24*time.Hour), 201)
	if event == "" {
		return fmt.Errorf("could not create primary test event")
	}
	r.request("event details", "GET", "/event/"+event, aliceToken, nil, 200)
	out := r.request("active event list", "GET", "/events", aliceToken, nil, 200)
	r.record("created event listed", len(list(out["data"])) == 1, "")
	for _, path := range []string{"/orderbook/" + event, "/orderbook/" + event + "/depth"} {
		r.request("empty order book", "GET", path, aliceToken, nil, 200)
	}
	for _, path := range []string{"/orderbook/not-a-uuid", "/orderbook/not-a-uuid/depth"} {
		r.request("malformed orderbook ID", "GET", path, aliceToken, nil, 400)
	}
	for _, path := range []string{"/orderbook/" + uuid.NewString(), "/orderbook/" + uuid.NewString() + "/depth"} {
		r.request("missing orderbook event", "GET", path, aliceToken, nil, 404)
	}
	conn, err := r.websocketChecks(aliceToken, event)
	if err != nil {
		return err
	}
	defer conn.Close()
	validationEvent := r.createEvent(aliceToken, "validation event", time.Now().Add(time.Hour), 201)
	for _, tc := range []struct {
		label   string
		payload map[string]any
	}{
		{"zero quantity", order(event, uuid.NewString(), "BUY", "YES", 0, .5)},
		{"negative quantity", order(event, uuid.NewString(), "BUY", "YES", -1, .5)},
		{"invalid side", order(event, uuid.NewString(), "INVALID", "YES", 1, .5)},
		{"invalid outcome", order(event, uuid.NewString(), "BUY", "INVALID", 1, .5)},
		{"out of range price", order(validationEvent, uuid.NewString(), "BUY", "YES", 1, 1.1)},
		{"missing event", order(uuid.NewString(), uuid.NewString(), "BUY", "YES", 1, .5)},
		{"malformed event", order("not-a-uuid", uuid.NewString(), "BUY", "YES", 1, .5)},
		{"insufficient cash", order(event, uuid.NewString(), "BUY", "YES", 1000, .5)},
		{"sell without shares", order(event, uuid.NewString(), "SELL", "YES", 1, .5)},
	} {
		r.request(tc.label, "POST", "/order", aliceToken, tc.payload, 400)
	}
	r.request("malformed order ID rejected", "POST", "/order", aliceToken, order(validationEvent, "not-a-uuid", "BUY", "YES", 1, .5), 400)
	expired := r.createEvent(aliceToken, "past expiry rejected", time.Now().Add(-time.Hour), 400)
	if expired != "" {
		r.request("expired event rejects orders", "POST", "/order", aliceToken, order(expired, uuid.NewString(), "BUY", "YES", 1, .5), 400)
	}
	if _, err := r.db.Exec(r.ctx, `UPDATE events SET expires_at=NOW()-INTERVAL '1 second' WHERE id=$1`, validationEvent); err != nil {
		return err
	}
	r.request("existing expired event rejects orders", "POST", "/order", aliceToken, order(validationEvent, uuid.NewString(), "BUY", "YES", 1, .5), 400)
	expiredDetails := r.request("expired event details", "GET", "/event/"+validationEvent, aliceToken, nil, 200)
	r.record("expired event reported inactive", object(expiredDetails["data"])["isActive"] == false, "")
	activeEvents := r.request("active events exclude expired", "GET", "/events", aliceToken, nil, 200)
	activeCount := 0
	for _, item := range list(activeEvents["data"]) {
		if text(object(item)["id"]) == validationEvent {
			activeCount++
		}
	}
	r.record("expired event absent from active list", activeCount == 0, "")
	id := uuid.NewString()
	accepted := r.request("limit BUY accepted", "POST", "/order", aliceToken, order(event, id, "BUY", "YES", 10, .5), 200)
	r.record("accepted order ID and status", text(object(accepted["data"])["orderId"]) == id && text(object(accepted["data"])["status"]) == "PENDING", "")
	if !r.await("order projected into PostgreSQL", 10*time.Second, func() bool { return r.rowExists(id) }) {
		return fmt.Errorf("DB projection failed after accepting order")
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var pushed map[string]any
	err = conn.ReadJSON(&pushed)
	r.record("WebSocket receives live depth", err == nil && text(pushed["type"]) == "depth", fmt.Sprintf("type=%s", text(pushed["type"])))
	depth := r.request("orderbook after BUY", "GET", "/orderbook/"+event, aliceToken, nil, 200)
	bids := list(object(object(depth["data"])["yes"])["bids"])
	r.record("orderbook contains quantity 10 at price 5000", len(bids) == 1 && object(bids[0])["quantity"] == float64(10) && object(bids[0])["price"] == float64(5000), "")
	r.request("duplicate order is idempotent", "POST", "/order", aliceToken, order(event, id, "BUY", "YES", 10, .5), 200)
	r.request("other user cannot cancel", "DELETE", "/order/"+id, bobToken, nil, 403)
	r.request("malformed cancellation ID", "DELETE", "/order/not-a-uuid", aliceToken, nil, 400)
	r.request("missing cancellation order", "DELETE", "/order/"+uuid.NewString(), aliceToken, nil, 404)
	r.request("owner cancels", "DELETE", "/order/"+id, aliceToken, nil, 200)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var canceledDepth map[string]any
	err = conn.ReadJSON(&canceledDepth)
	r.record("live WS depth removes canceled order", err == nil && text(canceledDepth["type"]) == "depth" && len(list(object(object(canceledDepth["data"])["YES"])["bids"])) == 0, "")
	r.await("cancellation projected into PostgreSQL", 10*time.Second, func() bool {
		var status string
		err := r.db.QueryRow(r.ctx, "SELECT status FROM orders WHERE id=$1", id).Scan(&status)
		return err == nil && status == "CANCELED"
	})
	depth = r.request("depth after cancellation", "GET", "/orderbook/"+event+"/depth", aliceToken, nil, 200)
	r.record("canceled order removed from book", len(list(object(object(depth["data"])["yes"])["bids"])) == 0, "")
	r.request("repeat cancel rejected", "DELETE", "/order/"+id, aliceToken, nil, 400)
	marketID := uuid.NewString()
	marketOrder := order(event, marketID, "BUY", "YES", 1, 0)
	marketOrder["orderType"] = "MARKET"
	market := r.request("market order without liquidity", "POST", "/order", aliceToken, marketOrder, 200)
	r.record("unfilled market order canceled by engine", text(object(market["data"])["status"]) == "CANCELED", "")
	if r.await("market order projected", 10*time.Second, func() bool { return r.rowExists(marketID) }) {
		var status string
		err := r.db.QueryRow(r.ctx, "SELECT status FROM orders WHERE id=$1", marketID).Scan(&status)
		r.record("market cancellation projected consistently", err == nil && status == "CANCELED", "database status="+status+" engine status=CANCELED")
	}
	liveID := uuid.NewString()
	r.request("NO outcome limit order", "POST", "/order", aliceToken, order(event, liveID, "BUY", "NO", 2, .6), 200)
	r.await("NO order projected", 10*time.Second, func() bool { return r.rowExists(liveID) })
	r.stop("engine")
	if err := r.start("engine"); err != nil {
		return err
	}
	r.await("engine recovers after restart", 15*time.Second, func() bool { return r.status(r.opsURL("engine")+"/health/ready") == 200 })
	depth = r.request("depth after restart", "GET", "/orderbook/"+event+"/depth", aliceToken, nil, 200)
	r.record("restart reconstructs NO order", len(list(object(object(depth["data"])["no"])["bids"])) == 1, "")
	if err := r.consumerRecovery(); err != nil {
		return err
	}
	if err := r.settlement(alice, bob, event, aliceToken); err != nil {
		return err
	}
	for _, name := range []string{"api", "engine", "db-worker", "ws"} {
		r.record("final readiness "+name, r.status(r.opsURL(name)+"/health/ready") == 200, "")
	}
	return r.fatalWorkerCheck(alice, bob, event)
}

func (r *smoke) websocketChecks(token, event string) (*websocket.Conn, error) {
	for _, tc := range []struct {
		label, address, auth, origin string
		want                         int
	}{
		{"WS missing token", r.ws, "", r.origin, 401}, {"WS invalid token", r.ws, "invalid", r.origin, 401},
		{"WS disallowed origin", r.ws, token, "https://evil.example", 403},
		{"WS query tokens disabled", r.ws + "?token=" + token, "", r.origin, 401},
	} {
		header := http.Header{}
		if tc.auth != "" {
			header.Set("Authorization", "Bearer "+tc.auth)
		}
		header.Set("Origin", tc.origin)
		conn, resp, err := websocket.DefaultDialer.Dial(tc.address, header)
		status := 0
		if resp != nil {
			status = resp.StatusCode
			resp.Body.Close()
		}
		if conn != nil {
			conn.Close()
		}
		r.record(tc.label, err != nil && status == tc.want, fmt.Sprintf("got=%d want=%d", status, tc.want))
	}
	header := http.Header{"Authorization": []string{"Bearer " + token}}
	native, _, err := websocket.DefaultDialer.Dial(r.ws, header)
	r.record("WS native Bearer authentication", err == nil, "")
	if native != nil {
		native.Close()
	}
	dialer := websocket.Dialer{Subprotocols: []string{"bearer", token}, HandshakeTimeout: 5 * time.Second}
	conn, _, err := dialer.Dial(r.ws, http.Header{"Origin": []string{r.origin}})
	if err != nil {
		return nil, fmt.Errorf("authenticated browser WebSocket upgrade failed")
	}
	r.record("WS browser JWT subprotocol", conn.Subprotocol() == "bearer", "")
	conn.WriteJSON(map[string]any{"method": "SUBSCRIBE", "params": []string{event}})
	// The following error response provides a barrier after subscription.
	conn.WriteJSON(map[string]any{"method": "UNKNOWN"})
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var message map[string]any
	err = conn.ReadJSON(&message)
	r.record("WS message validation", err == nil && text(message["type"]) == "error", "")
	return conn, nil
}

func (r *smoke) consumerRecovery() error {
	stream, group := r.namespace+":events", r.namespace+":db-workers"
	before := r.redis.XLen(r.ctx, stream+":dead-letter").Val()
	if err := r.redis.XAdd(r.ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"event": "invalid-json"}}).Err(); err != nil {
		return err
	}
	r.await("malformed event quarantined on real Redis", 10*time.Second, func() bool { return r.redis.XLen(r.ctx, stream+":dead-letter").Val() > before })
	env := events.NewEnvelope(events.NewEnvelopeParams{Type: events.EventOrderStatus, Data: []byte(`{}`)})
	raw, _ := json.Marshal(env)
	if err := r.redis.Eval(r.ctx, `local id = redis.call('XADD', KEYS[1], '*', 'event', ARGV[1]); redis.call('XREADGROUP', 'GROUP', ARGV[2], 'abandoned-smoke-consumer', 'STREAMS', KEYS[1], '>'); return id`, []string{stream}, string(raw), group).Err(); err != nil {
		return err
	}
	r.await("runtime abandoned message reclaimed without restart", 20*time.Second, func() bool {
		p, err := r.redis.XPending(r.ctx, stream, group).Result()
		return err == nil && p.Count == 0
	})
	return nil
}

func (r *smoke) settlement(buyer, seller, event, token string) error {
	// Test the real Redis -> WebSocket transport separately from the missing
	// engine depth emission; these fixture notifications are explicitly labeled.
	dialer := websocket.Dialer{Subprotocols: []string{"bearer", token}, HandshakeTimeout: 5 * time.Second}
	conn, _, err := dialer.Dial(r.ws, http.Header{"Origin": []string{r.origin}})
	if err != nil {
		return fmt.Errorf("fixture WebSocket connection failed")
	}
	defer conn.Close()
	if err := conn.WriteJSON(map[string]any{"method": "SUBSCRIBE", "params": []string{event}}); err != nil {
		return err
	}
	if err := conn.WriteJSON(map[string]any{"method": "UNKNOWN"}); err != nil {
		return err
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var barrier map[string]any
	if err := conn.ReadJSON(&barrier); err != nil {
		return err
	}
	data, _ := json.Marshal(map[string]any{"eventId": event, "data": map[string]any{"YES": map[string]any{"bids": []map[string]any{{"price": 5000, "quantity": 2, "total": 10000}}, "asks": []any{}}, "NO": map[string]any{"bids": []any{}, "asks": []any{}}}})
	env := events.NewEnvelope(events.NewEnvelopeParams{Type: events.EventDepthUpdated, Data: data})
	raw, _ := json.Marshal(env)
	if err := r.redis.Publish(r.ctx, r.namespace+":ws", string(raw)).Err(); err != nil {
		return err
	}
	var message map[string]any
	err = conn.ReadJSON(&message)
	r.record("real Redis to WS depth fixture", err == nil && text(message["type"]) == "depth", "fixture notification")
	buyOrder, sellOrder := uuid.NewString(), uuid.NewString()
	for _, row := range []struct{ id, user, side string }{{buyOrder, buyer, "BUY"}, {sellOrder, seller, "SELL"}} {
		_, err := r.db.Exec(r.ctx, `INSERT INTO orders (id,event_id,user_id,order_type,outcome,side,quantity,price,status,filled_quantity,remaining_quantity) VALUES ($1,$2,$3,'LIMIT','YES',$4,3,0.5,'PENDING',0,3)`, row.id, event, row.user, row.side)
		if err != nil {
			return fmt.Errorf("prepare settlement fixture: %w", err)
		}
	}
	trade := engine.Trade{ID: uuid.NewString(), EventID: event, OrderID: buyOrder, MatchOrderID: sellOrder, BuyerID: buyer, SellerID: seller, Outcome: "YES", TakerSide: "BUY", Quantity: 2, Price: 5000, CreatedAt: time.Now()}
	if err := r.publishTrade(trade); err != nil {
		return err
	}
	tradeData, _ := json.Marshal(trade)
	tradeEnvelope := events.NewEnvelope(events.NewEnvelopeParams{Type: events.EventTradeExecuted, Data: tradeData})
	tradeRaw, _ := json.Marshal(tradeEnvelope)
	if err := r.redis.Publish(r.ctx, r.namespace+":ws", string(tradeRaw)).Err(); err != nil {
		return err
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	message = nil
	err = conn.ReadJSON(&message)
	r.record("real Redis to WS trade fixture", err == nil && text(message["type"]) == "trade" && object(message["data"])["quantity"] == float64(2), "fixture notification")
	r.await("real DB worker settles trade fixture", 10*time.Second, func() bool {
		var count int
		err := r.db.QueryRow(r.ctx, "SELECT count(*) FROM trades WHERE id=$1", trade.ID).Scan(&count)
		return err == nil && count == 1
	})
	r.assertNumber("buyer cash debited", `SELECT (balance*10000)::bigint FROM users WHERE id=$1`, 990000, buyer)
	r.assertNumber("seller cash credited", `SELECT (balance*10000)::bigint FROM users WHERE id=$1`, 1010000, seller)
	r.assertNumber("two transaction legs", `SELECT count(*) FROM transactions WHERE trade_id=$1`, 2, trade.ID)
	r.assertNumber("event volume updated", `SELECT (volume*10000)::bigint FROM events WHERE id=$1`, 10000, event)
	r.assertNumber("buyer position updated", `SELECT shares::bigint FROM positions WHERE user_id=$1 AND event_id=$2 AND outcome='YES'`, 2, buyer, event)
	r.assertNumber("seller position updated", `SELECT shares::bigint FROM positions WHERE user_id=$1 AND event_id=$2 AND outcome='YES'`, -2, seller, event)
	balanceResponse := r.request("balance API after settlement", "GET", "/balances", token, nil, 200)
	r.record("balance API reflects durable settlement", object(balanceResponse["data"])["balance"] == float64(99), "")
	positionResponse := r.request("position API after settlement", "GET", "/position", token, nil, 200)
	positions := list(positionResponse["data"])
	r.record("position API returns user's settled shares", len(positions) == 1 && object(positions[0])["shares"] == float64(2) && text(object(positions[0])["eventId"]) == event, "")
	for _, id := range []string{buyOrder, sellOrder} {
		r.assertNumber("both order fills updated", `SELECT filled_quantity::bigint FROM orders WHERE id=$1`, 2, id)
		r.assertNumber("both remaining quantities updated", `SELECT remaining_quantity::bigint FROM orders WHERE id=$1`, 1, id)
	}
	if err := r.publishTrade(trade); err != nil {
		return err
	}
	r.await("duplicate trade acknowledged", 10*time.Second, func() bool {
		p, err := r.redis.XPending(r.ctx, r.namespace+":events", r.namespace+":db-workers").Result()
		info, err2 := r.redis.XInfoGroups(r.ctx, r.namespace+":events").Result()
		return err == nil && err2 == nil && p.Count == 0 && len(info) > 0 && info[0].Lag == 0
	})
	r.assertNumber("duplicate trade does not debit twice", `SELECT (balance*10000)::bigint FROM users WHERE id=$1`, 990000, buyer)
	r.assertNumber("duplicate trade does not duplicate transactions", `SELECT count(*) FROM transactions WHERE trade_id=$1`, 2, trade.ID)
	return nil
}

func (r *smoke) publishTrade(trade engine.Trade) error {
	data, _ := json.Marshal(trade)
	token, err := r.redis.Get(r.ctx, "engine:fencing:0").Uint64()
	if err != nil {
		return err
	}
	env := events.NewEnvelope(events.NewEnvelopeParams{Type: events.EventTradeExecuted, Data: data, FencingToken: token})
	raw, _ := json.Marshal(env)
	return r.redis.XAdd(r.ctx, &redis.XAddArgs{Stream: r.namespace + ":events", Values: map[string]any{"event": string(raw)}}).Err()
}

func (r *smoke) assertNumber(name, query string, want int64, args ...any) {
	var got int64
	err := r.db.QueryRow(r.ctx, query, args...).Scan(&got)
	r.record(name, err == nil && got == want, fmt.Sprintf("got=%d want=%d", got, want))
}

func (r *smoke) fatalWorkerCheck(buyer, seller, event string) error {
	trade := engine.Trade{ID: uuid.NewString(), EventID: event, OrderID: uuid.NewString(), MatchOrderID: uuid.NewString(), BuyerID: buyer, SellerID: seller, Outcome: "YES", TakerSide: "BUY", Quantity: 1, Price: 5000}
	if err := r.publishTrade(trade); err != nil {
		return err
	}
	s := r.processes["db-worker"]
	select {
	case <-s.done:
		exit, ok := s.err.(*exec.ExitError)
		r.record("DB worker exits nonzero on processing failure", ok && exit.ExitCode() == 1, "")
	case <-time.After(10 * time.Second):
		r.record("DB worker exits nonzero on processing failure", false, "process stayed alive")
	}
	r.assertNumber("failed settlement rolled back", `SELECT count(*) FROM trades WHERE id=$1`, 0, trade.ID)
	p, err := r.redis.XPending(r.ctx, r.namespace+":events", r.namespace+":db-workers").Result()
	r.record("failed settlement remains pending", err == nil && p.Count == 1, "")
	r.record("failed worker no longer serves readiness", r.status(r.opsURL("db-worker")+"/health/ready") != 200, "")
	return nil
}
