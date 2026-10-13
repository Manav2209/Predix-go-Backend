package config

import (
	"strings"
	"testing"
)

func validEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"PORT", "API_PORT", "WS_PORT", "OPS_PORT", "OPS_HOST", "PARTITION_COUNT", "LEASE_TTL", "LEASE_RENEW_INTERVAL", "WS_ALLOWED_ORIGINS", "WS_ALLOW_QUERY_TOKEN", "DB_STREAM", "DB_GROUP", "WS_STREAM", "ENGINE_ID", "COMMAND_STREAM_PREFIX"} {
		// Explicit defaults avoid relying on the developer's shell environment.
		defaults := map[string]string{"PORT": "3000", "API_PORT": "3000", "WS_PORT": "8080", "OPS_PORT": "9090", "OPS_HOST": "127.0.0.1", "PARTITION_COUNT": "1", "LEASE_TTL": "15s", "LEASE_RENEW_INTERVAL": "5s", "WS_ALLOW_QUERY_TOKEN": "false", "DB_STREAM": "events:out", "DB_GROUP": "db-workers", "WS_STREAM": "ws:updates", "ENGINE_ID": "test", "COMMAND_STREAM_PREFIX": "commands"}
		t.Setenv(key, defaults[key])
	}
	t.Setenv("DATABASE_URL", "postgres://test:test@localhost:5432/predix?sslmode=disable")
	t.Setenv("REDIS_URL", "localhost:6379")
	t.Setenv("JWT_SECRET", strings.Repeat("s", 32))
}

func TestLoadRejectsUnsafeAndInvalidConfiguration(t *testing.T) {
	cases := []struct{ key, value string }{
		{"DATABASE_URL", ""}, {"DATABASE_URL", "://bad"}, {"REDIS_URL", ""}, {"REDIS_URL", "bad"},
		{"REDIS_URL", ":6379"}, {"REDIS_URL", "localhost:abc"}, {"DB_GROUP", ""},
		{"JWT_SECRET", ""}, {"JWT_SECRET", "short"}, {"JWT_SECRET", strings.Repeat(" ", 32)},
		{"API_PORT", "0"}, {"OPS_PORT", "70000"}, {"OPS_HOST", "invalid:host"},
		{"PARTITION_COUNT", "garbage"}, {"PARTITION_COUNT", "0"}, {"LEASE_TTL", "invalid"},
		{"LEASE_RENEW_INTERVAL", "15s"}, {"LEASE_RENEW_INTERVAL", "0s"},
		{"WS_ALLOW_QUERY_TOKEN", "maybe"}, {"WS_ALLOWED_ORIGINS", "*"},
		{"WS_ALLOWED_ORIGINS", "https://example.com/path"}, {"WS_ALLOWED_ORIGINS", "https://*.example.com"},
	}
	for _, tc := range cases {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			validEnv(t)
			t.Setenv(tc.key, tc.value)
			if _, err := Load("api"); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestServiceSecretsAndSafeDefaults(t *testing.T) {
	validEnv(t)
	t.Setenv("DATABASE_URL", "")
	t.Setenv("JWT_SECRET", "")
	for _, service := range []string{"engine", "db-worker", "api", "ws"} {
		_, err := Load(service)
		if (service == "engine") != (err == nil) {
			t.Fatalf("service %s: %v", service, err)
		}
	}
	validEnv(t)
	t.Setenv("WS_ALLOWED_ORIGINS", "https://example.com, http://localhost:3000")
	cfg, err := Load("ws")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OpsAddress() != "127.0.0.1:9090" || cfg.WSAllowQueryToken || len(cfg.WSAllowedOrigins) != 2 {
		t.Fatalf("unsafe defaults: %+v", cfg)
	}
}
