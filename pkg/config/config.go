package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL string
	RedisURL    string
	JWTSecret   string
	Port        string

	// P3 — Production readiness
	APIPort string
	WSPort  string
	OpsPort string
	// DBStream is the Redis Stream the DB worker consumes (default events:out).
	DBStream string
	// DBGroup is the DB worker consumer group name (default db-workers).
	DBGroup string
	// WSStream is the Redis pub/sub channel for real-time fan-out
	// (default ws:updates).
	WSStream string

	// P2 — Distributed engine
	EngineID            string
	PartitionCount      int
	LeaseTTL            time.Duration
	LeaseRenewInterval  time.Duration
	CommandStreamPrefix string
}

func Load() Config {
	return Config{
		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:mysecretpassword@localhost:5432/predix?sslmode=disable"),
		RedisURL:    getEnv("REDIS_URL", "localhost:6379"),
		JWTSecret:   getEnv("JWT_SECRET", ""), // No fallback for production
		Port:        getEnv("PORT", "3000"),

		APIPort: getEnv("API_PORT", getEnv("PORT", "3000")),
		WSPort:  getEnv("WS_PORT", "8080"),
		OpsPort: getEnv("OPS_PORT", "9090"),

		DBStream: getEnv("DB_STREAM", "events:out"),
		DBGroup:  getEnv("DB_GROUP", "db-workers"),
		WSStream: getEnv("WS_STREAM", "ws:updates"),

		EngineID:            getEnv("ENGINE_ID", "engine-0"),
		PartitionCount:      getEnvInt("PARTITION_COUNT", 1),
		LeaseTTL:            getEnvDuration("LEASE_TTL", 15*time.Second),
		LeaseRenewInterval:  getEnvDuration("LEASE_RENEW_INTERVAL", 5*time.Second),
		CommandStreamPrefix: getEnv("COMMAND_STREAM_PREFIX", "commands"),
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		if n, err := strconv.Atoi(val); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return fallback
}
