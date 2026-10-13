package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	DatabaseURL         string
	RedisURL            string
	JWTSecret           string
	Port                string
	APIPort             string
	WSPort              string
	OpsPort             string
	OpsHost             string
	WSAllowedOrigins    []string
	WSAllowQueryToken   bool
	DBStream            string
	DBGroup             string
	WSStream            string
	EngineID            string
	PartitionCount      int
	LeaseTTL            time.Duration
	LeaseRenewInterval  time.Duration
	CommandStreamPrefix string
}

// Load validates configuration before services open connections or listeners.
// Secrets have no development fallbacks; only services that use them require them.
func Load(service string) (Config, error) {
	if err := LoadEnvFile(".env"); err != nil && !os.IsNotExist(err) {
		return Config{}, fmt.Errorf("load local environment: %w", err)
	}
	cfg := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"), RedisURL: os.Getenv("REDIS_URL"), JWTSecret: os.Getenv("JWT_SECRET"),
		Port: getEnv("PORT", "3000"), APIPort: getEnv("API_PORT", getEnv("PORT", "3000")),
		WSPort: getEnv("WS_PORT", "8080"), OpsPort: getEnv("OPS_PORT", "9090"), OpsHost: getEnv("OPS_HOST", "127.0.0.1"),
		DBStream: getEnv("DB_STREAM", "events:out"), DBGroup: getEnv("DB_GROUP", "db-workers"), WSStream: getEnv("WS_STREAM", "ws:updates"),
		EngineID: getEnv("ENGINE_ID", uuid.NewString()), CommandStreamPrefix: getEnv("COMMAND_STREAM_PREFIX", "commands"),
	}
	if service != "api" && service != "ws" && service != "engine" && service != "db-worker" {
		return cfg, fmt.Errorf("unknown service %q", service)
	}
	if strings.TrimSpace(cfg.RedisURL) == "" {
		return cfg, fmt.Errorf("REDIS_URL is required")
	}
	host, port, addressErr := net.SplitHostPort(cfg.RedisURL)
	redisPort, portErr := strconv.Atoi(port)
	if addressErr != nil || strings.TrimSpace(host) == "" || portErr != nil || redisPort < 1 || redisPort > 65535 {
		return cfg, fmt.Errorf("REDIS_URL must be a host:port address")
	}
	for name, value := range map[string]string{"ENGINE_ID": cfg.EngineID, "COMMAND_STREAM_PREFIX": cfg.CommandStreamPrefix, "DB_STREAM": cfg.DBStream, "DB_GROUP": cfg.DBGroup, "WS_STREAM": cfg.WSStream} {
		if strings.TrimSpace(value) == "" {
			return cfg, fmt.Errorf("%s must not be empty", name)
		}
	}
	if service == "api" || service == "db-worker" {
		if strings.TrimSpace(cfg.DatabaseURL) == "" {
			return cfg, fmt.Errorf("DATABASE_URL is required")
		}
		if _, err := pgxpool.ParseConfig(cfg.DatabaseURL); err != nil {
			return cfg, fmt.Errorf("invalid DATABASE_URL")
		}
	}
	if service == "api" || service == "ws" {
		if len(strings.TrimSpace(cfg.JWTSecret)) < 32 {
			return cfg, fmt.Errorf("JWT_SECRET must contain at least 32 bytes")
		}
	}
	for name, value := range map[string]string{"PORT": cfg.Port, "API_PORT": cfg.APIPort, "WS_PORT": cfg.WSPort, "OPS_PORT": cfg.OpsPort} {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 65535 {
			return cfg, fmt.Errorf("%s must be a port between 1 and 65535", name)
		}
	}
	if net.ParseIP(cfg.OpsHost) == nil && cfg.OpsHost != "localhost" {
		return cfg, fmt.Errorf("OPS_HOST must be an IP address or localhost")
	}
	var err error
	cfg.PartitionCount, err = strconv.Atoi(getEnv("PARTITION_COUNT", "1"))
	if err != nil || cfg.PartitionCount < 1 {
		return cfg, fmt.Errorf("PARTITION_COUNT must be a positive integer")
	}
	cfg.LeaseTTL, err = time.ParseDuration(getEnv("LEASE_TTL", "15s"))
	if err != nil || cfg.LeaseTTL <= 0 {
		return cfg, fmt.Errorf("LEASE_TTL must be a positive duration")
	}
	cfg.LeaseRenewInterval, err = time.ParseDuration(getEnv("LEASE_RENEW_INTERVAL", "5s"))
	if err != nil || cfg.LeaseRenewInterval <= 0 || cfg.LeaseRenewInterval >= cfg.LeaseTTL {
		return cfg, fmt.Errorf("LEASE_RENEW_INTERVAL must be positive and less than LEASE_TTL")
	}
	cfg.WSAllowQueryToken, err = strconv.ParseBool(getEnv("WS_ALLOW_QUERY_TOKEN", "false"))
	if err != nil {
		return cfg, fmt.Errorf("WS_ALLOW_QUERY_TOKEN must be a boolean")
	}
	if origins := strings.TrimSpace(os.Getenv("WS_ALLOWED_ORIGINS")); origins != "" {
		for _, origin := range strings.Split(origins, ",") {
			origin = strings.TrimSpace(origin)
			u, err := url.Parse(origin)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || strings.Contains(u.Host, "*") || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
				return cfg, fmt.Errorf("WS_ALLOWED_ORIGINS must contain exact http(s) origins without paths or wildcards")
			}
			cfg.WSAllowedOrigins = append(cfg.WSAllowedOrigins, origin)
		}
	}
	return cfg, nil
}

func (c Config) OpsAddress() string { return net.JoinHostPort(c.OpsHost, c.OpsPort) }

func getEnv(key, fallback string) string {
	if val, exists := os.LookupEnv(key); exists {
		return val
	}
	return fallback
}
