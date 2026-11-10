package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr        string
	GRPCAddr        string
	DatabaseURL     string
	RedisURL        string
	KafkaBrokers    []string
	Topic           string
	DLQTopic        string
	ConsumerGroup   string
	Workers         int
	MaxAttempts     int
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	DeliveryTimeout time.Duration
	IdempotencyTTL  time.Duration
	ShutdownTimeout time.Duration
	RunMigrations   bool
}

func Load() Config {
	return Config{
		HTTPAddr:        env("HTTP_ADDR", ":8080"),
		GRPCAddr:        env("GRPC_ADDR", ":9090"),
		DatabaseURL:     env("DATABASE_URL", "postgres://webhook:webhook@localhost:5432/webhook?sslmode=disable"),
		RedisURL:        env("REDIS_URL", "redis://localhost:6379/0"),
		KafkaBrokers:    strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ","),
		Topic:           env("KAFKA_TOPIC", "webhook.events"),
		DLQTopic:        env("KAFKA_DLQ_TOPIC", "webhook.events.dlq"),
		ConsumerGroup:   env("KAFKA_GROUP", "webhook-workers"),
		Workers:         envInt("WORKERS", 64),
		MaxAttempts:     envInt("MAX_ATTEMPTS", 5),
		BackoffBase:     envDur("BACKOFF_BASE", 500*time.Millisecond),
		BackoffMax:      envDur("BACKOFF_MAX", 30*time.Second),
		DeliveryTimeout: envDur("DELIVERY_TIMEOUT", 10*time.Second),
		IdempotencyTTL:  envDur("IDEMPOTENCY_TTL", 24*time.Hour),
		ShutdownTimeout: envDur("SHUTDOWN_TIMEOUT", 30*time.Second),
		RunMigrations:   env("RUN_MIGRATIONS", "true") == "true",
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return n
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return d
	}
	return def
}
