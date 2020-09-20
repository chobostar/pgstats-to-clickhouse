package internal

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	Interval          time.Duration
	PostgresDsn       string
	ClickhouseDsn     string
	StatioPostgresDsn string
}

func NewConfig() (*Config, error) {
	d, _ := time.ParseDuration("30s")
	cfg := &Config{
		Interval:          d,
		PostgresDsn:       "postgres://postgres@localhost:5432/postgres?sslmode=disable",
		ClickhouseDsn:     "http://localhost:8123/default",
		StatioPostgresDsn: "postgres://postgres@localhost:5432/postgres?sslmode=disable",
	}
	if v := os.Getenv("INTERVAL"); v != "" {
		i, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("read params errors: %w", err)
		}
		cfg.Interval = i
	}
	if v := os.Getenv("POSTGRES_DSN"); v != "" {
		cfg.PostgresDsn = v
	}
	if v := os.Getenv("CLICKHOUSE_DSN"); v != "" {
		cfg.ClickhouseDsn = v
	}
	if v := os.Getenv("STATIO_POSTGRES_DSN"); v != "" {
		cfg.StatioPostgresDsn = v
	}
	return cfg, nil
}
