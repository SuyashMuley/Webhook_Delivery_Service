// Package config loads service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	Env             string // dev | prod
	DatabaseURL     string
	HTTPAddr        string
	LogLevel        string // debug | info | warn | error
	LogFormat       string // json | text
	ShutdownTimeout time.Duration
}

// Load reads configuration from the environment. defaultHTTPAddr lets each
// binary pick its own listen address without needing separate variables.
func Load(defaultHTTPAddr string) (Config, error) {
	return load(os.Getenv, defaultHTTPAddr)
}

func load(get func(string) string, defaultHTTPAddr string) (Config, error) {
	c := Config{
		Env:         or(get("APP_ENV"), "dev"),
		DatabaseURL: get("DATABASE_URL"),
		HTTPAddr:    or(get("HTTP_ADDR"), defaultHTTPAddr),
		LogLevel:    or(get("LOG_LEVEL"), "info"),
		LogFormat:   get("LOG_FORMAT"),
	}
	if c.LogFormat == "" {
		c.LogFormat = "text"
		if c.Env == "prod" {
			c.LogFormat = "json"
		}
	}

	var errs []error
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if c.Env != "dev" && c.Env != "prod" {
		errs = append(errs, fmt.Errorf("APP_ENV must be dev or prod, got %q", c.Env))
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		errs = append(errs, fmt.Errorf("LOG_FORMAT must be json or text, got %q", c.LogFormat))
	}

	d, err := time.ParseDuration(or(get("SHUTDOWN_TIMEOUT"), "30s"))
	if err != nil {
		errs = append(errs, fmt.Errorf("SHUTDOWN_TIMEOUT: %w", err))
	}
	c.ShutdownTimeout = d

	return c, errors.Join(errs...)
}

func or(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}
