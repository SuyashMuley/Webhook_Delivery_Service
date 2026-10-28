package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	c, err := load(env(map[string]string{"DATABASE_URL": "postgres://x"}), ":8080")
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.Env != "dev" || c.LogFormat != "text" || c.ShutdownTimeout != 30*time.Second {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestLoadProdDefaultsToJSONLogs(t *testing.T) {
	c, err := load(env(map[string]string{"DATABASE_URL": "postgres://x", "APP_ENV": "prod"}), ":8080")
	if err != nil {
		t.Fatal(err)
	}
	if c.LogFormat != "json" {
		t.Fatalf("want json, got %q", c.LogFormat)
	}
}

func TestLoadRejectsLeaseShorterThanTimeout(t *testing.T) {
	_, err := load(env(map[string]string{
		"DATABASE_URL": "postgres://x", "DELIVERY_TIMEOUT": "30s", "LEASE_DURATION": "10s",
	}), ":8080")
	if err == nil || !strings.Contains(err.Error(), "LEASE_DURATION") {
		t.Fatalf("want lease error, got %v", err)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := load(env(map[string]string{"APP_ENV": "staging", "SHUTDOWN_TIMEOUT": "soon"}), ":8080")
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"DATABASE_URL", "APP_ENV", "SHUTDOWN_TIMEOUT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
}
