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

func TestLoadRetrySchedule(t *testing.T) {
	c, err := load(env(map[string]string{"DATABASE_URL": "postgres://x", "RETRY_SCHEDULE": "1s, 2m,1h"}), ":8080")
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{time.Second, 2 * time.Minute, time.Hour}
	if len(c.RetrySchedule) != 3 || c.RetrySchedule[0] != want[0] || c.RetrySchedule[1] != want[1] || c.RetrySchedule[2] != want[2] {
		t.Fatalf("schedule = %v, want %v", c.RetrySchedule, want)
	}

	if _, err := load(env(map[string]string{"DATABASE_URL": "postgres://x", "RETRY_SCHEDULE": "1s,nope,-5s"}), ":8080"); err == nil ||
		!strings.Contains(err.Error(), "RETRY_SCHEDULE") {
		t.Fatalf("want RETRY_SCHEDULE error, got %v", err)
	}
}

func TestLoadDefaultRetrySettings(t *testing.T) {
	c, err := load(env(map[string]string{"DATABASE_URL": "postgres://x"}), ":8080")
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxAttempts != 8 || len(c.RetrySchedule) != 7 || c.TenantSlice != 5 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}
