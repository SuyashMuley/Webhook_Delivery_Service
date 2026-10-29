package config

import (
	"strings"
	"testing"
	"time"

	"webhookdelivery/internal/secretbox"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	c, err := load(env(map[string]string{"DATABASE_URL": "postgres://x"}), ":8080", "")
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.Env != "dev" || c.LogFormat != "text" || c.ShutdownTimeout != 30*time.Second {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestLoadProdDefaultsToJSONLogs(t *testing.T) {
	key, _ := secretbox.GenerateKey("k1")
	c, err := load(env(map[string]string{"DATABASE_URL": "postgres://x", "APP_ENV": "prod", "SECRET_ENCRYPTION_KEYS": key}), ":8080", "")
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
	}), ":8080", "")
	if err == nil || !strings.Contains(err.Error(), "LEASE_DURATION") {
		t.Fatalf("want lease error, got %v", err)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := load(env(map[string]string{"APP_ENV": "staging", "SHUTDOWN_TIMEOUT": "soon"}), ":8080", "")
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"DATABASE_URL", "APP_ENV", "SHUTDOWN_TIMEOUT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
}

func TestLoadSecurityDefaults(t *testing.T) {
	dev, err := load(env(map[string]string{"DATABASE_URL": "postgres://x"}), ":8080", "webhooks_api")
	if err != nil {
		t.Fatal(err)
	}
	if !dev.AllowPrivateTargets || dev.DBRole != "webhooks_api" {
		t.Fatalf("dev defaults: %+v", dev)
	}
	if ring, err := dev.Keyring(); err != nil || ring.ActiveKeyID() != "dev" {
		t.Fatalf("dev should fall back to the dev keyring: %v", err)
	}

	key, _ := secretbox.GenerateKey("k1")
	prod, err := load(env(map[string]string{"DATABASE_URL": "postgres://x", "APP_ENV": "prod", "SECRET_ENCRYPTION_KEYS": key}), ":8080", "webhooks_worker")
	if err != nil {
		t.Fatal(err)
	}
	if prod.AllowPrivateTargets {
		t.Fatal("production must not allow private targets by default")
	}
	if ring, _ := prod.Keyring(); ring.ActiveKeyID() != "k1" {
		t.Fatal("production keyring not loaded")
	}

	off, _ := load(env(map[string]string{"DATABASE_URL": "postgres://x", "DB_ROLE": "none"}), ":8080", "webhooks_api")
	if off.DBRole != "" {
		t.Fatalf("DB_ROLE=none should disable role switching, got %q", off.DBRole)
	}
}

func TestProductionRequiresEncryptionKeys(t *testing.T) {
	_, err := load(env(map[string]string{"DATABASE_URL": "postgres://x", "APP_ENV": "prod"}), ":8080", "")
	if err == nil || !strings.Contains(err.Error(), "SECRET_ENCRYPTION_KEYS") {
		t.Fatalf("want SECRET_ENCRYPTION_KEYS error, got %v", err)
	}
	_, err = load(env(map[string]string{"DATABASE_URL": "postgres://x", "SECRET_ENCRYPTION_KEYS": "garbage"}), ":8080", "")
	if err == nil || !strings.Contains(err.Error(), "SECRET_ENCRYPTION_KEYS") {
		t.Fatalf("a malformed key must be rejected even in dev, got %v", err)
	}
}

func TestLoadRetrySchedule(t *testing.T) {
	c, err := load(env(map[string]string{"DATABASE_URL": "postgres://x", "RETRY_SCHEDULE": "1s, 2m,1h"}), ":8080", "")
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{time.Second, 2 * time.Minute, time.Hour}
	if len(c.RetrySchedule) != 3 || c.RetrySchedule[0] != want[0] || c.RetrySchedule[1] != want[1] || c.RetrySchedule[2] != want[2] {
		t.Fatalf("schedule = %v, want %v", c.RetrySchedule, want)
	}

	if _, err := load(env(map[string]string{"DATABASE_URL": "postgres://x", "RETRY_SCHEDULE": "1s,nope,-5s"}), ":8080", ""); err == nil ||
		!strings.Contains(err.Error(), "RETRY_SCHEDULE") {
		t.Fatalf("want RETRY_SCHEDULE error, got %v", err)
	}
}

func TestLoadDefaultRetrySettings(t *testing.T) {
	c, err := load(env(map[string]string{"DATABASE_URL": "postgres://x"}), ":8080", "")
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxAttempts != 8 || len(c.RetrySchedule) != 7 || c.TenantSlice != 5 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestOperationalDefaults(t *testing.T) {
	c, err := load(env(map[string]string{"DATABASE_URL": "postgres://x"}), ":8080", "")
	if err != nil {
		t.Fatal(err)
	}
	if c.MetricsAddr != ":9090" || c.DrainDelay != 3*time.Second || c.WorkerDrainTimeout != 20*time.Second ||
		c.RetainSucceeded != 7*24*time.Hour || c.RetainDead != 30*24*time.Hour || c.PurgeInterval != 5*time.Minute {
		t.Fatalf("unexpected operational defaults: %+v", c)
	}
	if c.TraceSampleRatio != 1 { // dev traces everything
		t.Fatalf("dev sample ratio = %v, want 1", c.TraceSampleRatio)
	}
	key, _ := secretbox.GenerateKey("k1")
	prod, err := load(env(map[string]string{"DATABASE_URL": "postgres://x", "APP_ENV": "prod", "SECRET_ENCRYPTION_KEYS": key}), ":8080", "")
	if err != nil {
		t.Fatal(err)
	}
	if prod.TraceSampleRatio != 0.1 {
		t.Fatalf("prod sample ratio = %v, want 0.1 (sample a tenth of traces)", prod.TraceSampleRatio)
	}
}

func TestOperationalSettingsAreValidated(t *testing.T) {
	c, err := load(env(map[string]string{
		"DATABASE_URL": "postgres://x", "METRICS_ADDR": "", "OTEL_EXPORTER_OTLP_ENDPOINT": "http://jaeger:4318/",
		"OTEL_TRACES_SAMPLER_ARG": "0.25", "PURGE_INTERVAL": "0s",
	}), ":8080", "")
	if err != nil {
		t.Fatal(err)
	}
	if c.OTLPEndpoint != "http://jaeger:4318" || c.TraceSampleRatio != 0.25 || c.PurgeInterval != 0 {
		t.Fatalf("settings not parsed: %+v", c)
	}
	for _, bad := range []string{"1.5", "-1", "lots"} {
		if _, err := load(env(map[string]string{"DATABASE_URL": "postgres://x", "OTEL_TRACES_SAMPLER_ARG": bad}), ":8080", ""); err == nil ||
			!strings.Contains(err.Error(), "OTEL_TRACES_SAMPLER_ARG") {
			t.Errorf("sampler ratio %q accepted: %v", bad, err)
		}
	}
}
