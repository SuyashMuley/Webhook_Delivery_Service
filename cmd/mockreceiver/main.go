// Command mockreceiver is a development webhook receiver. It logs every
// delivery it gets on POST /hook/{name}, verifies the signature when
// RECEIVER_SECRET is set, and always answers 200 for valid requests.
package main

import (
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"webhookdelivery/internal/sign"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("service", "mockreceiver")

	var secrets [][]byte
	if s := os.Getenv("RECEIVER_SECRET"); s != "" {
		key, err := sign.ParseSecret(s)
		if err != nil {
			log.Error("invalid RECEIVER_SECRET", "error", err)
			os.Exit(1)
		}
		secrets = append(secrets, key)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("POST /hook/{name}", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		id := r.Header.Get("webhook-id")
		if len(secrets) > 0 {
			err := sign.Verify(r.Header.Get("webhook-signature"), secrets, id,
				r.Header.Get("webhook-timestamp"), body, 5*time.Minute, time.Now())
			if err != nil {
				log.Warn("rejected delivery", "hook", r.PathValue("name"), "webhook_id", id, "error", err)
				http.Error(w, "invalid signature", http.StatusUnauthorized)
				return
			}
		}
		log.Info("received delivery", "hook", r.PathValue("name"), "webhook_id", id,
			"verified", len(secrets) > 0, "body", string(body))
		_, _ = w.Write([]byte("ok"))
	})

	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":9000"
	}
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Info("listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
