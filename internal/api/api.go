// Package api implements the tenant-facing HTTP API (/v1/...).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"webhookdelivery/internal/auth"
	"webhookdelivery/internal/store"
)

type Options struct {
	AllowInsecureURLs bool // permit http:// endpoint URLs (dev only)
	MaxAttempts       int  // attempts per delivery before it is dead-lettered
}

type API struct {
	store *store.Store
	log   *slog.Logger
	opts  Options
}

func New(s *store.Store, log *slog.Logger, opts Options) *API {
	if opts.MaxAttempts < 1 {
		opts.MaxAttempts = 8
	}
	return &API{store: s, log: log, opts: opts}
}

// Register mounts all /v1 routes, each behind API-key authentication.
func (a *API) Register(mux *http.ServeMux) {
	mux.Handle("POST /v1/endpoints", a.authenticate(a.createEndpoint))
	mux.Handle("GET /v1/endpoints", a.authenticate(a.listEndpoints))
	mux.Handle("GET /v1/endpoints/{id}", a.authenticate(a.getEndpoint))
	mux.Handle("PATCH /v1/endpoints/{id}", a.authenticate(a.updateEndpoint))
	mux.Handle("DELETE /v1/endpoints/{id}", a.authenticate(a.deleteEndpoint))

	mux.Handle("GET /v1/endpoints/{id}/secrets", a.authenticate(a.listSecrets))
	mux.Handle("POST /v1/endpoints/{id}/secrets/rotate", a.authenticate(a.rotateSecret))
	mux.Handle("DELETE /v1/endpoints/{id}/secrets/{secretId}", a.authenticate(a.revokeSecret))

	mux.Handle("POST /v1/events", a.authenticate(a.createEvent))
	mux.Handle("GET /v1/events/{id}/deliveries", a.authenticate(a.listEventDeliveries))

	mux.Handle("GET /v1/deliveries/{id}", a.authenticate(a.getDelivery))
	mux.Handle("POST /v1/deliveries/{id}/replay", a.authenticate(a.replayDelivery))
	mux.Handle("GET /v1/dlq", a.authenticate(a.listDLQ))
	mux.Handle("POST /v1/dlq/replay", a.authenticate(a.replayDLQ))
}

type tenantKey struct{}

func (a *API) authenticate(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token == "" {
			a.writeError(w, http.StatusUnauthorized, "unauthorized", "missing or malformed Authorization header; expected 'Bearer <api key>'")
			return
		}
		tenantID, err := a.store.AuthenticateKey(r.Context(), auth.HashKey(token))
		switch {
		case errors.Is(err, store.ErrNotFound):
			a.writeError(w, http.StatusUnauthorized, "unauthorized", "invalid API key")
			return
		case err != nil:
			a.internalError(w, r, err)
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), tenantKey{}, tenantID)))
	})
}

func tenantID(r *http.Request) string {
	return r.Context().Value(tenantKey{}).(string)
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (a *API) writeError(w http.ResponseWriter, status int, code, msg string) {
	var b errorBody
	b.Error.Code, b.Error.Message = code, msg
	writeJSON(w, status, b)
}

func (a *API) internalError(w http.ResponseWriter, r *http.Request, err error) {
	a.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "error", err)
	a.writeError(w, http.StatusInternalServerError, "internal", "internal server error")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// decode reads a size-limited, strict JSON body into dst. On failure it writes
// the error response and returns false.
func (a *API) decode(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBytes))
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if err == nil && dec.More() {
		err = errors.New("unexpected data after JSON body")
	}
	if err == nil {
		return true
	}
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		a.writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "request body too large")
	case errors.Is(err, io.EOF):
		a.writeError(w, http.StatusBadRequest, "invalid_request", "request body is empty")
	default:
		a.writeError(w, http.StatusBadRequest, "invalid_request", "invalid JSON: "+err.Error())
	}
	return false
}
