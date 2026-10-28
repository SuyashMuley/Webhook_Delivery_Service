package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"regexp"
)

const (
	maxEventPayload = 256 << 10
	maxIdempotency  = 255
)

var eventTypePattern = regexp.MustCompile(`^[A-Za-z0-9._:\-]{1,128}$`)

func (a *API) createEvent(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	// Allow a little overhead on top of the payload limit for the envelope fields.
	if !a.decode(w, r, &req, maxEventPayload+1<<10) {
		return
	}
	if !eventTypePattern.MatchString(req.Type) {
		a.writeError(w, http.StatusBadRequest, "invalid_request", "type is required (letters, digits and . _ : -, up to 128 chars)")
		return
	}
	if len(req.Payload) == 0 || bytes.Equal(bytes.TrimSpace(req.Payload), []byte("null")) {
		a.writeError(w, http.StatusBadRequest, "invalid_request", "payload is required")
		return
	}
	if len(req.Payload) > maxEventPayload {
		a.writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "payload exceeds 256KB")
		return
	}
	idem := r.Header.Get("Idempotency-Key")
	if len(idem) > maxIdempotency {
		a.writeError(w, http.StatusBadRequest, "invalid_request", "Idempotency-Key is too long")
		return
	}

	res, err := a.store.IngestEvent(r.Context(), tenantID(r), req.Type, req.Payload, idem, a.opts.MaxAttempts)
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	status := http.StatusAccepted
	if res.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"id":         res.EventID,
		"deliveries": res.Deliveries,
		"duplicate":  res.Duplicate,
	})
}

func (a *API) listEventDeliveries(w http.ResponseWriter, r *http.Request) {
	id, ok := a.pathID(w, r, "event")
	if !ok {
		return
	}
	ds, err := a.store.ListEventDeliveries(r.Context(), tenantID(r), id)
	if a.handleStoreError(w, r, err, "event") {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": ds})
}
