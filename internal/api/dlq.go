package api

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"webhookdelivery/internal/store"
)

const (
	maxReplayBatch     = 1000
	defaultReplayBatch = 100
	maxReplayRate      = 1000.0
	defaultReplayRate  = 10.0
)

var deadReasons = map[string]bool{
	store.DeadMaxAttempts:      true,
	store.DeadEndpointGone:     true,
	store.DeadLeaseExpired:     true,
	store.DeadEndpointDisabled: true,
}

// parseDeadFilter reads the endpoint_id, reason and since filters shared by
// DLQ listing and bulk replay. On failure it returns a message for a 400.
func parseDeadFilter(endpointID, reason, since string) (store.DeadFilter, string) {
	var f store.DeadFilter
	if endpointID != "" {
		if _, err := uuid.Parse(endpointID); err != nil {
			return f, "endpoint_id must be an endpoint id"
		}
		f.EndpointID = endpointID
	}
	if reason != "" {
		if !deadReasons[reason] {
			return f, "reason must be one of max_attempts, endpoint_gone, endpoint_disabled, lease_expired"
		}
		f.Reason = reason
	}
	if since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return f, "since must be an RFC 3339 timestamp"
		}
		f.Since = &t
	}
	return f, ""
}

func encodeCursor(c store.DeadCursor) string {
	return base64.RawURLEncoding.EncodeToString([]byte(c.DeadAt.UTC().Format(time.RFC3339Nano) + "|" + c.ID))
}

func decodeCursor(s string) (*store.DeadCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	at, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return nil, fmt.Errorf("malformed cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return nil, err
	}
	if _, err := uuid.Parse(id); err != nil {
		return nil, err
	}
	return &store.DeadCursor{DeadAt: t, ID: id}, nil
}

func (a *API) listDLQ(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f, msg := parseDeadFilter(q.Get("endpoint_id"), q.Get("reason"), q.Get("since"))
	if msg != "" {
		a.writeError(w, http.StatusBadRequest, "invalid_request", msg)
		return
	}
	limit := defaultPageSize
	if s := q.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > maxPageSize {
			a.writeError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("limit must be between 1 and %d", maxPageSize))
			return
		}
		limit = n
	}
	var cursor *store.DeadCursor
	if s := q.Get("after"); s != "" {
		var err error
		if cursor, err = decodeCursor(s); err != nil {
			a.writeError(w, http.StatusBadRequest, "invalid_request", "after is not a valid cursor")
			return
		}
	}

	rows, err := a.store.ListDead(r.Context(), tenantID(r), f, cursor, limit+1)
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	var next *string
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[limit-1]
		c := encodeCursor(store.DeadCursor{DeadAt: last.DeadAt, ID: last.DeliveryID})
		next = &c
	}
	if rows == nil {
		rows = []store.DeadDelivery{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": rows, "next_after": next})
}

func (a *API) getDelivery(w http.ResponseWriter, r *http.Request) {
	id, ok := a.pathID(w, r, "delivery")
	if !ok {
		return
	}
	d, err := a.store.GetDelivery(r.Context(), tenantID(r), id)
	if a.handleStoreError(w, r, err, "delivery") {
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (a *API) replayDelivery(w http.ResponseWriter, r *http.Request) {
	id, ok := a.pathID(w, r, "delivery")
	if !ok {
		return
	}
	err := a.store.ReplayDelivery(r.Context(), tenantID(r), id)
	if errors.Is(err, store.ErrConflict) {
		a.writeError(w, http.StatusConflict, "not_dead", "only dead-lettered deliveries can be replayed")
		return
	}
	if a.handleStoreError(w, r, err, "delivery") {
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"replayed": 1})
}

// replayDLQ requeues dead deliveries in bulk. Their due times are staggered so
// they are sent at rate_per_second, which keeps a replay from swamping a
// receiver that has only just recovered.
func (a *API) replayDLQ(w http.ResponseWriter, r *http.Request) {
	var req struct {
		EndpointID    string   `json:"endpoint_id"`
		Reason        string   `json:"reason"`
		Since         string   `json:"since"`
		Limit         int      `json:"limit"`
		RatePerSecond *float64 `json:"rate_per_second"`
	}
	if !a.decode(w, r, &req, maxEndpointBody) {
		return
	}
	f, msg := parseDeadFilter(req.EndpointID, req.Reason, req.Since)
	if msg != "" {
		a.writeError(w, http.StatusBadRequest, "invalid_request", msg)
		return
	}
	if req.Limit == 0 {
		req.Limit = defaultReplayBatch
	}
	if req.Limit < 1 || req.Limit > maxReplayBatch {
		a.writeError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("limit must be between 1 and %d", maxReplayBatch))
		return
	}
	rate := defaultReplayRate
	if req.RatePerSecond != nil {
		rate = *req.RatePerSecond
	}
	if rate <= 0 || rate > maxReplayRate {
		a.writeError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("rate_per_second must be in (0, %d]", int(maxReplayRate)))
		return
	}

	n, err := a.store.ReplayDead(r.Context(), tenantID(r), f, req.Limit, rate)
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"replayed": n})
}
