package api

import (
	"errors"
	"net/http"
	"time"

	"webhookdelivery/internal/sign"
	"webhookdelivery/internal/store"
)

const (
	defaultRotationGrace = 24 * time.Hour
	maxRotationGrace     = 7 * 24 * time.Hour
)

func (a *API) listSecrets(w http.ResponseWriter, r *http.Request) {
	id, ok := a.pathID(w, r, "endpoint")
	if !ok {
		return
	}
	infos, err := a.store.ListSecrets(r.Context(), tenantID(r), id)
	if a.handleStoreError(w, r, err, "endpoint") {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": infos})
}

// rotateSecret adds a new signing secret. For the grace period the old secret
// keeps signing too, so deliveries carry a signature for both and the receiver
// can be switched over without losing any.
func (a *API) rotateSecret(w http.ResponseWriter, r *http.Request) {
	id, ok := a.pathID(w, r, "endpoint")
	if !ok {
		return
	}
	var req struct {
		GracePeriod string `json:"grace_period"` // Go duration, e.g. "24h"; "0s" retires the old secret at once
		Secret      string `json:"secret"`       // optional whsec_ value; generated if omitted
	}
	if r.ContentLength != 0 && !a.decode(w, r, &req, maxEndpointBody) {
		return
	}

	grace := defaultRotationGrace
	if req.GracePeriod != "" {
		d, err := time.ParseDuration(req.GracePeriod)
		if err != nil || d < 0 || d > maxRotationGrace {
			a.writeError(w, http.StatusBadRequest, "invalid_request", "grace_period must be a duration between 0s and 168h")
			return
		}
		grace = d
	}
	secret := req.Secret
	if secret == "" {
		var err error
		if secret, err = sign.GenerateSecret(); err != nil {
			a.internalError(w, r, err)
			return
		}
	} else if _, err := sign.ParseSecret(secret); err != nil {
		a.writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	created, all, err := a.store.RotateSecret(r.Context(), tenantID(r), id, secret, grace)
	if errors.Is(err, store.ErrTooManySecrets) {
		a.writeError(w, http.StatusConflict, "too_many_secrets", err.Error())
		return
	}
	if a.handleStoreError(w, r, err, "endpoint") {
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"id":         created.ID,
		"secret":     secret, // shown once
		"created_at": created.CreatedAt,
		"secrets":    all,
	})
}

func (a *API) revokeSecret(w http.ResponseWriter, r *http.Request) {
	id, ok := a.pathID(w, r, "endpoint")
	if !ok {
		return
	}
	secretID := r.PathValue("secretId")
	if len(secretID) > 64 {
		a.writeError(w, http.StatusNotFound, "not_found", "secret not found")
		return
	}
	err := a.store.RevokeSecret(r.Context(), tenantID(r), id, secretID)
	if errors.Is(err, store.ErrLastSecret) {
		a.writeError(w, http.StatusConflict, "last_secret", err.Error())
		return
	}
	if errors.Is(err, store.ErrNotFound) {
		a.writeError(w, http.StatusNotFound, "not_found", "endpoint or secret not found")
		return
	}
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
