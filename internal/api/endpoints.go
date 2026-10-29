package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/google/uuid"

	"webhookdelivery/internal/sign"
	"webhookdelivery/internal/ssrf"
	"webhookdelivery/internal/store"
)

const (
	maxEndpointBody = 16 << 10
	maxURLLength    = 2048
	maxEventTypes   = 50
	// maxEndpointConcurrency bounds what a tenant can ask for. The tenant's own
	// max_inflight still applies on top.
	maxEndpointConcurrency = 100
	defaultPageSize        = 50
	maxPageSize            = 100
)

var subscriptionPattern = regexp.MustCompile(`^[A-Za-z0-9._:\-*]{1,128}$`)

type endpointView struct {
	ID          string   `json:"id"`
	URL         string   `json:"url"`
	Description string   `json:"description"`
	EventTypes  []string `json:"event_types"`
	Status      string   `json:"status"`
	// MaxConcurrency is how many deliveries may be in flight to this endpoint at once.
	MaxConcurrency int        `json:"max_concurrency"`
	Health         healthView `json:"health"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	// DisabledAt and DisabledReason are set when the endpoint was auto-disabled
	// for failing continuously. PATCH status=active re-enables it.
	DisabledAt     *time.Time `json:"disabled_at,omitempty"`
	DisabledReason *string    `json:"disabled_reason,omitempty"`
	// Secret is only returned when the endpoint is created.
	Secret string `json:"secret,omitempty"`
}

// healthView exposes the circuit breaker state so tenants can see why an
// endpoint is not being attempted.
type healthView struct {
	ConsecutiveFailures int        `json:"consecutive_failures"`
	FailingSince        *time.Time `json:"failing_since"`
	CircuitOpenUntil    *time.Time `json:"circuit_open_until"`
}

func viewOf(e store.Endpoint) endpointView {
	return endpointView{ID: e.ID, URL: e.URL, Description: e.Description, EventTypes: e.EventTypes,
		Status: e.Status, MaxConcurrency: e.MaxConcurrency, CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt,
		Health: healthView{ConsecutiveFailures: e.ConsecutiveFailures, FailingSince: e.FailingSince,
			CircuitOpenUntil: e.CircuitOpenUntil},
		DisabledAt: e.DisabledAt, DisabledReason: e.DisabledReason}
}

func (a *API) validateURL(raw string) error {
	if len(raw) > maxURLLength {
		return fmt.Errorf("url must be at most %d characters", maxURLLength)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return errors.New("url must be an absolute URL")
	}
	if schemeOK := u.Scheme == "https" || (u.Scheme == "http" && a.opts.AllowInsecureURLs); !schemeOK {
		return errors.New("url must use https")
	}
	if u.User != nil {
		return errors.New("url must not contain credentials")
	}
	// Fast feedback only: the sender re-checks the resolved address at connect
	// time, which is what actually prevents SSRF. Development allows internal hosts.
	if !a.opts.AllowInsecureURLs {
		if err := ssrf.ValidateHost(u.Hostname()); err != nil {
			return err
		}
	}
	return nil
}

func validateSubscriptions(types []string) error {
	if len(types) > maxEventTypes {
		return fmt.Errorf("at most %d event_types are allowed", maxEventTypes)
	}
	for _, t := range types {
		if !subscriptionPattern.MatchString(t) {
			return fmt.Errorf("invalid event type %q", t)
		}
	}
	return nil
}

func (a *API) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL            string   `json:"url"`
		Description    string   `json:"description"`
		EventTypes     []string `json:"event_types"`
		Secret         string   `json:"secret"`
		MaxConcurrency *int     `json:"max_concurrency"`
	}
	if !a.decode(w, r, &req, maxEndpointBody) {
		return
	}
	if msg := validateConcurrency(req.MaxConcurrency); msg != "" {
		a.writeError(w, http.StatusBadRequest, "invalid_request", msg)
		return
	}
	if err := a.validateURL(req.URL); err != nil {
		a.writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if len(req.EventTypes) == 0 {
		req.EventTypes = []string{"*"}
	}
	if err := validateSubscriptions(req.EventTypes); err != nil {
		a.writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
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

	ep, err := a.store.CreateEndpoint(r.Context(), store.Endpoint{
		TenantID:       tenantID(r),
		URL:            req.URL,
		Description:    req.Description,
		EventTypes:     req.EventTypes,
		MaxConcurrency: derefInt(req.MaxConcurrency),
		Secrets:        []store.Secret{{ID: "sec_" + uuid.NewString(), Value: secret, CreatedAt: time.Now().UTC()}},
	})
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	v := viewOf(ep)
	v.Secret = secret
	writeJSON(w, http.StatusCreated, v)
}

func (a *API) listEndpoints(w http.ResponseWriter, r *http.Request) {
	limit := defaultPageSize
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > maxPageSize {
			a.writeError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("limit must be between 1 and %d", maxPageSize))
			return
		}
		limit = n
	}
	after := r.URL.Query().Get("after")
	if after != "" {
		if _, err := uuid.Parse(after); err != nil {
			a.writeError(w, http.StatusBadRequest, "invalid_request", "after must be an endpoint id")
			return
		}
	}

	eps, err := a.store.ListEndpoints(r.Context(), tenantID(r), after, limit+1)
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	var next *string
	if len(eps) > limit {
		eps = eps[:limit]
		next = &eps[limit-1].ID
	}
	views := make([]endpointView, len(eps))
	for i, e := range eps {
		views[i] = viewOf(e)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": views, "next_after": next})
}

// pathID returns the {id} path value, or writes a 404 if it is not a UUID.
func (a *API) pathID(w http.ResponseWriter, r *http.Request, what string) (string, bool) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		a.writeError(w, http.StatusNotFound, "not_found", what+" not found")
		return "", false
	}
	return id, true
}

func (a *API) getEndpoint(w http.ResponseWriter, r *http.Request) {
	id, ok := a.pathID(w, r, "endpoint")
	if !ok {
		return
	}
	ep, err := a.store.GetEndpoint(r.Context(), tenantID(r), id)
	if a.handleStoreError(w, r, err, "endpoint") {
		return
	}
	writeJSON(w, http.StatusOK, viewOf(ep))
}

func (a *API) updateEndpoint(w http.ResponseWriter, r *http.Request) {
	id, ok := a.pathID(w, r, "endpoint")
	if !ok {
		return
	}
	var req struct {
		URL            *string   `json:"url"`
		Description    *string   `json:"description"`
		EventTypes     *[]string `json:"event_types"`
		Status         *string   `json:"status"`
		MaxConcurrency *int      `json:"max_concurrency"`
	}
	if !a.decode(w, r, &req, maxEndpointBody) {
		return
	}
	if msg := validateConcurrency(req.MaxConcurrency); msg != "" {
		a.writeError(w, http.StatusBadRequest, "invalid_request", msg)
		return
	}
	if req.URL != nil {
		if err := a.validateURL(*req.URL); err != nil {
			a.writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
	}
	if req.EventTypes != nil {
		if len(*req.EventTypes) == 0 {
			a.writeError(w, http.StatusBadRequest, "invalid_request", "event_types must not be empty")
			return
		}
		if err := validateSubscriptions(*req.EventTypes); err != nil {
			a.writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
	}
	if req.Status != nil && *req.Status != "active" && *req.Status != "paused" {
		a.writeError(w, http.StatusBadRequest, "invalid_request", "status must be active or paused")
		return
	}

	ep, err := a.store.UpdateEndpoint(r.Context(), tenantID(r), id, store.EndpointPatch{
		URL: req.URL, Description: req.Description, EventTypes: req.EventTypes, Status: req.Status,
		MaxConcurrency: req.MaxConcurrency,
	})
	if a.handleStoreError(w, r, err, "endpoint") {
		return
	}
	writeJSON(w, http.StatusOK, viewOf(ep))
}

func (a *API) deleteEndpoint(w http.ResponseWriter, r *http.Request) {
	id, ok := a.pathID(w, r, "endpoint")
	if !ok {
		return
	}
	if a.handleStoreError(w, r, a.store.DeleteEndpoint(r.Context(), tenantID(r), id), "endpoint") {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleStoreError writes the response for err and reports whether it did.
func (a *API) handleStoreError(w http.ResponseWriter, r *http.Request, err error, what string) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, store.ErrNotFound):
		a.writeError(w, http.StatusNotFound, "not_found", what+" not found")
	default:
		a.internalError(w, r, err)
	}
	return true
}

func validateConcurrency(n *int) string {
	if n != nil && (*n < 1 || *n > maxEndpointConcurrency) {
		return fmt.Sprintf("max_concurrency must be between 1 and %d", maxEndpointConcurrency)
	}
	return ""
}

func derefInt(n *int) int {
	if n == nil {
		return 0
	}
	return *n
}
