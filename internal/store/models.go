package store

import (
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound  = errors.New("store: not found")
	ErrConflict  = errors.New("store: resource is not in a state that allows this operation")
	ErrLeaseLost = errors.New("store: delivery lease no longer held")

	ErrLastSecret     = errors.New("store: an endpoint must keep at least one active signing secret")
	ErrTooManySecrets = errors.New("store: too many active signing secrets; wait for old ones to expire or revoke one")
	errNoSealer       = errors.New("store: no secret sealer configured")
)

// Reasons recorded in deliveries.dead_reason when a delivery enters the DLQ.
const (
	DeadMaxAttempts  = "max_attempts"
	DeadEndpointGone = "endpoint_gone"
	DeadLeaseExpired = "lease_expired"
	// DeadEndpointDisabled marks deliveries that were still waiting when an
	// endpoint was auto-disabled for failing continuously.
	DeadEndpointDisabled = "endpoint_disabled"
)

// Secret is one signing secret on an endpoint as persisted: the value is only
// ever stored encrypted (Sealed). Value holds the plaintext in memory, either
// as input to CreateEndpoint or after a claim has decrypted it; it is never
// serialized.
type Secret struct {
	ID        string     `json:"id"`
	Sealed    string     `json:"sealed,omitempty"`
	Legacy    string     `json:"value,omitempty"` // plaintext written before encryption existed; see `admin encrypt-secrets`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"` // nil: active until revoked or rotated out
	Value     string     `json:"-"`
}

// Live reports whether the secret should still sign deliveries at time now.
func (s Secret) Live(now time.Time) bool { return s.ExpiresAt == nil || s.ExpiresAt.After(now) }

// SecretInfo is the non-sensitive description of a secret.
type SecretInfo struct {
	ID        string     `json:"id"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at"`
}

type Endpoint struct {
	ID             string
	TenantID       string
	URL            string
	Description    string
	EventTypes     []string
	Secrets        []Secret
	Status         string
	MaxConcurrency int
	CreatedAt      time.Time
	UpdatedAt      time.Time

	// Health, maintained by the circuit breaker.
	ConsecutiveFailures int
	CircuitOpenUntil    *time.Time
	FailingSince        *time.Time
	DisabledAt          *time.Time
	DisabledReason      *string
}

// EndpointPatch holds optional changes; nil fields are left untouched.
type EndpointPatch struct {
	URL         *string
	Description *string
	EventTypes  *[]string
	Status      *string

	MaxConcurrency *int
}

type Tenant struct {
	ID   string
	Name string
}

type IngestResult struct {
	EventID    string
	Deliveries int
	Duplicate  bool
}

type DeliverySummary struct {
	ID             string     `json:"id"`
	EndpointID     string     `json:"endpoint_id"`
	Status         string     `json:"status"`
	AttemptCount   int        `json:"attempt_count"`
	LastStatusCode *int       `json:"last_status_code"`
	LastError      *string    `json:"last_error"`
	NextAttemptAt  time.Time  `json:"next_attempt_at"`
	CreatedAt      time.Time  `json:"created_at"`
	DeadAt         *time.Time `json:"dead_at,omitempty"`
	DeadReason     *string    `json:"dead_reason,omitempty"`
}

// AttemptRecord is one row of a delivery's attempt history.
type AttemptRecord struct {
	AttemptNo           int       `json:"attempt_no"`
	StartedAt           time.Time `json:"started_at"`
	DurationMS          *int      `json:"duration_ms"`
	StatusCode          *int      `json:"status_code"`
	Error               *string   `json:"error"`
	ResponseBodySnippet *string   `json:"response_body_snippet"`
}

// DeliveryDetail is a delivery with its full attempt history.
type DeliveryDetail struct {
	DeliverySummary
	EventID     string          `json:"event_id"`
	EventType   string          `json:"event_type"`
	MaxAttempts int             `json:"max_attempts"`
	Attempts    []AttemptRecord `json:"attempts"`
}

// DeadDelivery is one DLQ entry.
type DeadDelivery struct {
	DeliveryID     string    `json:"delivery_id"`
	EventID        string    `json:"event_id"`
	EventType      string    `json:"event_type"`
	EndpointID     string    `json:"endpoint_id"`
	Reason         string    `json:"reason"`
	AttemptCount   int       `json:"attempt_count"`
	LastStatusCode *int      `json:"last_status_code"`
	LastError      *string   `json:"last_error"`
	DeadAt         time.Time `json:"dead_at"`
	CreatedAt      time.Time `json:"created_at"`
}

// DeadFilter narrows DLQ listings and bulk replays. Zero values mean "any".
type DeadFilter struct {
	DeliveryID string
	EndpointID string
	Reason     string
	Since      *time.Time
}

// DeadCursor is the keyset position of a DLQ page (dead_at DESC, id DESC).
type DeadCursor struct {
	DeadAt time.Time
	ID     string
}

// ClaimedDelivery is a delivery leased to a worker, joined with everything
// needed to send it.
type ClaimedDelivery struct {
	ID             string
	TenantID       string
	EventID        string
	EndpointID     string
	Attempt        int // number of this attempt (1-based)
	MaxAttempts    int
	EventType      string
	Payload        json.RawMessage
	EventCreatedAt time.Time
	URL            string
	Secrets        []Secret // decrypted (Value set), including ones that have expired
	SecretsErr     error    // set if the endpoint's secrets could not be decrypted
}

// Attempt is the outcome of one send, recorded in the attempts table.
type Attempt struct {
	StartedAt   time.Time
	Duration    time.Duration
	StatusCode  int // 0 if no response
	Error       string
	BodySnippet string
	// Local marks a failure on our side (for example undecryptable secrets)
	// rather than the endpoint's: it still consumes an attempt but does not
	// count against the endpoint's circuit breaker.
	Local bool
}
