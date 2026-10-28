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
)

// Reasons recorded in deliveries.dead_reason when a delivery enters the DLQ.
const (
	DeadMaxAttempts  = "max_attempts"
	DeadEndpointGone = "endpoint_gone"
	DeadLeaseExpired = "lease_expired"
)

// Secret is one signing secret on an endpoint. Value is stored as-is for now;
// envelope encryption arrives with the security phase.
type Secret struct {
	ID        string    `json:"id"`
	Value     string    `json:"value"`
	CreatedAt time.Time `json:"created_at"`
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
}

// EndpointPatch holds optional changes; nil fields are left untouched.
type EndpointPatch struct {
	URL         *string
	Description *string
	EventTypes  *[]string
	Status      *string
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
	Secrets        []Secret
}

// Attempt is the outcome of one send, recorded in the attempts table.
type Attempt struct {
	StartedAt   time.Time
	Duration    time.Duration
	StatusCode  int // 0 if no response
	Error       string
	BodySnippet string
}
