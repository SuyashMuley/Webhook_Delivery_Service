package store

import (
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound  = errors.New("store: not found")
	ErrLeaseLost = errors.New("store: delivery lease no longer held")
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
}

// ClaimedDelivery is a delivery leased to a worker, joined with everything
// needed to send it.
type ClaimedDelivery struct {
	ID             string
	TenantID       string
	EventID        string
	EndpointID     string
	Attempt        int // number of this attempt (1-based)
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
