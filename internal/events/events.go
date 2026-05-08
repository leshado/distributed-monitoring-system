package events

import (
	"time"

	"distributed-monitoring-system/internal/domain"

	"github.com/google/uuid"
)

const (
	TopicChecksScheduled   = "checks.scheduled"
	TopicChecksExecuted    = "checks.executed"
	TopicIncidentsOpened   = "incidents.opened"
	TopicIncidentsResolved = "incidents.resolved"
	TopicAlertsTriggered   = "alerts.triggered"
)

type Envelope struct {
	EventID    string    `json:"event_id"`
	EventType  string    `json:"event_type"`
	OccurredAt time.Time `json:"occurred_at"`
	Producer   string    `json:"producer"`
	Schema     string    `json:"schema"`
}

type CheckScheduled struct {
	Envelope
	CheckID     domain.UUID      `json:"check_id"`
	ResourceID  domain.UUID      `json:"resource_id"`
	UserID      domain.UUID      `json:"user_id"`
	CheckType   domain.CheckType `json:"check_type"`
	ScheduledAt time.Time        `json:"scheduled_at"`
	DueAt       time.Time        `json:"due_at"`
	Attempt     int              `json:"attempt"`
}

type CheckExecuted struct {
	Envelope
	ExecutionID domain.UUID        `json:"execution_id"`
	CheckID     domain.UUID        `json:"check_id"`
	ResourceID  domain.UUID        `json:"resource_id"`
	UserID      domain.UUID        `json:"user_id"`
	CheckType   domain.CheckType   `json:"check_type"`
	Status      domain.CheckStatus `json:"status"`
	LatencyMs   int64              `json:"latency_ms"`
	StatusCode  int                `json:"status_code,omitempty"`
	Error       string             `json:"error,omitempty"`
	ObservedAt  time.Time          `json:"observed_at"`
	ScheduledAt time.Time          `json:"scheduled_at"`
	WorkerID    string             `json:"worker_id"`
}

type IncidentOpened struct {
	Envelope
	IncidentID      domain.UUID          `json:"incident_id"`
	CheckID         domain.UUID          `json:"check_id"`
	ResourceID      domain.UUID          `json:"resource_id"`
	UserID          domain.UUID          `json:"user_id"`
	State           domain.IncidentState `json:"state"`
	Severity        string               `json:"severity"`
	OpenReason      string               `json:"open_reason"`
	EscalationLevel int                  `json:"escalation_level"`
	OpenedAt        time.Time            `json:"opened_at"`
}

type IncidentResolved struct {
	Envelope
	IncidentID domain.UUID          `json:"incident_id"`
	CheckID    domain.UUID          `json:"check_id"`
	ResourceID domain.UUID          `json:"resource_id"`
	UserID     domain.UUID          `json:"user_id"`
	State      domain.IncidentState `json:"state"`
	ResolvedAt time.Time            `json:"resolved_at"`
	Reason     string               `json:"reason"`
}

type AlertTriggered struct {
	Envelope
	AlertID     domain.UUID      `json:"alert_id"`
	UserID      domain.UUID      `json:"user_id"`
	IncidentID  domain.UUID      `json:"incident_id"`
	CheckID     domain.UUID      `json:"check_id"`
	ResourceID  domain.UUID      `json:"resource_id"`
	Kind        domain.AlertKind `json:"kind"`
	Severity    string           `json:"severity"`
	Message     string           `json:"message"`
	DedupKey    string           `json:"dedup_key"`
	TriggeredAt time.Time        `json:"triggered_at"`
}

func NewEnvelope(eventType, producer string, now time.Time) Envelope {
	return Envelope{
		EventID:    uuid.NewString(),
		EventType:  eventType,
		OccurredAt: now.UTC(),
		Producer:   producer,
		Schema:     "v1",
	}
}
