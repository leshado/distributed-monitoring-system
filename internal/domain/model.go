package domain

import (
	"encoding/json"
	"time"
)

type UUID string

type CheckType string

const (
	CheckTypeHTTP CheckType = "http"
	CheckTypeTCP  CheckType = "tcp"
	CheckTypeTLS  CheckType = "tls"
)

type CheckStatus string

const (
	CheckStatusOK   CheckStatus = "ok"
	CheckStatusFail CheckStatus = "fail"
)

type IncidentState string

const (
	IncidentStateOK         IncidentState = "OK"
	IncidentStateDegraded   IncidentState = "DEGRADED"
	IncidentStateDown       IncidentState = "DOWN"
	IncidentStateRecovering IncidentState = "RECOVERING"
)

type AlertKind string

const (
	AlertKindIncidentOpened   AlertKind = "incident_opened"
	AlertKindIncidentResolved AlertKind = "incident_resolved"
	AlertKindEscalation       AlertKind = "incident_escalation"
)

type User struct {
	ID                   UUID       `json:"id"`
	TelegramChatID       int64      `json:"telegram_chat_id"`
	Username             string     `json:"username"`
	AlertIntervalMinutes int        `json:"alert_interval_minutes"`
	SilencedUntil        *time.Time `json:"silenced_until,omitempty"`
	Timezone             string     `json:"timezone"`
	CreatedAt            time.Time  `json:"created_at"`
}

type Resource struct {
	ID        UUID      `json:"id"`
	UserID    UUID      `json:"user_id"`
	Name      string    `json:"name"`
	Target    string    `json:"target"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type HTTPCheckConfig struct {
	Method       string            `json:"method"`
	URL          string            `json:"url"`
	Headers      map[string]string `json:"headers,omitempty"`
	ExpectedCode int               `json:"expected_code"`
	BodyContains string            `json:"body_contains,omitempty"`
	JSONPath     string            `json:"json_path,omitempty"`
	JSONEquals   string            `json:"json_equals,omitempty"`
}

type TCPCheckConfig struct {
	Address string `json:"address"`
}

type TLSCheckConfig struct {
	Address            string `json:"address"`
	ServerName         string `json:"server_name,omitempty"`
	MinDaysValidity    int    `json:"min_days_validity"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify,omitempty"`
}

type CheckConfig struct {
	HTTP *HTTPCheckConfig `json:"http,omitempty"`
	TCP  *TCPCheckConfig  `json:"tcp,omitempty"`
	TLS  *TLSCheckConfig  `json:"tls,omitempty"`
}

type Check struct {
	ID             UUID            `json:"id"`
	ResourceID     UUID            `json:"resource_id"`
	UserID         UUID            `json:"user_id"`
	Type           CheckType       `json:"type"`
	Name           string          `json:"name"`
	Interval       time.Duration   `json:"interval"`
	Timeout        time.Duration   `json:"timeout"`
	Jitter         time.Duration   `json:"jitter"`
	Enabled        bool            `json:"enabled"`
	PausedAt       *time.Time      `json:"paused_at,omitempty"`
	PausedUntil    *time.Time      `json:"paused_until,omitempty"`
	Config         CheckConfig     `json:"config"`
	NextRunAt      time.Time       `json:"next_run_at"`
	ConsecutiveOK  int             `json:"consecutive_ok"`
	ConsecutiveBad int             `json:"consecutive_bad"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
}

type CheckExecution struct {
	ID          UUID          `json:"id"`
	CheckID     UUID          `json:"check_id"`
	ResourceID  UUID          `json:"resource_id"`
	Status      CheckStatus   `json:"status"`
	Latency     time.Duration `json:"latency"`
	StatusCode  int           `json:"status_code,omitempty"`
	Error       string        `json:"error,omitempty"`
	ObservedAt  time.Time     `json:"observed_at"`
	ScheduledAt time.Time     `json:"scheduled_at"`
	WorkerID    string        `json:"worker_id"`
}

type Incident struct {
	ID               UUID          `json:"id"`
	CheckID          UUID          `json:"check_id"`
	ResourceID       UUID          `json:"resource_id"`
	UserID           UUID          `json:"user_id"`
	State            IncidentState `json:"state"`
	Severity         string        `json:"severity"`
	OpenReason       string        `json:"open_reason"`
	OpenedAt         time.Time     `json:"opened_at"`
	ResolvedAt       *time.Time    `json:"resolved_at,omitempty"`
	LastTransitionAt time.Time     `json:"last_transition_at"`
	EscalationLevel  int           `json:"escalation_level"`
}

type MetricAggregate struct {
	CheckID    UUID      `json:"check_id"`
	Bucket     time.Time `json:"bucket"`
	UptimePct  float64   `json:"uptime_pct"`
	LatencyP50 float64   `json:"latency_p50_ms"`
	LatencyP95 float64   `json:"latency_p95_ms"`
	LatencyP99 float64   `json:"latency_p99_ms"`
	Total      int64     `json:"total"`
	Failures   int64     `json:"failures"`
}
