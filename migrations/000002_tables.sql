-- +goose Up
CREATE TABLE IF NOT EXISTS users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    telegram_chat_id bigint NOT NULL UNIQUE,
    username text NOT NULL DEFAULT '',
    alert_interval_minutes integer NOT NULL DEFAULT 30 CHECK (alert_interval_minutes >= 1),
    silenced_until timestamptz,
    timezone text NOT NULL DEFAULT 'UTC',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS resources (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name text NOT NULL,
    target text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS checks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    resource_id uuid NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    type text NOT NULL CHECK (type IN ('http', 'tcp', 'tls')),
    name text NOT NULL,
    interval_seconds bigint NOT NULL CHECK (interval_seconds >= 10),
    timeout_millis bigint NOT NULL CHECK (timeout_millis > 0),
    jitter_millis bigint NOT NULL DEFAULT 0 CHECK (jitter_millis >= 0),
    enabled boolean NOT NULL DEFAULT true,
    paused_at timestamptz,
    paused_until timestamptz,
    config jsonb NOT NULL,
    next_run_at timestamptz NOT NULL DEFAULT now(),
    consecutive_ok integer NOT NULL DEFAULT 0,
    consecutive_bad integer NOT NULL DEFAULT 0,
    metadata jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS check_executions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    check_id uuid NOT NULL REFERENCES checks(id) ON DELETE CASCADE,
    resource_id uuid NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
    status text NOT NULL CHECK (status IN ('ok', 'fail')),
    latency_ms bigint NOT NULL CHECK (latency_ms >= 0),
    status_code integer NOT NULL DEFAULT 0,
    error text,
    observed_at timestamptz NOT NULL,
    scheduled_at timestamptz NOT NULL,
    worker_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS incidents (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    check_id uuid NOT NULL REFERENCES checks(id) ON DELETE CASCADE,
    resource_id uuid NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    state text NOT NULL CHECK (state IN ('OK', 'DEGRADED', 'DOWN', 'RECOVERING')),
    severity text NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    open_reason text NOT NULL,
    opened_at timestamptz NOT NULL,
    resolved_at timestamptz,
    last_transition_at timestamptz NOT NULL,
    escalation_level integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS incident_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id uuid NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    event_type text NOT NULL,
    state text NOT NULL DEFAULT '',
    note text NOT NULL DEFAULT '',
    occurred_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alerts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    incident_id uuid NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    check_id uuid NOT NULL REFERENCES checks(id) ON DELETE CASCADE,
    resource_id uuid NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
    kind text NOT NULL CHECK (kind IN ('incident_opened', 'incident_resolved', 'incident_escalation')),
    severity text NOT NULL,
    message text NOT NULL,
    dedup_key text NOT NULL UNIQUE,
    status text NOT NULL CHECK (status IN ('triggered', 'retrying', 'delivered', 'failed')),
    attempts integer NOT NULL DEFAULT 0,
    last_error text,
    triggered_at timestamptz NOT NULL,
    delivered_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS metrics_aggregated (
    check_id uuid NOT NULL REFERENCES checks(id) ON DELETE CASCADE,
    bucket_start timestamptz NOT NULL,
    bucket_duration interval NOT NULL,
    uptime_pct double precision NOT NULL CHECK (uptime_pct >= 0 AND uptime_pct <= 100),
    latency_p50_ms double precision NOT NULL DEFAULT 0,
    latency_p95_ms double precision NOT NULL DEFAULT 0,
    latency_p99_ms double precision NOT NULL DEFAULT 0,
    total_count bigint NOT NULL DEFAULT 0,
    failure_count bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (check_id, bucket_start, bucket_duration)
);

-- +goose Down
DROP TABLE IF EXISTS metrics_aggregated;
DROP TABLE IF EXISTS alerts;
DROP TABLE IF EXISTS incident_events;
DROP TABLE IF EXISTS incidents;
DROP TABLE IF EXISTS check_executions;
DROP TABLE IF EXISTS checks;
DROP TABLE IF EXISTS resources;
DROP TABLE IF EXISTS users;
