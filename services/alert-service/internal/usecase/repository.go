package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"distributed-monitoring-system/internal/domain"
	"distributed-monitoring-system/internal/events"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	GetTelegramChat(ctx context.Context, userID domain.UUID) (int64, error)
	GetUser(ctx context.Context, userID domain.UUID) (domain.User, error)
	GetAlertContext(ctx context.Context, event events.AlertTriggered) (AlertContext, error)
	UpsertTriggered(ctx context.Context, event events.AlertTriggered) error
	MarkDelivered(ctx context.Context, dedupKey string, deliveredAt time.Time) error
	MarkFailed(ctx context.Context, dedupKey string, reason string) error
}

type PostgresRepository struct {
	db *pgxpool.Pool
}

type AlertContext struct {
	User     domain.User
	Check    domain.Check
	Incident domain.Incident
}

func NewPostgresRepository(db *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) GetTelegramChat(ctx context.Context, userID domain.UUID) (int64, error) {
	var chatID int64
	err := r.db.QueryRow(ctx, `SELECT telegram_chat_id FROM users WHERE id = $1`, userID).Scan(&chatID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("user %s not found", userID)
		}
		return 0, fmt.Errorf("select telegram chat: %w", err)
	}
	return chatID, nil
}

func (r *PostgresRepository) UpsertTriggered(ctx context.Context, event events.AlertTriggered) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO alerts
			(id, user_id, incident_id, check_id, resource_id, kind, severity, message, dedup_key, status, triggered_at, attempts)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'triggered', $10, 0)
		ON CONFLICT (dedup_key) DO UPDATE
		SET attempts = alerts.attempts + 1,
		    status = 'retrying',
		    last_error = NULL,
		    updated_at = now()`,
		event.AlertID, event.UserID, event.IncidentID, event.CheckID, event.ResourceID,
		event.Kind, event.Severity, event.Message, event.DedupKey, event.TriggeredAt)
	if err != nil {
		return fmt.Errorf("upsert alert: %w", err)
	}
	return nil
}

func (r *PostgresRepository) MarkDelivered(ctx context.Context, dedupKey string, deliveredAt time.Time) error {
	_, err := r.db.Exec(ctx, `
		UPDATE alerts
		SET status = 'delivered', delivered_at = $2, updated_at = now()
		WHERE dedup_key = $1`, dedupKey, deliveredAt)
	if err != nil {
		return fmt.Errorf("mark alert delivered: %w", err)
	}
	return nil
}

func (r *PostgresRepository) MarkFailed(ctx context.Context, dedupKey string, reason string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE alerts
		SET status = 'failed', last_error = $2, updated_at = now()
		WHERE dedup_key = $1`, dedupKey, reason)
	if err != nil {
		return fmt.Errorf("mark alert failed: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetUser(ctx context.Context, userID domain.UUID) (domain.User, error) {
	var user domain.User
	err := r.db.QueryRow(ctx, `
		SELECT id, telegram_chat_id, username, alert_interval_minutes, silenced_until, timezone, created_at
		FROM users WHERE id = $1`, userID).Scan(
		&user.ID, &user.TelegramChatID, &user.Username, &user.AlertIntervalMinutes, &user.SilencedUntil, &user.Timezone, &user.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.User{}, fmt.Errorf("user %s not found", userID)
		}
		return domain.User{}, fmt.Errorf("select user: %w", err)
	}
	return user, nil
}

func (r *PostgresRepository) GetAlertContext(ctx context.Context, event events.AlertTriggered) (AlertContext, error) {
	var out AlertContext
	var checkType string
	var intervalSec int64
	var timeoutMillis int64
	var jitterMillis int64
	var configPayload []byte
	var state string
	err := r.db.QueryRow(ctx, `
		SELECT
			u.id, u.telegram_chat_id, u.username, u.alert_interval_minutes, u.silenced_until, u.timezone, u.created_at,
			c.id, c.resource_id, c.user_id, c.type, c.name, c.interval_seconds, c.timeout_millis,
			c.jitter_millis, c.enabled, c.paused_at, c.paused_until, c.config, c.next_run_at, c.consecutive_ok, c.consecutive_bad,
			c.created_at, c.updated_at,
			i.id, i.check_id, i.resource_id, i.user_id, i.state, i.severity, i.open_reason,
			i.opened_at, i.resolved_at, i.last_transition_at, i.escalation_level
		FROM users u
		JOIN checks c ON c.id = $2 AND c.user_id = u.id
		JOIN incidents i ON i.id = $3
		WHERE u.id = $1`, event.UserID, event.CheckID, event.IncidentID).Scan(
		&out.User.ID, &out.User.TelegramChatID, &out.User.Username, &out.User.AlertIntervalMinutes, &out.User.SilencedUntil, &out.User.Timezone, &out.User.CreatedAt,
		&out.Check.ID, &out.Check.ResourceID, &out.Check.UserID, &checkType, &out.Check.Name, &intervalSec, &timeoutMillis,
		&jitterMillis, &out.Check.Enabled, &out.Check.PausedAt, &out.Check.PausedUntil, &configPayload, &out.Check.NextRunAt, &out.Check.ConsecutiveOK, &out.Check.ConsecutiveBad,
		&out.Check.CreatedAt, &out.Check.UpdatedAt,
		&out.Incident.ID, &out.Incident.CheckID, &out.Incident.ResourceID, &out.Incident.UserID, &state, &out.Incident.Severity, &out.Incident.OpenReason,
		&out.Incident.OpenedAt, &out.Incident.ResolvedAt, &out.Incident.LastTransitionAt, &out.Incident.EscalationLevel,
	)
	if err != nil {
		return AlertContext{}, fmt.Errorf("select alert context: %w", err)
	}
	out.Check.Type = domain.CheckType(checkType)
	out.Check.Interval = time.Duration(intervalSec) * time.Second
	out.Check.Timeout = time.Duration(timeoutMillis) * time.Millisecond
	out.Check.Jitter = time.Duration(jitterMillis) * time.Millisecond
	out.Incident.State = domain.IncidentState(state)
	if err := json.Unmarshal(configPayload, &out.Check.Config); err != nil {
		return AlertContext{}, fmt.Errorf("decode check config: %w", err)
	}
	return out, nil
}
