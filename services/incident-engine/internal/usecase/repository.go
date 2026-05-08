package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"distributed-monitoring-system/internal/domain"
	"distributed-monitoring-system/internal/events"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	ApplyCheckResult(ctx context.Context, event events.CheckExecuted) (consecutiveOK int, consecutiveBad int, err error)
	GetOpenIncident(ctx context.Context, checkID domain.UUID) (*domain.Incident, error)
	OpenIncident(ctx context.Context, incident domain.Incident) (domain.UUID, bool, error)
	UpdateState(ctx context.Context, incidentID domain.UUID, state domain.IncidentState, at time.Time, note string) error
	ResolveIncident(ctx context.Context, incidentID domain.UUID, at time.Time, reason string) error
	Escalate(ctx context.Context, incidentID domain.UUID, level int, at time.Time) error
}

type PostgresRepository struct {
	db *pgxpool.Pool
}

func NewPostgresRepository(db *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) ApplyCheckResult(ctx context.Context, event events.CheckExecuted) (int, int, error) {
	var okCount, badCount int
	err := r.db.QueryRow(ctx, `
		UPDATE checks
		SET
			consecutive_ok = CASE WHEN $2 = 'ok' THEN consecutive_ok + 1 ELSE 0 END,
			consecutive_bad = CASE WHEN $2 = 'fail' THEN consecutive_bad + 1 ELSE 0 END,
			updated_at = now()
		WHERE id = $1
		RETURNING consecutive_ok, consecutive_bad`, event.CheckID, string(event.Status)).Scan(&okCount, &badCount)
	if err != nil {
		return 0, 0, fmt.Errorf("apply check counters: %w", err)
	}
	return okCount, badCount, nil
}

func (r *PostgresRepository) GetOpenIncident(ctx context.Context, checkID domain.UUID) (*domain.Incident, error) {
	row := r.db.QueryRow(ctx, `
		SELECT id, check_id, resource_id, user_id, state, severity, open_reason, opened_at,
		       resolved_at, last_transition_at, escalation_level
		FROM incidents
		WHERE check_id = $1 AND resolved_at IS NULL
		ORDER BY opened_at DESC
		LIMIT 1`, checkID)
	incident, err := scanIncident(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &incident, nil
}

func (r *PostgresRepository) OpenIncident(ctx context.Context, incident domain.Incident) (domain.UUID, bool, error) {
	var openedID domain.UUID
	var inserted bool
	row := r.db.QueryRow(ctx, `
		WITH upsert AS (
			INSERT INTO incidents
				(id, check_id, resource_id, user_id, state, severity, open_reason, opened_at, last_transition_at, escalation_level)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (check_id) WHERE resolved_at IS NULL
			DO UPDATE SET
				state = EXCLUDED.state,
				severity = EXCLUDED.severity,
				open_reason = CASE WHEN incidents.open_reason = '' THEN EXCLUDED.open_reason ELSE incidents.open_reason END,
				last_transition_at = GREATEST(incidents.last_transition_at, EXCLUDED.last_transition_at),
				escalation_level = GREATEST(incidents.escalation_level, EXCLUDED.escalation_level),
				updated_at = now()
			RETURNING id, (xmax = 0) AS inserted
		)
		SELECT id, inserted FROM upsert`,
		incident.ID, incident.CheckID, incident.ResourceID, incident.UserID, incident.State,
		incident.Severity, incident.OpenReason, incident.OpenedAt, incident.LastTransitionAt, incident.EscalationLevel,
	)
	if err := row.Scan(&openedID, &inserted); err != nil {
		return "", false, fmt.Errorf("upsert incident: %w", err)
	}
	if inserted {
		if err := r.insertIncidentEvent(ctx, openedID, "opened", string(incident.State), incident.OpenReason, incident.OpenedAt); err != nil {
			return "", false, err
		}
	}
	return openedID, inserted, nil
}

func (r *PostgresRepository) UpdateState(ctx context.Context, incidentID domain.UUID, state domain.IncidentState, at time.Time, note string) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin state tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	ct, err := tx.Exec(ctx, `
		UPDATE incidents
		SET state = $2, last_transition_at = $3, updated_at = now()
		WHERE id = $1
			AND resolved_at IS NULL
			AND state <> $2`, incidentID, state, at)
	if err != nil {
		return fmt.Errorf("update incident state: %w", err)
	}
	if ct.RowsAffected() == 0 {
		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit state tx: %w", err)
		}
		return nil
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO incident_events (id, incident_id, event_type, state, note, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		uuid.NewString(), incidentID, "state_changed", state, note, at); err != nil {
		return fmt.Errorf("insert incident event: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit state tx: %w", err)
	}
	return nil
}

func (r *PostgresRepository) ResolveIncident(ctx context.Context, incidentID domain.UUID, at time.Time, reason string) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin resolve tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	ct, err := tx.Exec(ctx, `
		UPDATE incidents
		SET state = $2, resolved_at = $3, last_transition_at = $3, updated_at = now()
		WHERE id = $1 AND resolved_at IS NULL`, incidentID, domain.IncidentStateOK, at)
	if err != nil {
		return fmt.Errorf("resolve incident: %w", err)
	}
	if ct.RowsAffected() == 0 {
		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit resolve tx: %w", err)
		}
		return nil
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO incident_events (id, incident_id, event_type, state, note, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		uuid.NewString(), incidentID, "resolved", domain.IncidentStateOK, reason, at); err != nil {
		return fmt.Errorf("insert resolve event: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit resolve tx: %w", err)
	}
	return nil
}

func (r *PostgresRepository) Escalate(ctx context.Context, incidentID domain.UUID, level int, at time.Time) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin escalation tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	ct, err := tx.Exec(ctx, `
		UPDATE incidents
		SET escalation_level = $2, last_transition_at = $3, updated_at = now()
		WHERE id = $1
			AND resolved_at IS NULL
			AND escalation_level < $2`, incidentID, level, at)
	if err != nil {
		return fmt.Errorf("escalate incident: %w", err)
	}
	if ct.RowsAffected() == 0 {
		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit escalation tx: %w", err)
		}
		return nil
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO incident_events (id, incident_id, event_type, state, note, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		uuid.NewString(), incidentID, "escalated", "", fmt.Sprintf("level %d", level), at); err != nil {
		return fmt.Errorf("insert escalation event: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit escalation tx: %w", err)
	}
	return nil
}

func (r *PostgresRepository) insertIncidentEvent(ctx context.Context, incidentID domain.UUID, eventType, state, note string, at time.Time) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO incident_events (id, incident_id, event_type, state, note, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6)`, uuid.NewString(), incidentID, eventType, state, note, at)
	if err != nil {
		return fmt.Errorf("insert incident event: %w", err)
	}
	return nil
}

type incidentScanner interface {
	Scan(dest ...any) error
}

func scanIncident(row incidentScanner) (domain.Incident, error) {
	var incident domain.Incident
	var state string
	if err := row.Scan(
		&incident.ID,
		&incident.CheckID,
		&incident.ResourceID,
		&incident.UserID,
		&state,
		&incident.Severity,
		&incident.OpenReason,
		&incident.OpenedAt,
		&incident.ResolvedAt,
		&incident.LastTransitionAt,
		&incident.EscalationLevel,
	); err != nil {
		return domain.Incident{}, fmt.Errorf("scan incident: %w", err)
	}
	incident.State = domain.IncidentState(state)
	return incident, nil
}
