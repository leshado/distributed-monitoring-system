package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"distributed-monitoring-system/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	GetCheck(ctx context.Context, id domain.UUID) (domain.Check, error)
	SaveExecution(ctx context.Context, execution domain.CheckExecution) error
}

type PostgresRepository struct {
	db *pgxpool.Pool
}

func NewPostgresRepository(db *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) GetCheck(ctx context.Context, id domain.UUID) (domain.Check, error) {
	row := r.db.QueryRow(ctx, `
		SELECT id, resource_id, user_id, type, name, interval_seconds, timeout_millis,
		       jitter_millis, enabled, paused_at, paused_until, config, next_run_at, consecutive_ok, consecutive_bad,
		       created_at, updated_at
		FROM checks
		WHERE id = $1`, id)
	check, err := scanCheck(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Check{}, fmt.Errorf("check %s not found", id)
		}
		return domain.Check{}, err
	}
	return check, nil
}

func (r *PostgresRepository) SaveExecution(ctx context.Context, execution domain.CheckExecution) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin execution tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	_, err = tx.Exec(ctx, `
		INSERT INTO check_executions
			(id, check_id, resource_id, status, latency_ms, status_code, error, observed_at, scheduled_at, worker_id)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9, $10)`,
		execution.ID, execution.CheckID, execution.ResourceID, execution.Status,
		execution.Latency.Milliseconds(), execution.StatusCode, execution.Error,
		execution.ObservedAt, execution.ScheduledAt, execution.WorkerID)
	if err != nil {
		return fmt.Errorf("insert check execution: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit execution tx: %w", err)
	}
	return nil
}

type checkScanner interface {
	Scan(dest ...any) error
}

func scanCheck(row checkScanner) (domain.Check, error) {
	var (
		check         domain.Check
		checkType     string
		intervalSec   int64
		timeoutMillis int64
		jitterMillis  int64
		configPayload []byte
	)
	if err := row.Scan(
		&check.ID,
		&check.ResourceID,
		&check.UserID,
		&checkType,
		&check.Name,
		&intervalSec,
		&timeoutMillis,
		&jitterMillis,
		&check.Enabled,
		&check.PausedAt,
		&check.PausedUntil,
		&configPayload,
		&check.NextRunAt,
		&check.ConsecutiveOK,
		&check.ConsecutiveBad,
		&check.CreatedAt,
		&check.UpdatedAt,
	); err != nil {
		return domain.Check{}, fmt.Errorf("scan check: %w", err)
	}
	check.Type = domain.CheckType(checkType)
	check.Interval = time.Duration(intervalSec) * time.Second
	check.Timeout = time.Duration(timeoutMillis) * time.Millisecond
	check.Jitter = time.Duration(jitterMillis) * time.Millisecond
	if err := json.Unmarshal(configPayload, &check.Config); err != nil {
		return domain.Check{}, fmt.Errorf("decode check config for %s: %w", check.ID, err)
	}
	return check, nil
}
