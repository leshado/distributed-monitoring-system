package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"distributed-monitoring-system/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ChecksRepository interface {
	ClaimDue(ctx context.Context, now time.Time, limit int) ([]domain.Check, error)
}

type PostgresChecksRepository struct {
	db *pgxpool.Pool
}

func NewPostgresChecksRepository(db *pgxpool.Pool) *PostgresChecksRepository {
	return &PostgresChecksRepository{db: db}
}

func (r *PostgresChecksRepository) ClaimDue(ctx context.Context, now time.Time, limit int) ([]domain.Check, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin claim tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	rows, err := tx.Query(ctx, `
		SELECT id, resource_id, user_id, type, name, interval_seconds, timeout_millis,
		       jitter_millis, enabled, paused_at, paused_until, config, next_run_at, consecutive_ok, consecutive_bad,
		       created_at, updated_at
		FROM checks
		WHERE enabled = true
		  AND (paused_until IS NULL OR paused_until <= $1)
		  AND next_run_at <= $1
		ORDER BY next_run_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("select due checks: %w", err)
	}
	defer rows.Close()

	checks := make([]domain.Check, 0, limit)
	for rows.Next() {
		check, err := scanCheck(rows)
		if err != nil {
			return nil, err
		}
		checks = append(checks, check)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due checks: %w", err)
	}

	for _, check := range checks {
		nextRun := nextRunAt(now, check.Interval, check.Jitter)
		if _, err = tx.Exec(ctx, `UPDATE checks SET next_run_at = $2, updated_at = now() WHERE id = $1`, check.ID, nextRun); err != nil {
			return nil, fmt.Errorf("update next_run_at for %s: %w", check.ID, err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim tx: %w", err)
	}
	return checks, nil
}

func nextRunAt(base time.Time, interval time.Duration, jitter time.Duration) time.Time {
	if jitter <= 0 {
		return base.Add(interval)
	}
	return base.Add(interval + time.Duration(rand.Int64N(int64(jitter))))
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
