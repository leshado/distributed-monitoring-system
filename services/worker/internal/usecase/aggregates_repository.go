package usecase

import (
	"context"
	"fmt"
	"time"

	"distributed-monitoring-system/internal/domain"

	"github.com/jackc/pgx/v5/pgxpool"
)

type AggregatedMetrics struct {
	CheckID      domain.UUID
	BucketStart  time.Time
	BucketDur    time.Duration
	UptimePct    float64
	P50Ms        float64
	P95Ms        float64
	P99Ms        float64
	TotalCount   int64
	FailureCount int64
}

type AggregatorRepository interface {
	UpsertAggregates(ctx context.Context, rows []AggregatedMetrics) error
}

type PostgresAggregatorRepository struct {
	db *pgxpool.Pool
}

func NewPostgresAggregatorRepository(db *pgxpool.Pool) *PostgresAggregatorRepository {
	return &PostgresAggregatorRepository{db: db}
}

func (r *PostgresAggregatorRepository) UpsertAggregates(ctx context.Context, rows []AggregatedMetrics) error {
	if len(rows) == 0 {
		return nil
	}

	checkIDs := make([]domain.UUID, 0, len(rows))
	bucketStarts := make([]time.Time, 0, len(rows))
	bucketDurs := make([]time.Duration, 0, len(rows))
	uptime := make([]float64, 0, len(rows))
	p50 := make([]float64, 0, len(rows))
	p95 := make([]float64, 0, len(rows))
	p99 := make([]float64, 0, len(rows))
	total := make([]int64, 0, len(rows))
	fail := make([]int64, 0, len(rows))

	for _, r := range rows {
		checkIDs = append(checkIDs, r.CheckID)
		bucketStarts = append(bucketStarts, r.BucketStart)
		bucketDurs = append(bucketDurs, r.BucketDur)
		uptime = append(uptime, r.UptimePct)
		p50 = append(p50, r.P50Ms)
		p95 = append(p95, r.P95Ms)
		p99 = append(p99, r.P99Ms)
		total = append(total, r.TotalCount)
		fail = append(fail, r.FailureCount)
	}

	_, err := r.db.Exec(ctx, `
		INSERT INTO metrics_aggregated (
			check_id, bucket_start, bucket_duration,
			uptime_pct, latency_p50_ms, latency_p95_ms, latency_p99_ms,
			total_count, failure_count, updated_at
		)
		SELECT
			u.check_id,
			u.bucket_start,
			u.bucket_duration,
			u.uptime_pct,
			u.p50,
			u.p95,
			u.p99,
			u.total_count,
			u.failure_count,
			now()
		FROM unnest(
			$1::uuid[],
			$2::timestamptz[],
			$3::interval[],
			$4::double precision[],
			$5::double precision[],
			$6::double precision[],
			$7::double precision[],
			$8::bigint[],
			$9::bigint[]
		) AS u(
			check_id, bucket_start, bucket_duration,
			uptime_pct, p50, p95, p99,
			total_count, failure_count
		)
		ON CONFLICT (check_id, bucket_start, bucket_duration)
		do update set
			uptime_pct =
				CASE
					WHEN metrics_aggregated.total_count + EXCLUDED.total_count = 0 THEN 100
					ELSE
						((metrics_aggregated.total_count + EXCLUDED.total_count)
						 - (metrics_aggregated.failure_count + EXCLUDED.failure_count))::double precision
						/ (metrics_aggregated.total_count + EXCLUDED.total_count)::double precision
						* 100
				END,
			latency_p50_ms = EXCLUDED.latency_p50_ms,
			latency_p95_ms = EXCLUDED.latency_p95_ms,
			latency_p99_ms = EXCLUDED.latency_p99_ms,
			total_count = metrics_aggregated.total_count + EXCLUDED.total_count,
			failure_count = metrics_aggregated.failure_count + EXCLUDED.failure_count,
			updated_at = now()
	`, checkIDs, bucketStarts, bucketDurs, uptime, p50, p95, p99, total, fail)
	if err != nil {
		return fmt.Errorf("upsert aggregates: %w", err)
	}
	return nil
}
