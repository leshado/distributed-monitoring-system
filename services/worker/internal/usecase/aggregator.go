package usecase

import (
	"context"
	"fmt"
	"sort"
	"time"

	"distributed-monitoring-system/internal/adapters"
	"distributed-monitoring-system/internal/domain"
	"distributed-monitoring-system/internal/events"
	"distributed-monitoring-system/internal/runtime"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type AggregatorConfig struct {
	ServiceName      string
	AggregationDelay time.Duration
	FlushInterval    time.Duration
	MaxBatchSize     int
}

type Aggregator struct {
	cfg   AggregatorConfig
	repo  AggregatorRepository
	log   *zap.Logger
	buf   map[bucketKey]*bucketAgg
	outCh chan AggregatedMetrics
}

type bucketKey struct {
	CheckID     domain.UUID
	BucketStart time.Time
	BucketDur   time.Duration
}

type bucketAgg struct {
	checkID     domain.UUID
	bucketStart time.Time
	bucketDur   time.Duration
	latenciesMs []int64
	total       int64
	failures    int64
	lastSeenAt  time.Time
}

func NewAggregator(cfg AggregatorConfig, repo AggregatorRepository, logger *zap.Logger) *Aggregator {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 2 * time.Second
	}
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 200
	}
	if cfg.AggregationDelay <= 0 {
		cfg.AggregationDelay = 10 * time.Second
	}
	return &Aggregator{
		cfg:   cfg,
		repo:  repo,
		log:   logger,
		buf:   make(map[bucketKey]*bucketAgg, 1024),
		outCh: make(chan AggregatedMetrics, 1024),
	}
}

func (s *Aggregator) Handle(_ context.Context, msg adapters.Message) error {
	event, err := adapters.Decode[events.CheckExecuted](msg)
	if err != nil {
		return adapters.Permanent(fmt.Errorf("decode executed event: %w", err))
	}
	if event.Status != domain.CheckStatusOK && event.Status != domain.CheckStatusFail {
		return adapters.Permanent(fmt.Errorf("unknown status %q", event.Status))
	}

	bucketStart := event.ObservedAt.UTC().Truncate(time.Hour)
	k := bucketKey{CheckID: event.CheckID, BucketStart: bucketStart, BucketDur: time.Hour}
	ag := s.buf[k]
	if ag == nil {
		ag = &bucketAgg{
			checkID:     event.CheckID,
			bucketStart: bucketStart,
			bucketDur:   time.Hour,
			latenciesMs: make([]int64, 0, 256),
		}
		s.buf[k] = ag
	}
	ag.total++
	if event.Status == domain.CheckStatusFail {
		ag.failures++
	}
	if event.LatencyMs >= 0 {
		ag.latenciesMs = append(ag.latenciesMs, event.LatencyMs)
	}
	ag.lastSeenAt = time.Now().UTC()
	return nil
}

func (s *Aggregator) Run(ctx context.Context) error {
	ctx, span := otel.Tracer(s.cfg.ServiceName).Start(ctx, "worker.aggregator.run")
	defer span.End()

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = s.flush(ctx, true)
			return nil
		case <-ticker.C:
			if err := s.flush(ctx, false); err != nil {
				s.log.Error("aggregator flush failed", zap.Error(err))
			}
		}
	}
}

func (s *Aggregator) flush(ctx context.Context, force bool) error {
	now := time.Now().UTC()
	rows := make([]AggregatedMetrics, 0, s.cfg.MaxBatchSize)

	for k, ag := range s.buf {
		if !force {
			if now.Sub(ag.lastSeenAt) < s.cfg.AggregationDelay {
				continue
			}
		}

		rows = append(rows, s.compute(ag))
		delete(s.buf, k)
		if len(rows) >= s.cfg.MaxBatchSize {
			break
		}
	}

	if len(rows) == 0 {
		return nil
	}

	return runtime.Retry(ctx, runtime.RetryConfig{Attempts: 3, MinDelay: 200 * time.Millisecond, MaxDelay: 2 * time.Second}, func(ctx context.Context) error {
		return s.repo.UpsertAggregates(ctx, rows)
	})
}

func (s *Aggregator) compute(ag *bucketAgg) AggregatedMetrics {
	uptime := 100.0
	if ag.total > 0 {
		uptime = (float64(ag.total-ag.failures) / float64(ag.total)) * 100.0
	}

	p50, p95, p99 := percentileMs(ag.latenciesMs)
	return AggregatedMetrics{
		CheckID:      ag.checkID,
		BucketStart:  ag.bucketStart,
		BucketDur:    ag.bucketDur,
		UptimePct:    uptime,
		P50Ms:        p50,
		P95Ms:        p95,
		P99Ms:        p99,
		TotalCount:   ag.total,
		FailureCount: ag.failures,
	}
}

func percentileMs(vals []int64) (p50 float64, p95 float64, p99 float64) {
	if len(vals) == 0 {
		return 0, 0, 0
	}
	s := make([]int64, len(vals))
	copy(s, vals)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })

	pick := func(q float64) float64 {
		if len(s) == 0 {
			return 0
		}
		idx := int(float64(len(s)-1) * q)
		if idx < 0 {
			idx = 0
		}
		if idx >= len(s) {
			idx = len(s) - 1
		}
		return float64(s[idx])
	}

	return pick(0.50), pick(0.95), pick(0.99)
}
