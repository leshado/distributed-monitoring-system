package usecase

import (
	"context"
	"fmt"
	"time"

	"distributed-monitoring-system/internal/adapters"
	"distributed-monitoring-system/internal/domain"
	"distributed-monitoring-system/internal/events"
	"distributed-monitoring-system/internal/observability"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type Service struct {
	repo       ChecksRepository
	producer   *adapters.Producer
	redis      *redis.Client
	metrics    *observability.Metrics
	logger     *zap.Logger
	service    string
	tick       time.Duration
	leaseTTL   time.Duration
	batchSize  int
	instanceID string
}

type SchedulerConfig struct {
	ServiceName string
	Tick        time.Duration
	LeaseTTL    time.Duration
	BatchSize   int
	InstanceID  string
}

func NewScheduler(
	cfg SchedulerConfig,
	repo ChecksRepository,
	producer *adapters.Producer,
	redis *redis.Client,
	m *observability.Metrics,
	logger *zap.Logger,
) *Service {
	return &Service{
		repo: repo, producer: producer, redis: redis, metrics: m, logger: logger,
		service: cfg.ServiceName, tick: cfg.Tick, leaseTTL: cfg.LeaseTTL, batchSize: cfg.BatchSize,
		instanceID: cfg.InstanceID,
	}
}

func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()
	if err := s.tickOnce(ctx); err != nil {
		s.logger.Warn("initial scheduler tick failed", zap.Error(err))
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.tickOnce(ctx); err != nil {
				s.logger.Error("scheduler tick failed", zap.Error(err))
			}
		}
	}
}

func (s *Service) tickOnce(ctx context.Context) error {
	ctx, span := otel.Tracer(s.service).Start(ctx, "scheduler.tick")
	defer span.End()

	token := fmt.Sprintf("%s:%d", s.instanceID, time.Now().UnixNano())
	lock, ok, err := adapters.TryLock(ctx, s.redis, "locks:scheduler:tick", token, adapters.LockOptions{TTL: s.leaseTTL, RenewInterval: s.leaseTTL / 3, StartRenewal: true, ReleaseOnCancel: true})
	if err != nil {
		return fmt.Errorf("acquire scheduler lease: %w", err)
	}
	if !ok {
		s.logger.Debug("scheduler lease held by another instance")
		return nil
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := lock.Release(releaseCtx); err != nil {
			s.logger.Warn("release scheduler lease failed", zap.Error(err))
		}
	}()

	now := time.Now().UTC()
	checks, err := s.repo.ClaimDue(ctx, now, s.batchSize)
	if err != nil {
		return err
	}
	for _, check := range checks {
		if err := s.publish(ctx, check, now); err != nil {
			return err
		}
		s.metrics.ChecksScheduled.Inc()
	}
	if len(checks) > 0 {
		s.logger.Info("scheduled due checks", zap.Int("count", len(checks)))
	}
	return nil
}

func (s *Service) publish(ctx context.Context, check domain.Check, now time.Time) error {
	event := events.CheckScheduled{
		Envelope:    events.NewEnvelope("checks.scheduled", s.service, now),
		CheckID:     check.ID,
		ResourceID:  check.ResourceID,
		UserID:      check.UserID,
		CheckType:   check.Type,
		ScheduledAt: now,
		DueAt:       check.NextRunAt,
		Attempt:     1,
	}
	return s.producer.Publish(ctx, events.TopicChecksScheduled, []byte(check.ID), event)
}
