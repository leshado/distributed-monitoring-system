package app

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"distributed-monitoring-system/internal/adapters"
	"distributed-monitoring-system/internal/checkrunner"
	"distributed-monitoring-system/internal/domain"
	"distributed-monitoring-system/internal/events"
	"distributed-monitoring-system/internal/observability"
	"distributed-monitoring-system/internal/runtime"
	sharedtransport "distributed-monitoring-system/internal/transport"
	"distributed-monitoring-system/services/worker/config"
	"distributed-monitoring-system/services/worker/internal/usecase"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func Run(ctx context.Context, cfg config.Config) error {
	logger, err := observability.NewLogger(cfg.ServiceName, cfg.LogLevel)
	if err != nil {
		return err
	}
	defer func() { _ = logger.Sync() }()

	traceShutdown, err := observability.InitTracing(ctx, cfg.ServiceName, cfg.TraceSampleRatio)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := traceShutdown(shutdownCtx); err != nil {
			logger.Warn("trace shutdown failed", zap.Error(err))
		}
	}()

	pg, err := adapters.NewPostgresPool(ctx, adapters.PostgresConfig{DSN: cfg.DatabaseURL, MaxConns: int32(cfg.PostgresMaxConns)})
	if err != nil {
		return err
	}
	defer pg.Close()

	redisClient, err := adapters.NewRedisClient(ctx, adapters.RedisConfig{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB})
	if err != nil {
		return err
	}
	defer func() { _ = redisClient.Close() }()

	m := observability.NewMetrics("check_worker")
	kafkaMetrics := adapters.DefaultKafkaMetrics(m.Registry(), "check_worker")
	repo := usecase.NewPostgresRepository(pg)
	checkRunner := checkrunner.New()
	producer := adapters.NewProducer(cfg.KafkaBrokers, logger)
	defer func() { _ = producer.Close() }()

	processor := &processor{
		cfg: cfg, repo: repo, checker: checkRunner, producer: producer,
		redis: redisClient, metrics: m, logger: logger,
	}
	aggRepo := usecase.NewPostgresAggregatorRepository(pg)
	aggSvc := usecase.NewAggregator(usecase.AggregatorConfig{ServiceName: cfg.ServiceName}, aggRepo, logger)

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	metricsServer := sharedtransport.NewHTTPServer(cfg.MetricsAddr, mux, logger)

	errCh := make(chan error, cfg.Concurrency+3)
	consumers := make([]*adapters.Consumer, 0, cfg.Concurrency)
	for i := 0; i < cfg.Concurrency; i++ {
		consumer := adapters.NewConsumerWithOptions(cfg.KafkaBrokers, events.TopicChecksScheduled, cfg.ConsumerGroup, logger, adapters.ConsumerOptions{
			ServiceName: cfg.ServiceName,
			Retry: adapters.RetryConfig{
				MaxAttempts: 8,
				MinDelay:    200 * time.Millisecond,
				MaxDelay:    15 * time.Second,
				Jitter:      0.2,
			},
			Producer:   producer,
			DLQEnabled: true,
			Metrics:    kafkaMetrics,
		})
		consumers = append(consumers, consumer)
		go func() { errCh <- consumer.Run(ctx, processor.Handle) }()
	}
	aggConsumer := adapters.NewConsumerWithOptions(cfg.KafkaBrokers, events.TopicChecksExecuted, cfg.ConsumerGroup+"-agg", logger, adapters.ConsumerOptions{
		ServiceName: cfg.ServiceName,
		Retry: adapters.RetryConfig{
			MaxAttempts: 8,
			MinDelay:    200 * time.Millisecond,
			MaxDelay:    15 * time.Second,
			Jitter:      0.2,
		},
		Producer:   producer,
		DLQEnabled: true,
		Metrics:    kafkaMetrics,
	})
	consumers = append(consumers, aggConsumer)
	go func() { errCh <- aggConsumer.Run(ctx, aggSvc.Handle) }()
	defer func() {
		for _, consumer := range consumers {
			_ = consumer.Close()
		}
	}()
	go func() { errCh <- metricsServer.Run(ctx) }()
	go func() { errCh <- aggSvc.Run(ctx) }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("worker app: %w", err)
		}
		return nil
	}
}

type processor struct {
	cfg      config.Config
	repo     usecase.Repository
	checker  checkrunner.Checker
	producer *adapters.Producer
	redis    *redis.Client
	metrics  *observability.Metrics
	logger   *zap.Logger
}

func (p *processor) Handle(ctx context.Context, msg adapters.Message) error {
	event, err := adapters.Decode[events.CheckScheduled](msg)
	if err != nil {
		return fmt.Errorf("decode scheduled event: %w", err)
	}
	doneKey := "idempotency:check-worker:done:" + event.EventID
	exists, err := p.redis.Exists(ctx, doneKey).Result()
	if err != nil {
		return fmt.Errorf("check idempotency: %w", err)
	}
	if exists > 0 {
		return nil
	}
	lockKey := "idempotency:check-worker:processing:" + event.EventID
	token := p.cfg.WorkerID + ":" + uuid.NewString()
	lock, ok, err := adapters.TryLock(ctx, p.redis, lockKey, token, adapters.LockOptions{TTL: 10 * time.Minute, RenewInterval: 2 * time.Minute, StartRenewal: true, ReleaseOnCancel: true})
	if err != nil {
		return fmt.Errorf("acquire processing lock: %w", err)
	}
	if !ok {
		return nil
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := lock.Release(releaseCtx); err != nil {
			p.logger.Warn("release processing lock failed", zap.Error(err))
		}
	}()

	check, err := p.repo.GetCheck(ctx, event.CheckID)
	if err != nil {
		return err
	}
	if !check.Enabled {
		return p.markDone(ctx, doneKey)
	}

	result := p.checker.Run(ctx, check)
	execution := domain.CheckExecution{
		ID:          domain.UUID(uuid.NewString()),
		CheckID:     check.ID,
		ResourceID:  check.ResourceID,
		Status:      result.Status,
		Latency:     result.Latency,
		StatusCode:  result.StatusCode,
		Error:       result.Error,
		ObservedAt:  time.Now().UTC(),
		ScheduledAt: event.ScheduledAt,
		WorkerID:    p.cfg.WorkerID,
	}
	if err := runtime.Retry(ctx, runtime.RetryConfig{Attempts: 3, MinDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second}, func(ctx context.Context) error {
		return p.repo.SaveExecution(ctx, execution)
	}); err != nil {
		return err
	}

	executed := events.CheckExecuted{
		Envelope:    events.NewEnvelope("checks.executed", p.cfg.ServiceName, execution.ObservedAt),
		ExecutionID: execution.ID,
		CheckID:     check.ID,
		ResourceID:  check.ResourceID,
		UserID:      check.UserID,
		CheckType:   check.Type,
		Status:      result.Status,
		LatencyMs:   result.Latency.Milliseconds(),
		StatusCode:  result.StatusCode,
		Error:       result.Error,
		ObservedAt:  execution.ObservedAt,
		ScheduledAt: execution.ScheduledAt,
		WorkerID:    p.cfg.WorkerID,
	}
	if err := runtime.Retry(ctx, runtime.RetryConfig{Attempts: 3, MinDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second}, func(ctx context.Context) error {
		return p.producer.Publish(ctx, events.TopicChecksExecuted, []byte(check.ID), executed)
	}); err != nil {
		return err
	}
	if err := p.markDone(ctx, doneKey); err != nil {
		return err
	}
	p.metrics.ChecksExecuted.WithLabelValues(string(result.Status), string(check.Type)).Inc()
	p.metrics.CheckLatency.WithLabelValues(string(check.Type)).Observe(result.Latency.Seconds())
	return nil
}

func (p *processor) markDone(ctx context.Context, key string) error {
	return p.redis.Set(ctx, key, "1", 24*time.Hour).Err()
}
