package app

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"distributed-monitoring-system/internal/adapters"
	"distributed-monitoring-system/internal/events"
	"distributed-monitoring-system/internal/observability"
	sharedtransport "distributed-monitoring-system/internal/transport"
	"distributed-monitoring-system/services/incident-engine/config"
	"distributed-monitoring-system/services/incident-engine/internal/usecase"

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

	m := observability.NewMetrics("incident_engine")
	kafkaMetrics := adapters.DefaultKafkaMetrics(m.Registry(), "incident_engine")
	producer := adapters.NewProducer(cfg.KafkaBrokers, logger)
	defer func() { _ = producer.Close() }()

	repo := usecase.NewPostgresRepository(pg)
	incidentEngine := usecase.NewEngine(usecase.EngineConfig{
		ServiceName:       cfg.ServiceName,
		WindowDuration:    cfg.WindowDuration,
		FailureThreshold:  cfg.FailureThreshold,
		RecoveryThreshold: cfg.RecoveryThreshold,
		EscalationAfter:   cfg.EscalationAfter,
	}, repo, redisClient, producer, m, logger)

	consumer := adapters.NewConsumerWithOptions(cfg.KafkaBrokers, events.TopicChecksExecuted, cfg.ConsumerGroup, logger, adapters.ConsumerOptions{
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
	defer func() { _ = consumer.Close() }()

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	metricsServer := sharedtransport.NewHTTPServer(cfg.MetricsAddr, mux, logger)

	errCh := make(chan error, 2)
	go func() { errCh <- consumer.Run(ctx, incidentEngine.Handle) }()
	go func() { errCh <- metricsServer.Run(ctx) }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("incident engine app: %w", err)
		}
		return nil
	}
}
