package app

import (
	"context"
	"fmt"
	"os"
	"time"

	"distributed-monitoring-system/internal/adapters"
	"distributed-monitoring-system/internal/observability"
	sharedtransport "distributed-monitoring-system/internal/transport"
	"distributed-monitoring-system/services/scheduler/config"
	"distributed-monitoring-system/services/scheduler/internal/usecase"

	"net/http"

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

	pg, err := adapters.NewPostgresPool(ctx, adapters.PostgresConfig{
		DSN:      cfg.DatabaseURL,
		MaxConns: int32(cfg.PostgresMaxConns),
	})
	if err != nil {
		return err
	}
	defer pg.Close()

	redisClient, err := adapters.NewRedisClient(ctx, adapters.RedisConfig{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB})
	if err != nil {
		return err
	}
	defer func() { _ = redisClient.Close() }()

	m := observability.NewMetrics("scheduler")
	producer := adapters.NewProducer(cfg.KafkaBrokers, logger)
	defer func() { _ = producer.Close() }()

	repo := usecase.NewPostgresChecksRepository(pg)
	service := usecase.NewScheduler(usecase.SchedulerConfig{
		ServiceName: cfg.ServiceName,
		Tick:        cfg.TickInterval,
		LeaseTTL:    cfg.LeaseTTL,
		BatchSize:   cfg.BatchSize,
		InstanceID:  cfg.ServiceName + "-" + os.Getenv("HOSTNAME"),
	}, repo, producer, redisClient, m, logger)

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	metricsServer := sharedtransport.NewHTTPServer(cfg.MetricsAddr, mux, logger)

	errCh := make(chan error, 2)
	go func() { errCh <- metricsServer.Run(ctx) }()
	go func() { errCh <- service.Run(ctx) }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("scheduler app: %w", err)
		}
		return nil
	}
}
