package app

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"distributed-monitoring-system/internal/adapters"
	"distributed-monitoring-system/internal/domain"
	"distributed-monitoring-system/internal/events"
	"distributed-monitoring-system/internal/observability"
	"distributed-monitoring-system/internal/runtime"
	sharedtransport "distributed-monitoring-system/internal/transport"
	"distributed-monitoring-system/services/alert-service/config"
	"distributed-monitoring-system/services/alert-service/internal/usecase"

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

	m := observability.NewMetrics("alert_service")
	kafkaMetrics := adapters.DefaultKafkaMetrics(m.Registry(), "alert_service")
	dlqProducer := adapters.NewProducer(cfg.KafkaBrokers, logger)
	defer func() { _ = dlqProducer.Close() }()
	repo := usecase.NewPostgresRepository(pg)
	telegramSender := usecase.NewTelegramSender(adapters.NewTelegramClient(cfg.TelegramBotToken), redisClient, 20)
	processor := &processor{cfg: cfg, repo: repo, sender: telegramSender, redis: redisClient, metrics: m, logger: logger}

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	metricsServer := sharedtransport.NewHTTPServer(cfg.MetricsAddr, mux, logger)

	errCh := make(chan error, cfg.Concurrency+1)
	consumers := make([]*adapters.Consumer, 0, cfg.Concurrency)
	for i := 0; i < cfg.Concurrency; i++ {
		consumer := adapters.NewConsumerWithOptions(cfg.KafkaBrokers, events.TopicAlertsTriggered, cfg.ConsumerGroup, logger, adapters.ConsumerOptions{
			ServiceName: cfg.ServiceName,
			Retry: adapters.RetryConfig{
				MaxAttempts: cfg.RetryAttempts,
				MinDelay:    cfg.RetryMinDelay,
				MaxDelay:    cfg.RetryMaxDelay,
				Jitter:      0.2,
			},
			Producer:   dlqProducer,
			DLQEnabled: true,
			Metrics:    kafkaMetrics,
		})
		consumers = append(consumers, consumer)
		go func(c *adapters.Consumer) { errCh <- c.Run(ctx, processor.Handle) }(consumer)
	}
	defer func() {
		for _, consumer := range consumers {
			_ = consumer.Close()
		}
	}()
	go func() { errCh <- metricsServer.Run(ctx) }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("alert service app: %w", err)
		}
		return nil
	}
}

type processor struct {
	cfg     config.Config
	repo    usecase.Repository
	sender  usecase.Sender
	redis   *redis.Client
	metrics *observability.Metrics
	logger  *zap.Logger
}

func (p *processor) Handle(ctx context.Context, msg adapters.Message) error {
	event, err := adapters.Decode[events.AlertTriggered](msg)
	if err != nil {
		return fmt.Errorf("decode alert event: %w", err)
	}
	deliveredKey := "alert:delivered:" + event.DedupKey
	delivered, err := p.redis.Exists(ctx, deliveredKey).Result()
	if err != nil {
		return fmt.Errorf("check delivered dedup: %w", err)
	}
	if delivered > 0 {
		return nil
	}
	lockKey := "alert:processing:" + event.DedupKey
	token := p.cfg.ServiceName + ":" + event.EventID
	lock, ok, err := adapters.TryLock(ctx, p.redis, lockKey, token, adapters.LockOptions{TTL: 5 * time.Minute, RenewInterval: 1 * time.Minute, StartRenewal: true, ReleaseOnCancel: true})
	if err != nil {
		return fmt.Errorf("acquire alert lock: %w", err)
	}
	if !ok {
		return nil
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := lock.Release(releaseCtx); err != nil {
			p.logger.Warn("release alert lock failed", zap.Error(err))
		}
	}()

	if err := p.repo.UpsertTriggered(ctx, event); err != nil {
		return err
	}
	user, err := p.repo.GetUser(ctx, event.UserID)
	if err != nil {
		_ = p.repo.MarkFailed(ctx, event.DedupKey, err.Error())
		return err
	}

	if event.Kind != domain.AlertKindIncidentResolved {
		if user.SilencedUntil != nil && time.Now().UTC().Before(*user.SilencedUntil) {
			p.logger.Info("alert silenced", zap.String("user_id", string(user.ID)), zap.String("dedup_key", event.DedupKey))
			return nil
		}

		throttleKey := fmt.Sprintf("alert:throttle:%s:%s", user.ID, event.IncidentID)
		lastAlert, err := p.redis.Get(ctx, throttleKey).Result()
		if err == nil && lastAlert != "" {
			p.logger.Info("alert throttled", zap.String("user_id", string(user.ID)), zap.String("incident_id", string(event.IncidentID)))
			return nil
		}
		alertInterval := user.AlertIntervalMinutes
		if alertInterval < 1 {
			alertInterval = 30
		}
		if err := p.redis.Set(ctx, throttleKey, "1", time.Duration(alertInterval)*time.Minute).Err(); err != nil {
			p.logger.Warn("failed to set throttle key", zap.Error(err))
		}
	}

	alertCtx, err := p.repo.GetAlertContext(ctx, event)
	if err != nil {
		_ = p.repo.MarkFailed(ctx, event.DedupKey, err.Error())
		return err
	}
	message := usecase.FormatTelegramMessage(event, alertCtx)
	sendErr := runtime.Retry(ctx, runtime.RetryConfig{
		Attempts: p.cfg.RetryAttempts,
		MinDelay: p.cfg.RetryMinDelay,
		MaxDelay: p.cfg.RetryMaxDelay,
	}, func(ctx context.Context) error {
		return p.sender.Send(ctx, user.TelegramChatID, message)
	})
	if sendErr != nil {
		_ = p.repo.MarkFailed(ctx, event.DedupKey, sendErr.Error())
		p.metrics.AlertsDelivered.WithLabelValues("failed").Inc()
		return sendErr
	}
	if err := p.repo.MarkDelivered(ctx, event.DedupKey, time.Now().UTC()); err != nil {
		return err
	}
	if err := p.redis.Set(ctx, deliveredKey, "1", p.cfg.DedupTTL).Err(); err != nil {
		return fmt.Errorf("set alert delivered dedup: %w", err)
	}
	p.metrics.AlertsTriggered.WithLabelValues(string(event.Kind)).Inc()
	p.metrics.AlertsDelivered.WithLabelValues("delivered").Inc()
	return nil
}
