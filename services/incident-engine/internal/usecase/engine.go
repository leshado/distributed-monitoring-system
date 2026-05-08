package usecase

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"distributed-monitoring-system/internal/adapters"
	"distributed-monitoring-system/internal/domain"
	"distributed-monitoring-system/internal/events"
	"distributed-monitoring-system/internal/observability"
	"distributed-monitoring-system/internal/runtime"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type EngineConfig struct {
	ServiceName       string
	WindowDuration    time.Duration
	FailureThreshold  int
	RecoveryThreshold int
	EscalationAfter   time.Duration
}

type Engine struct {
	cfg      EngineConfig
	repo     Repository
	redis    *redis.Client
	producer *adapters.Producer
	metrics  *observability.Metrics
	logger   *zap.Logger
}

type windowStats struct {
	Total    int
	Failures int
	Success  int
}

func NewEngine(cfg EngineConfig, repo Repository, redis *redis.Client, producer *adapters.Producer, m *observability.Metrics, logger *zap.Logger) *Engine {
	return &Engine{cfg: cfg, repo: repo, redis: redis, producer: producer, metrics: m, logger: logger}
}

func (e *Engine) Handle(ctx context.Context, msg adapters.Message) (err error) {
	event, err := adapters.Decode[events.CheckExecuted](msg)
	if err != nil {
		return adapters.Permanent(fmt.Errorf("decode executed event: %w", err))
	}
	ctx, span := otel.Tracer(e.cfg.ServiceName).Start(ctx, "incident.evaluate")
	defer span.End()

	stats, err := e.recordWindow(ctx, event)
	if err != nil {
		return err
	}
	consecutiveOK, consecutiveBad, err := e.repo.ApplyCheckResult(ctx, event)
	if err != nil {
		return err
	}
	open, err := e.repo.GetOpenIncident(ctx, event.CheckID)
	if err != nil {
		return err
	}

	switch event.Status {
	case domain.CheckStatusFail:
		return e.handleFailure(ctx, event, open, stats, consecutiveBad)
	case domain.CheckStatusOK:
		return e.handleSuccess(ctx, event, open, consecutiveOK)
	default:
		return adapters.Permanent(fmt.Errorf("unknown check status %q", event.Status))
	}
}

func (e *Engine) handleFailure(ctx context.Context, event events.CheckExecuted, open *domain.Incident, stats windowStats, consecutiveBad int) error {
	now := event.ObservedAt
	if open == nil {
		if consecutiveBad < e.cfg.FailureThreshold || stats.Failures < e.cfg.FailureThreshold {
			return nil
		}
		state := domain.IncidentStateDown
		severity := "critical"
		if stats.Total > 0 && float64(stats.Failures)/float64(stats.Total) < 0.75 {
			state = domain.IncidentStateDegraded
			severity = "warning"
		}
		incident := domain.Incident{
			ID:               domain.UUID(uuid.NewString()),
			CheckID:          event.CheckID,
			ResourceID:       event.ResourceID,
			UserID:           event.UserID,
			State:            state,
			Severity:         severity,
			OpenReason:       event.Error,
			OpenedAt:         now,
			LastTransitionAt: now,
			EscalationLevel:  0,
		}
		if incident.OpenReason == "" {
			incident.OpenReason = "check failed"
		}
		openedID, inserted, err := e.repo.OpenIncident(ctx, incident)
		if err != nil {
			return err
		}
		incident.ID = openedID
		if inserted {
			e.metrics.IncidentsOpened.Inc()
			if err := e.publishOpened(ctx, incident, now); err != nil {
				return err
			}
			return e.publishAlert(ctx, incident, domain.AlertKindIncidentOpened, fmt.Sprintf("Проверка %s не проходит. Причина: %s", incident.CheckID, incident.OpenReason), "opened", now)
		}
		return nil
	}

	if open.State == domain.IncidentStateRecovering {
		if err := e.repo.UpdateState(ctx, open.ID, domain.IncidentStateDegraded, now, "failure during recovery"); err != nil {
			return err
		}
		open.State = domain.IncidentStateDegraded
	}

	nextLevel := open.EscalationLevel + 1
	if e.cfg.EscalationAfter > 0 && now.Sub(open.OpenedAt) >= time.Duration(nextLevel)*e.cfg.EscalationAfter {
		if err := e.repo.Escalate(ctx, open.ID, nextLevel, now); err != nil {
			return err
		}
		open.EscalationLevel = nextLevel
		return e.publishAlert(ctx, *open, domain.AlertKindEscalation, fmt.Sprintf("Проблема по проверке %s всё ещё актуальна. Уровень эскалации: %d.", open.CheckID, nextLevel), fmt.Sprintf("escalation:%d", nextLevel), now)
	}
	return nil
}

func (e *Engine) handleSuccess(ctx context.Context, event events.CheckExecuted, open *domain.Incident, consecutiveOK int) error {
	if open == nil {
		return nil
	}
	now := event.ObservedAt
	if consecutiveOK >= e.cfg.RecoveryThreshold {
		if err := e.repo.ResolveIncident(ctx, open.ID, now, "recovery threshold reached"); err != nil {
			return err
		}
		e.metrics.IncidentsClosed.Inc()
		resolved := *open
		resolved.State = domain.IncidentStateOK
		resolved.ResolvedAt = &now
		if err := e.publishResolved(ctx, resolved, now); err != nil {
			return err
		}
		return e.publishAlert(ctx, resolved, domain.AlertKindIncidentResolved, fmt.Sprintf("Проверка %s снова проходит успешно.", open.CheckID), "resolved", now)
	}
	if open.State != domain.IncidentStateRecovering {
		return e.repo.UpdateState(ctx, open.ID, domain.IncidentStateRecovering, now, "successful check observed")
	}
	return nil
}

func (e *Engine) recordWindow(ctx context.Context, event events.CheckExecuted) (windowStats, error) {
	key := "incident:window:" + string(event.CheckID)
	nowMs := event.ObservedAt.UnixMilli()
	cutoff := event.ObservedAt.Add(-e.cfg.WindowDuration).UnixMilli()
	member := event.EventID + ":" + string(event.Status)
	pipe := e.redis.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(nowMs), Member: member})
	pipe.ZRemRangeByScore(ctx, key, "0", strconv.FormatInt(cutoff, 10))
	pipe.Expire(ctx, key, e.cfg.WindowDuration*2)
	if _, err := pipe.Exec(ctx); err != nil {
		return windowStats{}, fmt.Errorf("record sliding window: %w", err)
	}
	members, err := e.redis.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     key,
		Start:   strconv.FormatInt(cutoff, 10),
		Stop:    "+inf",
		ByScore: true,
	}).Result()
	if err != nil {
		return windowStats{}, fmt.Errorf("read sliding window: %w", err)
	}
	stats := windowStats{Total: len(members)}
	for _, member := range members {
		if strings.HasSuffix(member, ":"+string(domain.CheckStatusFail)) {
			stats.Failures++
		}
		if strings.HasSuffix(member, ":"+string(domain.CheckStatusOK)) {
			stats.Success++
		}
	}
	return stats, nil
}

func (e *Engine) publishOpened(ctx context.Context, incident domain.Incident, now time.Time) error {
	event := events.IncidentOpened{
		Envelope:        events.NewEnvelope("incidents.opened", e.cfg.ServiceName, now),
		IncidentID:      incident.ID,
		CheckID:         incident.CheckID,
		ResourceID:      incident.ResourceID,
		UserID:          incident.UserID,
		State:           incident.State,
		Severity:        incident.Severity,
		OpenReason:      incident.OpenReason,
		EscalationLevel: incident.EscalationLevel,
		OpenedAt:        incident.OpenedAt,
	}
	return runtime.Retry(ctx, runtime.RetryConfig{Attempts: 3, MinDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second}, func(ctx context.Context) error {
		return e.producer.Publish(ctx, events.TopicIncidentsOpened, []byte(incident.ID), event)
	})
}

func (e *Engine) publishResolved(ctx context.Context, incident domain.Incident, now time.Time) error {
	event := events.IncidentResolved{
		Envelope:   events.NewEnvelope("incidents.resolved", e.cfg.ServiceName, now),
		IncidentID: incident.ID,
		CheckID:    incident.CheckID,
		ResourceID: incident.ResourceID,
		UserID:     incident.UserID,
		State:      domain.IncidentStateOK,
		ResolvedAt: now,
		Reason:     "recovery threshold reached",
	}
	return runtime.Retry(ctx, runtime.RetryConfig{Attempts: 3, MinDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second}, func(ctx context.Context) error {
		return e.producer.Publish(ctx, events.TopicIncidentsResolved, []byte(incident.ID), event)
	})
}

func (e *Engine) publishAlert(ctx context.Context, incident domain.Incident, kind domain.AlertKind, message, dedupSuffix string, now time.Time) error {
	event := events.AlertTriggered{
		Envelope:    events.NewEnvelope("alerts.triggered", e.cfg.ServiceName, now),
		AlertID:     domain.UUID(uuid.NewString()),
		UserID:      incident.UserID,
		IncidentID:  incident.ID,
		CheckID:     incident.CheckID,
		ResourceID:  incident.ResourceID,
		Kind:        kind,
		Severity:    incident.Severity,
		Message:     message,
		DedupKey:    fmt.Sprintf("%s:%s", incident.ID, dedupSuffix),
		TriggeredAt: now,
	}
	return runtime.Retry(ctx, runtime.RetryConfig{Attempts: 3, MinDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second}, func(ctx context.Context) error {
		return e.producer.Publish(ctx, events.TopicAlertsTriggered, []byte(event.DedupKey), event)
	})
}
