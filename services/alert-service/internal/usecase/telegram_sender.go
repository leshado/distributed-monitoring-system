package usecase

import (
	"context"
	"fmt"
	"html"
	"strings"
	"time"

	"distributed-monitoring-system/internal/adapters"
	"distributed-monitoring-system/internal/domain"
	"distributed-monitoring-system/internal/events"
	"distributed-monitoring-system/internal/failures"

	"github.com/redis/go-redis/v9"
)

type Sender interface {
	Send(ctx context.Context, chatID int64, text string) error
}

type TelegramSender struct {
	client         *adapters.TelegramClient
	redis          *redis.Client
	perMinuteLimit int64
}

func NewTelegramSender(client *adapters.TelegramClient, redis *redis.Client, perMinuteLimit int64) *TelegramSender {
	if perMinuteLimit <= 0 {
		perMinuteLimit = 20
	}
	return &TelegramSender{client: client, redis: redis, perMinuteLimit: perMinuteLimit}
}

func (s *TelegramSender) Send(ctx context.Context, chatID int64, text string) error {
	for {
		key := fmt.Sprintf("rate:telegram:%d:%s", chatID, time.Now().UTC().Format("200601021504"))
		count, err := s.redis.Incr(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("telegram rate limit increment: %w", err)
		}
		if count == 1 {
			_ = s.redis.Expire(ctx, key, time.Minute+5*time.Second).Err()
		}
		if count <= s.perMinuteLimit {
			return s.client.SendMessage(ctx, chatID, text)
		}
		ttl, err := s.redis.TTL(ctx, key).Result()
		if err != nil || ttl <= 0 {
			ttl = time.Second
		}
		timer := time.NewTimer(ttl)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func FormatTelegramMessage(event events.AlertTriggered, ctx AlertContext) string {
	title := "🔴 Проблема с сервисом"
	if event.Kind == domain.AlertKindIncidentResolved {
		title = "✅ Сервис восстановился"
	}
	if event.Kind == domain.AlertKindEscalation {
		title = "⚠️ Проблема всё ещё актуальна"
	}

	lines := []string{
		"<b>" + title + "</b>",
		"",
		"<b>" + escape(ctx.Check.Name) + "</b>",
		"Тип: <b>" + readableCheckType(ctx.Check.Type) + "</b>",
		"Цель: <code>" + escape(checkTarget(ctx.Check)) + "</code>",
		"Важность: <b>" + severityLabel(event.Severity) + "</b>",
	}

	if event.Kind == domain.AlertKindIncidentResolved {
		lines = append(lines,
			"",
			"Проверки снова проходят успешно.",
			"Восстановлено: "+formatUserTime(ctx.User, event.TriggeredAt),
		)
		return strings.Join(lines, "\n")
	}

	explained := failures.Explain(ctx.Check, ctx.Incident.OpenReason, 0)
	lines = append(lines,
		"",
		"Проблема: <b>"+escape(explained.Title)+"</b>",
		escape(explained.Description),
		"Началась: "+formatUserTime(ctx.User, ctx.Incident.OpenedAt),
	)
	if explained.Hint != "" {
		lines = append(lines, "Что проверить: "+escape(explained.Hint))
	}
	if ctx.Incident.EscalationLevel > 0 {
		lines = append(lines, fmt.Sprintf("Эскалация: уровень %d", ctx.Incident.EscalationLevel))
	}
	return strings.Join(lines, "\n")
}

func checkTarget(check domain.Check) string {
	switch check.Type {
	case domain.CheckTypeHTTP:
		if check.Config.HTTP != nil {
			return check.Config.HTTP.URL
		}
	case domain.CheckTypeTCP:
		if check.Config.TCP != nil {
			return check.Config.TCP.Address
		}
	case domain.CheckTypeTLS:
		if check.Config.TLS != nil {
			return check.Config.TLS.Address
		}
	}
	return string(check.ID)
}

func readableCheckType(checkType domain.CheckType) string {
	switch checkType {
	case domain.CheckTypeHTTP:
		return "HTTP-страница или API"
	case domain.CheckTypeTCP:
		return "TCP-порт"
	case domain.CheckTypeTLS:
		return "TLS-сертификат"
	default:
		return strings.ToUpper(string(checkType))
	}
}

func severityLabel(severity string) string {
	switch severity {
	case "critical":
		return "🔴 Критично"
	case "warning":
		return "🟡 Предупреждение"
	default:
		return "ℹ️ Информация"
	}
}

func formatUserTime(user domain.User, t time.Time) string {
	loc := time.UTC
	tz := strings.TrimSpace(user.Timezone)
	if tz == "" {
		tz = "UTC"
	}
	if tz != "UTC" {
		if loaded, err := time.LoadLocation(tz); err == nil {
			loc = loaded
		}
	}
	return t.In(loc).Format("02.01 15:04") + " " + tz
}

func escape(s string) string {
	return html.EscapeString(s)
}
