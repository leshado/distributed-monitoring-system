package failures

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"distributed-monitoring-system/internal/domain"
)

type Kind string

const (
	KindNone               Kind = "none"
	KindConnectionRefused  Kind = "connection_refused"
	KindTimeout            Kind = "timeout"
	KindDNS                Kind = "dns_error"
	KindTLS                Kind = "tls_error"
	KindHTTPStatusMismatch Kind = "http_status_mismatch"
	KindBodyMismatch       Kind = "body_mismatch"
	KindCertificateExpiry  Kind = "certificate_expiring"
	KindUnknown            Kind = "unknown"
)

type Explanation struct {
	Kind        Kind
	Title       string
	Description string
	Hint        string
	Technical   string
}

var statusRe = regexp.MustCompile(`unexpected status (\d+), expected (\d+)`)

func Explain(check domain.Check, raw string, statusCode int) Explanation {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Explanation{Kind: KindNone, Title: "Проверка прошла успешно", Description: "Сервис ответил ожидаемым образом."}
	}

	lower := strings.ToLower(raw)
	switch {
	case strings.Contains(lower, "connection refused"):
		return Explanation{Kind: KindConnectionRefused, Title: "Сервис не принимает соединение", Description: "Бот дошел до адреса, но приложение на этом порту не отвечает.", Hint: "Проверьте, что сервер запущен, порт открыт и адрес указан правильно.", Technical: raw}
	case strings.Contains(lower, "i/o timeout") || strings.Contains(lower, "context deadline exceeded") || strings.Contains(lower, "timeout"):
		return Explanation{Kind: KindTimeout, Title: "Сервис не ответил вовремя", Description: "Запрос занял больше разрешенного времени.", Hint: "Проверьте нагрузку, сеть и timeout проверки.", Technical: raw}
	case strings.Contains(lower, "no such host") || strings.Contains(lower, "server misbehaving"):
		return Explanation{Kind: KindDNS, Title: "Домен не найден", Description: "Не удалось получить IP-адрес домена.", Hint: "Проверьте домен и DNS-записи.", Technical: raw}
	case strings.Contains(lower, "certificate expires"):
		return Explanation{Kind: KindCertificateExpiry, Title: "TLS-сертификат скоро истечет", Description: "Сертификат подходит к концу раньше заданного порога.", Hint: "Обновите сертификат или измените порог проверки.", Technical: raw}
	case strings.Contains(lower, "tls") || strings.Contains(lower, "certificate") || strings.Contains(lower, "x509"):
		return Explanation{Kind: KindTLS, Title: "Проблема с TLS-соединением", Description: "Не удалось установить защищенное соединение.", Hint: "Проверьте сертификат, SNI, адрес и порт.", Technical: raw}
	case strings.Contains(lower, "unexpected status"):
		return explainStatus(raw, statusCode)
	case strings.Contains(lower, "body does not contain"):
		return Explanation{Kind: KindBodyMismatch, Title: "Ответ не содержит ожидаемый текст", Description: "Сервис ответил, но содержимое не совпало с проверкой.", Hint: "Проверьте ожидаемый текст или сам ответ сервиса.", Technical: raw}
	default:
		return Explanation{Kind: KindUnknown, Title: "Проверка завершилась ошибкой", Description: "Сервис не прошел проверку.", Hint: hintByType(check.Type), Technical: raw}
	}
}

func explainStatus(raw string, statusCode int) Explanation {
	title := "HTTP-статус не совпал"
	description := "Сервис ответил, но вернул не тот HTTP-код."
	if m := statusRe.FindStringSubmatch(raw); len(m) == 3 {
		got, _ := strconv.Atoi(m[1])
		want, _ := strconv.Atoi(m[2])
		description = fmt.Sprintf("Ожидали HTTP %d, получили HTTP %d.", want, got)
	} else if statusCode > 0 {
		description = fmt.Sprintf("Сервис вернул HTTP %d.", statusCode)
	}
	return Explanation{Kind: KindHTTPStatusMismatch, Title: title, Description: description, Hint: "Проверьте health endpoint, expected status и логи сервиса.", Technical: raw}
}

func hintByType(checkType domain.CheckType) string {
	switch checkType {
	case domain.CheckTypeHTTP:
		return "Проверьте URL, HTTP-статус и содержимое ответа."
	case domain.CheckTypeTCP:
		return "Проверьте host:port, firewall и запущен ли процесс."
	case domain.CheckTypeTLS:
		return "Проверьте host:port, сертификат и имя сервера."
	default:
		return "Проверьте настройки монитора и доступность сервиса."
	}
}
