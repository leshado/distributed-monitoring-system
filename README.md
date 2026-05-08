# Distributed Monitoring System

Distributed Monitoring System - это backend-платформа для мониторинга доступности сервисов с интерфейсом через
Telegram-бота. Система проверяет HTTP-страницы и API, TCP-порты и TLS-сертификаты, хранит историю проверок, считает
доступность, обнаруживает инциденты и отправляет уведомления в Telegram.

Система состоит из нескольких независимых Go-сервисов и использует:

- Kafka для событийного взаимодействия;
- PostgreSQL как основное хранилище;
- Redis для locks, leases, deduplication и sliding windows;
- Docker Compose для локального запуска;
- Prometheus и Grafana для метрик и dashboard;
- OpenTelemetry для базовой инициализации tracing.

## Возможности

Telegram-бот является основным пользовательским интерфейсом.

Через бота можно:

- добавить HTTP, TCP или TLS-проверку;
- посмотреть общий dashboard состояния;
- открыть карточку конкретной проверки;
- запустить проверку вручную;
- поставить проверку на паузу на время или до ручного включения;
- возобновить, изменить цель или удалить проверку;
- настроить интервал повторных алертов, режим тишины и часовой пояс;
- посмотреть активные инциденты и историю последних событий.

## Типы проверок

HTTP:

- проверяет URL;
- поддерживает ожидаемый HTTP-код;
- поддерживает поиск строки в теле ответа;
- использует timeout для каждой проверки.

TCP:

- проверяет, что можно открыть соединение с `host:port`;
- подходит для баз данных, брокеров, внутренних сервисов и кастомных портов.

TLS:

- открывает TLS-соединение с `host:port`;
- проверяет доступность сертификата;
- проверяет минимальный срок действия сертификата в днях.

## Архитектура

![Архитектура Distributed Monitoring System](docs/architecture.png)

Схема хранится как PNG в [docs/architecture.png](docs/architecture.png).

Сервисы:

- `api`: REST API, Telegram-бот, управление пользователями, настройками и проверками.
- `scheduler`: выбирает проверки, которые пора запустить, и публикует события `checks.scheduled`.
- `worker`: выполняет HTTP/TCP/TLS-проверки, сохраняет результаты и публикует `checks.executed`.
- `incident-engine`: анализирует результаты проверок, открывает/закрывает инциденты и публикует события для алертов.
- `alert-service`: дедуплицирует уведомления и отправляет сообщения в Telegram.
- `migrate`: применяет миграции PostgreSQL.

Инфраструктура:

- PostgreSQL хранит основное состояние.
- Redis используется для locks, leases, дедупликации и sliding window для антифлаппинга.
- Kafka связывает сервисы через события.
- Prometheus endpoints отдают метрики сервисов.
- OpenTelemetry tracing инициализируется в каждом сервисе.

## Структура репозитория

```text
.
├── demo/
│   └── monitor-targets/        локальные HTTP/TCP/TLS demo targets
├── deployments/
│   ├── grafana/                provisioning и dashboard для Grafana
│   └── prometheus/             Prometheus scrape config
├── docs/
│   └── kafka-event-schemas.json
├── internal/
│   ├── adapters/               PostgreSQL, Redis, Kafka, Telegram clients
│   ├── checkrunner/            выполнение HTTP/TCP/TLS-проверок
│   ├── domain/                 доменные модели и value types
│   ├── events/                 Kafka topics и event structs
│   ├── failures/               объяснение ошибок человеческим языком
│   ├── observability/          логирование, метрики, tracing
│   ├── runtime/                env, retry, graceful shutdown
│   └── transport/              общий HTTP server и API transport
├── migrations/                 миграции PostgreSQL
└── services/
    ├── alert-service/
    ├── api/
    ├── incident-engine/
    ├── migrate/
    ├── scheduler/
    └── worker/
```

## Kafka events

Контракты событий описаны в двух местах:

- [internal/events/events.go](internal/events/events.go) - Go-структуры, которые реально используются кодом.
- [docs/kafka-event-schemas.json](docs/kafka-event-schemas.json) - человекочитаемая документация формата сообщений.

Основные topics:

- `checks.scheduled`
- `checks.executed`
- `incidents.opened`
- `incidents.resolved`
- `alerts.triggered`

Поле `schema: "v1"` добавляется в envelope каждого события.

## База данных

Миграции управляются через `github.com/pressly/goose/v3`.

В Docker Compose миграции выполняет отдельный сервис `migrate`: он стартует после готовности PostgreSQL и до запуска
основных сервисов.

- `000001_foundation.sql`: extensions и общие функции.
- `000002_tables.sql`: users, resources, checks, executions, incidents, alerts, aggregates.
- `000003_indexes_triggers.sql`: индексы и triggers для `updated_at`.

Основные таблицы:

- `users`
- `resources`
- `checks`
- `check_executions`
- `incidents`
- `incident_events`
- `alerts`
- `metrics_aggregated`

Локальный запуск миграций:

```bash
DATABASE_URL='postgres://monitoring:monitoring@localhost:5432/monitoring?sslmode=disable' make migrate-local
```

## Расчет доступности

Доступность за 24 часа считается как time-based estimate.

Каждый результат проверки считается состоянием монитора до следующего результата. Если проверка поставлена на паузу,
окно аналитики не продолжает искусственно тянуть последнее состояние до текущего времени.

## Локальный запуск

Требования:

- Go 1.26+
- Docker и Docker Compose
- Telegram bot token

Создать `.env`:

```bash
cp .env.example .env
```

Указать реальный токен:

```env
TELEGRAM_BOT_TOKEN=123456:replace-with-real-token
API_KEY=local-dev-key
```

Запустить инфраструктуру и сервисы:

```bash
docker compose up --build
```

Или через Make:

```bash
make up
```

Остановить:

```bash
make down
```

API будет доступен на:

```text
http://localhost:8080
```

Prometheus:

```text
http://localhost:9090
```

Grafana:

```text
http://localhost:3000
```

Логин и пароль Grafana по умолчанию:

```text
admin / admin
```

Метрики:

- `http://localhost:9100/metrics` - API
- `http://localhost:9101/metrics` - Scheduler
- `http://localhost:9102/metrics` - Worker
- `http://localhost:9103/metrics` - Incident Engine
- `http://localhost:9104/metrics` - Alert Service

## Метрики и наблюдаемость

Каждый сервис поднимает отдельный HTTP endpoint `/metrics` в формате Prometheus.

Метрики используются для observability: по ним можно понять, что сервисы живы, как быстро они обрабатывают запросы и
Kafka-сообщения, сколько проверок выполняется, сколько ошибок и инцидентов появляется, сколько алертов отправляется.

Примеры метрик:

- `dms_api_bot_handler_latency_seconds` - latency HTTP handlers в API.
- `dms_check_worker_checks_executed_total` - количество выполненных проверок по статусу и типу.
- `dms_check_worker_check_latency_seconds` - latency внешних HTTP/TCP/TLS-проверок.
- `dms_incident_engine_incidents_opened_total` - количество открытых инцидентов.
- `dms_incident_engine_incidents_closed_total` - количество закрытых инцидентов.
- `dms_alert_service_alerts_triggered_total` - количество созданных алертов.
- `dms_alert_service_alerts_delivered_total` - результат отправки алертов.
- `dms_*_kafka_handler_duration_seconds` - время обработки Kafka-сообщений.
- `dms_*_kafka_handler_retries_total` - retry при обработке Kafka-сообщений.
- `dms_*_kafka_dlq_total` - сообщения, отправленные в DLQ.

Docker Compose поднимает Prometheus и Grafana:

- [deployments/prometheus/prometheus.yml](deployments/prometheus/prometheus.yml) описывает scrape targets для всех
  сервисов.
- Grafana автоматически получает Prometheus datasource.
- Dashboard `Distributed Monitoring System` загружается
  из [deployments/grafana/dashboards/distributed-monitoring-system.json](deployments/grafana/dashboards/distributed-monitoring-system.json).

Dashboard показывает:

- количество открытых и закрытых инцидентов;
- количество созданных алертов;
- rate выполненных проверок по типу и статусу;
- p95 latency внешних проверок;
- Kafka retries и DLQ;
- результат отправки Telegram-алертов.

## Переменные окружения

Минимально нужны:

- `TELEGRAM_BOT_TOKEN`
- `API_KEY`
- `DATABASE_URL`

В Docker Compose `DATABASE_URL`, `REDIS_ADDR` и `KAFKA_BROKERS` уже заданы внутри сервисов.

Основные optional variables:

- `REDIS_ADDR`
- `REDIS_PASSWORD`
- `REDIS_DB`
- `KAFKA_BROKERS`
- `LOG_LEVEL`
- `POSTGRES_MAX_CONNS`
- `TRACE_SAMPLE_PER_1000`
- `HTTP_ADDR`
- `METRICS_ADDR`
- `TELEGRAM_POLL_LIMIT`
- `API_MAX_BODY_BYTES`
- `API_RATE_LIMIT_RPS`
- `API_RATE_LIMIT_BURST`
- `SCHEDULER_TICK_INTERVAL`
- `SCHEDULER_LEASE_TTL`
- `SCHEDULER_BATCH_SIZE`
- `WORKER_CONCURRENCY`
- `WORKER_CONSUMER_GROUP`
- `WORKER_ID`
- `INCIDENT_WINDOW`
- `INCIDENT_FAILURE_THRESHOLD`
- `INCIDENT_RECOVERY_THRESHOLD`
- `INCIDENT_ESCALATION_AFTER`
- `ALERT_RETRY_ATTEMPTS`
- `ALERT_RETRY_MIN_DELAY`
- `ALERT_RETRY_MAX_DELAY`
- `ALERT_DEDUP_TTL`
- `ALERT_CONCURRENCY`
- `GRAFANA_ADMIN_USER`
- `GRAFANA_ADMIN_PASSWORD`

## Demo targets

В проекте есть отдельный demo-сервис для проверки реальных HTTP, TCP и TLS failures. Он не входит в Docker Compose и
запускается отдельно на хосте.

Запуск:

```bash
go run ./demo/monitor-targets
```

Demo targets:

- HTTP: `http://localhost:18080/health`
- TCP: `localhost:19090`
- TLS: `localhost:19443`

Если бот работает внутри Docker, для доступа к demo targets нужно использовать `host.docker.internal`:

```text
http://host.docker.internal:18080/health
host.docker.internal:19090
host.docker.internal:19443
```

Команды demo-сервиса:

```text
status
targets
start http|tcp|tls|all
stop http|tcp|tls|all
ok http|tcp|tls|all
fail http|tcp|tls|all
slow http
exit
```

Поведение:

- `fail http` оставляет HTTP-сервер запущенным, но возвращает ошибочный ответ.
- `fail tcp` останавливает TCP listener.
- `fail tls` останавливает TLS listener.
- `slow http` задерживает HTTP-ответ и помогает проверить timeout.

## Сценарий проверки через Telegram

Команды бота:

```text
/start
/status
/checks
/add
/incidents
/settings
/help
```

Рекомендуемый demo flow:

1. Запустить `docker compose up --build`.
2. Запустить `go run ./demo/monitor-targets`.
3. Открыть Telegram-бота и нажать `Добавить`.
4. Добавить HTTP target: `http://host.docker.internal:18080/health`.
5. Добавить TCP target: `host.docker.internal:19090`.
6. Перевести TCP target в ошибку командой `fail tcp` в demo-сервисе.
7. Дождаться открытия инцидента.
8. Восстановить target командой `ok tcp`.
9. Проверить, что инцидент закрылся, а история читается в карточке проверки.

## REST API

REST API защищен заголовками:

- `X-API-Key`
- `X-Telegram-Chat-ID`

Создать resource:

```bash
curl -X POST http://localhost:8080/v1/resources \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: local-dev-key' \
  -H 'X-Telegram-Chat-ID: 123456789' \
  -d '{"name":"Example API","target":"https://example.com"}'
```

Создать HTTP check:

```bash
curl -X POST http://localhost:8080/v1/checks \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: local-dev-key' \
  -H 'X-Telegram-Chat-ID: 123456789' \
  -d '{
    "resource_id":"<resource-id>",
    "type":"http",
    "name":"Example API",
    "interval_seconds":60,
    "timeout_millis":5000,
    "jitter_millis":3000,
    "config":{
      "http":{
        "method":"GET",
        "url":"https://example.com",
        "expected_code":200
      }
    }
  }'
```

Получить список checks:

```bash
curl http://localhost:8080/v1/checks \
  -H 'X-API-Key: local-dev-key' \
  -H 'X-Telegram-Chat-ID: 123456789'
```

Получить аналитику:

```bash
curl http://localhost:8080/v1/checks/<check-id>/analytics \
  -H 'X-API-Key: local-dev-key' \
  -H 'X-Telegram-Chat-ID: 123456789'
```

## Команды разработки

```bash
make fmt
make tidy
make build
make test
make vet
make docker-build
make migrate-local
make demo
```

Сборка всех Go-пакетов:

```bash
go build ./...
```

Проверка:

```bash
go test ./...
go vet ./...
```

## Что демонстрирует проект

- Разделение backend-системы на несколько самостоятельных Go-сервисов.
- Асинхронное взаимодействие сервисов через Kafka-события.
- Проектирование схемы PostgreSQL и работу с миграциями.
- Использование Redis для distributed locks, leases, дедупликации и sliding windows.
- Пользовательский интерфейс через Telegram-бота, а не только REST API.
- Полный жизненный цикл инцидента: открытие, восстановление и повышение приоритета.
- Метрики Prometheus, dashboard в Grafana и базовую инициализацию tracing.
- Локальный запуск всей системы через Docker Compose.
- Симуляцию отказов HTTP, TCP и TLS-сервисов через demo targets.
