package config

import (
	"time"

	env "distributed-monitoring-system/internal/runtime"
)

type Config struct {
	ServiceName      string
	MetricsAddr      string
	LogLevel         string
	DatabaseURL      string
	PostgresMaxConns int
	RedisAddr        string
	RedisPassword    string
	RedisDB          int
	KafkaBrokers     []string
	ConsumerGroup    string
	TelegramBotToken string
	RetryAttempts    int
	RetryMinDelay    time.Duration
	RetryMaxDelay    time.Duration
	DedupTTL         time.Duration
	Concurrency      int
	TraceSampleRatio float64
}

func Load() (Config, error) {
	maxConns, err := env.Int("POSTGRES_MAX_CONNS", 10)
	if err != nil {
		return Config{}, err
	}
	redisDB, err := env.Int("REDIS_DB", 0)
	if err != nil {
		return Config{}, err
	}
	attempts, err := env.Int("ALERT_RETRY_ATTEMPTS", 5)
	if err != nil {
		return Config{}, err
	}
	minDelay, err := env.Duration("ALERT_RETRY_MIN_DELAY", 500*time.Millisecond)
	if err != nil {
		return Config{}, err
	}
	maxDelay, err := env.Duration("ALERT_RETRY_MAX_DELAY", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	dedupTTL, err := env.Duration("ALERT_DEDUP_TTL", 30*time.Minute)
	if err != nil {
		return Config{}, err
	}
	concurrency, err := env.Int("ALERT_CONCURRENCY", 32)
	if err != nil {
		return Config{}, err
	}
	traceRatioInt, err := env.Int("TRACE_SAMPLE_PER_1000", 50)
	if err != nil {
		return Config{}, err
	}
	dbURL, err := env.RequiredString("DATABASE_URL")
	if err != nil {
		return Config{}, err
	}
	token, err := env.RequiredString("TELEGRAM_BOT_TOKEN")
	if err != nil {
		return Config{}, err
	}
	return Config{
		ServiceName:      env.String("SERVICE_NAME", "alert-service"),
		MetricsAddr:      env.String("METRICS_ADDR", ":9104"),
		LogLevel:         env.String("LOG_LEVEL", "info"),
		DatabaseURL:      dbURL,
		PostgresMaxConns: maxConns,
		RedisAddr:        env.String("REDIS_ADDR", "localhost:6379"),
		RedisPassword:    env.String("REDIS_PASSWORD", ""),
		RedisDB:          redisDB,
		KafkaBrokers:     env.CSV("KAFKA_BROKERS", []string{"localhost:9092"}),
		ConsumerGroup:    env.String("ALERT_CONSUMER_GROUP", "alert-service"),
		TelegramBotToken: token,
		RetryAttempts:    attempts,
		RetryMinDelay:    minDelay,
		RetryMaxDelay:    maxDelay,
		DedupTTL:         dedupTTL,
		Concurrency:      concurrency,
		TraceSampleRatio: float64(traceRatioInt) / 1000,
	}, nil
}
