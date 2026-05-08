package config

import (
	"time"

	env "distributed-monitoring-system/internal/runtime"
)

type Config struct {
	ServiceName       string
	MetricsAddr       string
	LogLevel          string
	DatabaseURL       string
	PostgresMaxConns  int
	RedisAddr         string
	RedisPassword     string
	RedisDB           int
	KafkaBrokers      []string
	ConsumerGroup     string
	WindowDuration    time.Duration
	FailureThreshold  int
	RecoveryThreshold int
	EscalationAfter   time.Duration
	TraceSampleRatio  float64
}

func Load() (Config, error) {
	maxConns, err := env.Int("POSTGRES_MAX_CONNS", 20)
	if err != nil {
		return Config{}, err
	}
	redisDB, err := env.Int("REDIS_DB", 0)
	if err != nil {
		return Config{}, err
	}
	window, err := env.Duration("INCIDENT_WINDOW", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	failThreshold, err := env.Int("INCIDENT_FAILURE_THRESHOLD", 3)
	if err != nil {
		return Config{}, err
	}
	recoveryThreshold, err := env.Int("INCIDENT_RECOVERY_THRESHOLD", 2)
	if err != nil {
		return Config{}, err
	}
	escalationAfter, err := env.Duration("INCIDENT_ESCALATION_AFTER", 30*time.Minute)
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
	return Config{
		ServiceName:       env.String("SERVICE_NAME", "incident-engine"),
		MetricsAddr:       env.String("METRICS_ADDR", ":9103"),
		LogLevel:          env.String("LOG_LEVEL", "info"),
		DatabaseURL:       dbURL,
		PostgresMaxConns:  maxConns,
		RedisAddr:         env.String("REDIS_ADDR", "localhost:6379"),
		RedisPassword:     env.String("REDIS_PASSWORD", ""),
		RedisDB:           redisDB,
		KafkaBrokers:      env.CSV("KAFKA_BROKERS", []string{"localhost:9092"}),
		ConsumerGroup:     env.String("INCIDENT_CONSUMER_GROUP", "incident-engine"),
		WindowDuration:    window,
		FailureThreshold:  failThreshold,
		RecoveryThreshold: recoveryThreshold,
		EscalationAfter:   escalationAfter,
		TraceSampleRatio:  float64(traceRatioInt) / 1000,
	}, nil
}
