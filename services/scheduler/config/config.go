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
	TickInterval     time.Duration
	LeaseTTL         time.Duration
	BatchSize        int
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
	tick, err := env.Duration("SCHEDULER_TICK_INTERVAL", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	leaseTTL, err := env.Duration("SCHEDULER_LEASE_TTL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	batchSize, err := env.Int("SCHEDULER_BATCH_SIZE", 500)
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
		ServiceName:      env.String("SERVICE_NAME", "scheduler"),
		MetricsAddr:      env.String("METRICS_ADDR", ":9101"),
		LogLevel:         env.String("LOG_LEVEL", "info"),
		DatabaseURL:      dbURL,
		PostgresMaxConns: maxConns,
		RedisAddr:        env.String("REDIS_ADDR", "localhost:6379"),
		RedisPassword:    env.String("REDIS_PASSWORD", ""),
		RedisDB:          redisDB,
		KafkaBrokers:     env.CSV("KAFKA_BROKERS", []string{"localhost:9092"}),
		TickInterval:     tick,
		LeaseTTL:         leaseTTL,
		BatchSize:        batchSize,
		TraceSampleRatio: float64(traceRatioInt) / 1000,
	}, nil
}
