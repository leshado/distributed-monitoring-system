package config

import env "distributed-monitoring-system/internal/runtime"

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
	Concurrency      int
	WorkerID         string
	TraceSampleRatio float64
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
	concurrency, err := env.Int("WORKER_CONCURRENCY", 64)
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
		ServiceName:      env.String("SERVICE_NAME", "check-worker"),
		MetricsAddr:      env.String("METRICS_ADDR", ":9102"),
		LogLevel:         env.String("LOG_LEVEL", "info"),
		DatabaseURL:      dbURL,
		PostgresMaxConns: maxConns,
		RedisAddr:        env.String("REDIS_ADDR", "localhost:6379"),
		RedisPassword:    env.String("REDIS_PASSWORD", ""),
		RedisDB:          redisDB,
		KafkaBrokers:     env.CSV("KAFKA_BROKERS", []string{"localhost:9092"}),
		ConsumerGroup:    env.String("WORKER_CONSUMER_GROUP", "check-workers"),
		Concurrency:      concurrency,
		WorkerID:         env.String("WORKER_ID", env.String("HOSTNAME", "worker-local")),
		TraceSampleRatio: float64(traceRatioInt) / 1000,
	}, nil
}
