package observability

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewLogger(service, level string) (*zap.Logger, error) {
	var cfg zap.Config
	cfg = zap.NewProductionConfig()
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.EncodeDuration = zapcore.MillisDurationEncoder
	cfg.InitialFields = map[string]any{"service": service}
	if level != "" {
		parsed, err := zapcore.ParseLevel(level)
		if err != nil {
			return nil, err
		}
		cfg.Level = zap.NewAtomicLevelAt(parsed)
	}
	return cfg.Build()
}
