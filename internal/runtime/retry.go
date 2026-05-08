package runtime

import (
	"context"
	"math/rand/v2"
	"time"
)

type RetryConfig struct {
	Attempts int
	MinDelay time.Duration
	MaxDelay time.Duration
}

func Retry(ctx context.Context, cfg RetryConfig, fn func(context.Context) error) error {
	if cfg.Attempts <= 0 {
		cfg.Attempts = 1
	}
	if cfg.MinDelay <= 0 {
		cfg.MinDelay = 100 * time.Millisecond
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 5 * time.Second
	}

	var err error
	delay := cfg.MinDelay
	for attempt := 1; attempt <= cfg.Attempts; attempt++ {
		if err = fn(ctx); err == nil {
			return nil
		}
		if attempt == cfg.Attempts {
			return err
		}
		jitter := time.Duration(rand.Int64N(int64(delay / 2)))
		timer := time.NewTimer(delay + jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay *= 2
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
	}
	return err
}
