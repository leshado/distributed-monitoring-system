package main

import (
	"context"
	"fmt"
	"os"

	"distributed-monitoring-system/internal/runtime"
	"distributed-monitoring-system/services/scheduler/config"
	"distributed-monitoring-system/services/scheduler/internal/app"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	ctx, stop := runtime.SignalContext(context.Background())
	defer stop()
	if err := app.Run(ctx, cfg); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "run scheduler: %v\n", err)
		os.Exit(1)
	}
}
