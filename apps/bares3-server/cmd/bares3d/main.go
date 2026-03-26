package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"bares3-server/internal/app"
	"bares3-server/internal/buildinfo"
	"bares3-server/internal/config"
	"bares3-server/internal/logx"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "", "path to config.yml")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Fprintln(os.Stdout, buildinfo.Current().String())
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	logger, err := logx.New(logx.Options{
		Name:         "bares3",
		Level:        cfg.Logging.Level,
		Format:       cfg.Logging.Format,
		Dir:          cfg.Paths.LogDir,
		RotateSizeMB: cfg.Logging.RotateSizeMB,
		RotateKeep:   cfg.Logging.RotateKeep,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = logger.Sync()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("starting BareS3", logx.StartField(), zap.String("version", buildinfo.Current().String()))

	service := app.New(cfg, logger)
	if err := service.Run(ctx); err != nil {
		logger.Fatal("service stopped with error", zap.Error(err))
	}
}
