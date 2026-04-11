package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"bares3-server/internal/admin"
	"bares3-server/internal/buildinfo"
	"bares3-server/internal/config"
	"bares3-server/internal/fileserve"
	"bares3-server/internal/logx"
	"bares3-server/internal/replication"
	"bares3-server/internal/s3api"
	"bares3-server/internal/s3creds"
	"bares3-server/internal/storage"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type App struct {
	cfg    config.Config
	logger *zap.Logger
	store  *storage.Store
	creds  *s3creds.Store
}

type serverSpec struct {
	name    string
	addr    string
	handler http.Handler
	logger  *zap.Logger
}

func New(cfg config.Config, logger *zap.Logger) *App {
	creds, err := s3creds.New(cfg.Paths.DataDir, logx.MustChild(logger, "s3creds"))
	if err != nil {
		panic(fmt.Sprintf("initialize s3 credential store: %v", err))
	}
	return &App{cfg: cfg, logger: logger, store: storage.New(cfg, logx.MustChild(logger, "storage")), creds: creds}
}

func (a *App) Run(ctx context.Context) error {
	if err := a.prepareRuntimeDirs(); err != nil {
		return err
	}

	a.logger.Info(
		"runtime prepared",
		logx.ReadyField(),
		zap.String("version", buildinfo.Current().Version),
		zap.String("config_path", a.configPathForLog()),
		zap.String("data_dir", a.cfg.Paths.DataDir),
		zap.String("log_dir", a.cfg.Paths.LogDir),
		zap.String("tmp_dir", a.cfg.Paths.TmpDir),
		zap.String("metadata_layout", a.store.MetadataLayout()),
	)

	specs := []serverSpec{
		{name: "admin", addr: a.cfg.Listen.Admin, handler: admin.NewHandler(a.cfg, a.store, a.creds, logx.MustChild(a.logger, "admin")), logger: logx.MustChild(a.logger, "admin")},
		{name: "s3", addr: a.cfg.Listen.S3, handler: s3api.NewHandler(a.cfg, a.store, a.creds, logx.MustChild(a.logger, "s3")), logger: logx.MustChild(a.logger, "s3")},
		{name: "file", addr: a.cfg.Listen.File, handler: fileserve.NewHandler(a.cfg, a.store, logx.MustChild(a.logger, "file")), logger: logx.MustChild(a.logger, "file")},
	}

	group, groupCtx := errgroup.WithContext(ctx)
	servers := make([]*http.Server, 0, len(specs))

	for _, spec := range specs {
		srv := &http.Server{
			Addr:              spec.addr,
			Handler:           spec.handler,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
		}
		servers = append(servers, srv)

		spec := spec
		group.Go(func() error {
			return serve(groupCtx, srv, spec)
		})
	}
	group.Go(func() error {
		worker := replication.NewWorker(a.cfg, a.store, logx.MustChild(a.logger, "replication"))
		return worker.Run(groupCtx)
	})

	group.Go(func() error {
		<-groupCtx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var firstErr error
		for _, srv := range servers {
			if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	})

	err := group.Wait()
	if errors.Is(ctx.Err(), context.Canceled) {
		a.logger.Info("shutdown complete", logx.SuccessField())
		return nil
	}
	return err
}

func (a *App) prepareRuntimeDirs() error {
	for _, dir := range []string{a.cfg.Paths.DataDir, a.cfg.Paths.LogDir, a.cfg.Paths.TmpDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create runtime dir %s: %w", dir, err)
		}
	}
	return nil
}

func (a *App) configPathForLog() string {
	if a.cfg.Runtime.ConfigUsed {
		return a.cfg.Runtime.ConfigPath
	}
	return "defaults"
}

func serve(ctx context.Context, srv *http.Server, spec serverSpec) error {
	listener, err := net.Listen("tcp", spec.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", spec.addr, err)
	}

	spec.logger.Info("server listening", logx.StartField(), zap.String("service", spec.name), zap.String("addr", spec.addr))

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve %s: %w", spec.name, err)
		}
		return nil
	}
}
