package logx

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Options struct {
	Name         string
	Level        string
	Format       string
	Dir          string
	RotateSizeMB int
	RotateKeep   int
}

func New(options Options) (*zap.Logger, error) {
	writer, err := NewWriter(options.Dir, options.RotateSizeMB, options.RotateKeep)
	if err != nil {
		return nil, err
	}

	level := zap.NewAtomicLevelAt(parseLevel(options.Level))
	format := strings.ToLower(strings.TrimSpace(options.Format))
	if format == "" {
		format = "pretty"
	}

	var consoleEncoder zapcore.Encoder
	var fileEncoder zapcore.Encoder

	if format == "json" {
		encoderConfig := zap.NewProductionEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		consoleEncoder = zapcore.NewJSONEncoder(encoderConfig)
		fileEncoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		consoleEncoder = NewPrettyEncoder(ShouldColor())
		fileEncoder = NewPrettyEncoder(false)
	}

	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), level),
		zapcore.NewCore(fileEncoder, zapcore.AddSync(writer), level),
	)

	logger := zap.New(core)
	if strings.TrimSpace(options.Name) != "" {
		logger = logger.Named(options.Name)
	}

	_ = zap.RedirectStdLog(logger)
	return logger, nil
}

func parseLevel(raw string) zapcore.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return zap.DebugLevel
	case "warn", "warning":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	default:
		return zap.InfoLevel
	}
}

func MustChild(logger *zap.Logger, name string) *zap.Logger {
	if logger == nil {
		panic(fmt.Sprintf("nil logger for child %q", name))
	}
	return logger.Named(name)
}
