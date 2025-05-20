package logging

 import (
	"fmt"
	"log/slog"
	"os"

	"github.com/tonyfg/trucker/pkg/config"
)

type SimpleLogger struct {
	Debug func(string)
	Info func(string)
	Warn func(string)
	Error func(string)
}

func Init(cfg config.Config) {
	var logger *slog.Logger
	opts := &slog.HandlerOptions{}

	switch cfg.LogLevel {
	case "debug":
		opts.Level = slog.LevelDebug
	case "info", "":
		opts.Level = slog.LevelInfo
	case "warn":
		opts.Level = slog.LevelWarn
	case "error":
		opts.Level = slog.LevelError
	default:
		panic(fmt.Sprintf(
			"Unknown log level: %s\nAvailable levels are: debug, info, warn, error",
			cfg.LogStyle,
		))
	}

	switch cfg.LogStyle {
	case "text", "":
		logger = slog.New(slog.NewTextHandler(os.Stderr, opts))
	case "json":
		logger = slog.New(slog.NewJSONHandler(os.Stderr, opts))
	default:
		panic(fmt.Sprintf(
			"Unknown logger style: %s\nAvailable styles are: text, json",
			cfg.LogStyle,
		))
	}

	slog.SetDefault(logger)
}

func MakeSimpleLogger(module string) SimpleLogger {
	return SimpleLogger{
		Debug: func(msg string) { slog.Debug(module, "msg", msg) },
		Info: func(msg string) { slog.Info(module, "msg", msg) },
		Warn: func(msg string) { slog.Warn(module, "msg", msg) },
		Error: func(msg string) { slog.Error(module, "msg", msg) },
	}
}
