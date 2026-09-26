// internal/logging/logging.go
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New builds a JSON logger tagged with the service name (e.g. "api",
// "consumer", "priceservice"), so a single log stream can be filtered
// by which process emitted the entry.
func New(service string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: levelFromEnv(),
	})
	return slog.New(handler).With("service", service)
}

func levelFromEnv() slog.Level {
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
