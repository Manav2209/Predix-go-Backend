// Package logging provides JSON-structured logging for the distributed
// engine (P3.3). Every line carries an instanceId identifying the process,
// and callers attach command context (requestId, commandId, orderId, eventId,
// partitionId, sequence) via With/Info attributes.
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
)

type Logger struct {
	slog *slog.Logger
}

// New builds a JSON logger on w (default stdout). If instanceID is empty a
// hostname:pid identifier is generated.
func New(instanceID string, level slog.Level, w io.Writer) *Logger {
	if w == nil {
		w = os.Stdout
	}

	if instanceID == "" {
		host, err := os.Hostname()
		if err != nil {
			host = "unknown"
		}
		instanceID = host + ":" + itoa(os.Getpid())
	}

	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
	})

	return &Logger{
		slog: slog.New(handler).With("instanceId", instanceID),
	}
}

func (l *Logger) With(attrs ...any) *Logger {
	return &Logger{slog: l.slog.With(attrs...)}
}

func (l *Logger) Debug(msg string, attrs ...any) {
	l.slog.Debug(msg, attrs...)
}

func (l *Logger) Info(msg string, attrs ...any) {
	l.slog.Info(msg, attrs...)
}

func (l *Logger) Warn(msg string, attrs ...any) {
	l.slog.Warn(msg, attrs...)
}

func (l *Logger) Error(msg string, err error, attrs ...any) {
	l.slog.Error(msg, append(attrs, "error", err)...)
}

func (l *Logger) InfoContext(ctx context.Context, msg string, attrs ...any) {
	l.slog.InfoContext(ctx, msg, attrs...)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	var buf [20]byte
	i := len(buf)

	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	return string(buf[i:])
}
