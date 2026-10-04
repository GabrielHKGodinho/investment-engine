package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GabrielHKGodinho/investment-engine/internal/logging"
	"github.com/GabrielHKGodinho/investment-engine/internal/metrics"
	"github.com/GabrielHKGodinho/investment-engine/internal/order"
	"github.com/GabrielHKGodinho/investment-engine/internal/postgres"
	"github.com/GabrielHKGodinho/investment-engine/internal/rabbitmq"
	"github.com/GabrielHKGodinho/investment-engine/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const (
	// defaultPort is used when the platform does not inject PORT, as with
	// `go run` or docker compose.
	defaultPort = "8080"

	// metricsAddr serves /metrics on a separate, internal-only listener, as in
	// the consumer (9101) and the price service (9102): the public port only
	// exposes the API itself.
	metricsAddr = ":9100"

	// shutdownTimeout must stay below the platform's grace period
	// (docker stop waits 10s by default before sending SIGKILL).
	shutdownTimeout = 8 * time.Second
)

func main() {
	if err := run(); err != nil {
		slog.Error("api stopped", "error", err.Error())
		os.Exit(1)
	}
	slog.Info("api stopped")
}

func run() error {
	slog.SetDefault(logging.New("api"))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Once the first signal cancels ctx, restore the default signal behavior,
	// so a second Ctrl+C kills the process immediately if shutdown gets stuck.
	context.AfterFunc(ctx, stop)

	shutdownTracing, err := telemetry.Init(ctx, "api")
	if err != nil {
		return fmt.Errorf("api: telemetry setup: %w", err)
	}
	defer func() {
		// ctx is already canceled when shutdown starts, so flush with a fresh one.
		flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := shutdownTracing(flushCtx); err != nil {
			slog.Error("telemetry shutdown failed", "error", err.Error())
		}
	}()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL environment variable is required")
	}
	db, err := postgres.Connect(context.Background(), dsn)
	if err != nil {
		return fmt.Errorf("api: database setup: %w", err)
	}
	defer db.Close()
	slog.Info("connected to database")

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		return errors.New("RABBITMQ_URL environment variable is required")
	}
	conn, err := rabbitmq.Dial(rabbitURL)
	if err != nil {
		return fmt.Errorf("api: rabbitmq setup: %w", err)
	}
	defer conn.Close()
	slog.Info("connected to rabbitmq")

	// Registered right after Dial so that no connection loss goes unnoticed.
	connClosed := conn.NotifyClose()

	setupChannel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("api: order messaging setup: %w", err)
	}
	err = order.SetupMessaging(setupChannel)
	_ = setupChannel.Close() // the setup channel is closed on both paths
	if err != nil {
		return fmt.Errorf("api: order messaging setup: %w", err)
	}

	store := order.NewPostgresOrderStore(db)
	publisher := order.NewPublisher(conn)
	handler := order.NewHandler(store, publisher)

	// Like in the consumer, the metrics server holds no state between scrapes,
	// so it does not take part in graceful shutdown.
	go func() {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("GET /metrics", promhttp.Handler())
		if err := http.ListenAndServe(metricsAddr, metricsMux); err != nil {
			slog.Error("metrics server failed", "error", err.Error())
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders", handler.ListOrders)
	mux.HandleFunc("POST /orders", handler.CreateOrder)

	// Twelve-factor port binding: the platform decides the port.
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}
	listenAddr := ":" + port

	srv := &http.Server{
		Addr: listenAddr,
		Handler: otelhttp.NewHandler(metrics.Middleware(mux), "api",
			otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
				return r.Method + " " + r.URL.Path
			}),
		),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1) // buffered: the goroutine can always send and exit
	go func() {
		slog.Info("api listening", "addr", listenAddr)
		serverErr <- srv.ListenAndServe()
	}()

	// stopErr stays nil for a requested shutdown and makes run() fail when the
	// process stops because a dependency was lost.
	var stopErr error

	select {
	case err := <-serverErr:
		// The server failed by itself (e.g. port already in use).
		return fmt.Errorf("api: server failed: %w", err)
	case amqpErr := <-connClosed:
		// Without the broker every new order would be saved but its event never
		// published (ADR-007), leaving it PENDING forever. Exiting with an error
		// lets the platform restart the process, which reconnects on startup.
		stopErr = errors.New("api: rabbitmq connection lost")
		if amqpErr != nil {
			stopErr = fmt.Errorf("api: rabbitmq connection lost: %w", amqpErr)
		}
		slog.Error("rabbitmq connection lost: draining in-flight requests before exiting",
			"error", stopErr.Error())
	case <-ctx.Done():
		slog.Info("shutdown requested: draining in-flight requests")
	}

	// ctx is already canceled at this point, so it can't be the parent here.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// Shutdown runs here, in the body of run(), so it finishes before any
	// of the deferred closes above.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("api: graceful shutdown failed: %w", err)
	}

	return stopErr
}
