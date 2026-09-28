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

	"github.com/GabrielHKGodinho/investment-engine/internal/execution"
	"github.com/GabrielHKGodinho/investment-engine/internal/logging"
	"github.com/GabrielHKGodinho/investment-engine/internal/order"
	"github.com/GabrielHKGodinho/investment-engine/internal/postgres"
	"github.com/GabrielHKGodinho/investment-engine/internal/priceservice"
	"github.com/GabrielHKGodinho/investment-engine/internal/rabbitmq"
	"github.com/GabrielHKGodinho/investment-engine/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	err := run()
	if err != nil {
		slog.Error("consumer stopped", "error", err.Error())
		os.Exit(1)
	}
	slog.Info("consumer stopped")
}

func run() error {
	slog.SetDefault(logging.New("consumer"))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	context.AfterFunc(ctx, stop)

	slog.Info("consumer starting")

	shutdownTracing, err := telemetry.Init(ctx, "consumer")
	if err != nil {
		return fmt.Errorf("consumer: telemetry setup: %w", err)
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
		return fmt.Errorf("consumer: database setup: %w", err)
	}
	defer db.Close()
	slog.Info("connected to database")

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		return errors.New("RABBITMQ_URL environment variable is required")
	}
	conn, err := rabbitmq.Dial(rabbitURL)
	if err != nil {
		return fmt.Errorf("consumer failed to dial: %w", err)
	}
	defer conn.Close()
	slog.Info("connected to rabbitmq")

	priceServiceAddr := os.Getenv("PRICE_SERVICE_ADDR")
	if priceServiceAddr == "" {
		return errors.New("PRICE_SERVICE_ADDR environment variable is required")
	}
	priceClient, err := priceservice.NewClient(priceServiceAddr)
	if err != nil {
		return fmt.Errorf("consumer: price service client setup: %w", err)
	}
	defer priceClient.Close()
	slog.Info("price service client ready")

	go func() {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.Handler())
		if err := http.ListenAndServe(":9101", mux); err != nil {
			slog.Error("metrics server failed", "error", err.Error())
		}
	}()

	store := order.NewPostgresOrderStore(db)
	consumer := execution.NewConsumer(conn, store, priceClient)
	if err := consumer.Run(ctx); err != nil {
		return fmt.Errorf("consumer returned an error while running: %w", err)
	}

	return nil
}
