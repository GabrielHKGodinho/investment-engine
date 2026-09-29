package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/GabrielHKGodinho/investment-engine/internal/logging"
	"github.com/GabrielHKGodinho/investment-engine/internal/priceservice"
	pricepb "github.com/GabrielHKGodinho/investment-engine/internal/priceservice/pb"
	"github.com/GabrielHKGodinho/investment-engine/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	slog.SetDefault(logging.New("priceservice"))

	// TODO: this binary has no graceful shutdown (unlike api and consumer).
	// On SIGTERM, Go's default disposition kills the process immediately,
	// so this deferred flush never runs and buffered spans are lost.
	shutdownTracing, err := telemetry.Init(context.Background(), "priceservice")
	if err != nil {
		slog.Error("telemetry setup failed", "error", err.Error())
		os.Exit(1)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := shutdownTracing(flushCtx); err != nil {
			slog.Error("telemetry shutdown failed", "error", err.Error())
		}
	}()

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		slog.Error("failed to listen", "error", err.Error())
		os.Exit(1)
	}

	grpcServer := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	pricepb.RegisterPriceServiceServer(grpcServer, priceservice.NewServer())
	reflection.Register(grpcServer)

	go func() {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.Handler())
		if err := http.ListenAndServe(":9102", mux); err != nil {
			slog.Error("metrics server failed", "error", err.Error())
		}
	}()

	slog.Info("price service listening", "addr", ":50051")
	if err := grpcServer.Serve(listener); err != nil {
		slog.Error("failed to serve", "error", err.Error())
		os.Exit(1)
	}
}
