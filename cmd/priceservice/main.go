package main

import (
	"log/slog"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/GabrielHKGodinho/investment-engine/internal/logging"
	"github.com/GabrielHKGodinho/investment-engine/internal/priceservice"
	pricepb "github.com/GabrielHKGodinho/investment-engine/internal/priceservice/pb"
)

func main() {
	slog.SetDefault(logging.New("priceservice"))

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		slog.Error("failed to listen", "error", err.Error())
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	pricepb.RegisterPriceServiceServer(grpcServer, priceservice.NewServer())
	reflection.Register(grpcServer)

	slog.Info("price service listening", "addr", ":50051")
	if err := grpcServer.Serve(listener); err != nil {
		slog.Error("failed to serve", "error", err.Error())
		os.Exit(1)
	}
}
