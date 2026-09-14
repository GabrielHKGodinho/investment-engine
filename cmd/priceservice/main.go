package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/GabrielHKGodinho/investment-engine/internal/priceservice"
	pricepb "github.com/GabrielHKGodinho/investment-engine/internal/priceservice/pb"
)

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pricepb.RegisterPriceServiceServer(grpcServer, priceservice.NewServer())
	reflection.Register(grpcServer) // enables tools like grpcurl to discover the service

	log.Println("price service listening on :50051")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
