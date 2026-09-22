package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/GabrielHKGodinho/investment-engine/internal/execution"
	"github.com/GabrielHKGodinho/investment-engine/internal/order"
	"github.com/GabrielHKGodinho/investment-engine/internal/postgres"
	"github.com/GabrielHKGodinho/investment-engine/internal/priceservice"
	"github.com/GabrielHKGodinho/investment-engine/internal/rabbitmq"
)

func main() {
	err := run()
	if err != nil {
		log.Fatalf("consumer stopped with error: %v", err)
	}
	log.Println("consumer stopped")
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	context.AfterFunc(ctx, stop)

	log.Println("consumer starting")

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL environment variable is required")
	}
	db, err := postgres.Connect(context.Background(), dsn)
	if err != nil {
		return fmt.Errorf("consumer: database setup: %w", err)
	}
	defer db.Close()
	log.Println("connected to database")

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		return errors.New("RABBITMQ_URL environment variable is required")
	}
	conn, err := rabbitmq.Dial(rabbitURL)
	if err != nil {
		return fmt.Errorf("consumer failed to dial: %w", err)
	}
	defer conn.Close()
	log.Println("connected to rabbitmq")

	priceServiceAddr := os.Getenv("PRICE_SERVICE_ADDR")
	if priceServiceAddr == "" {
		return errors.New("PRICE_SERVICE_ADDR environment variable is required")
	}
	priceClient, err := priceservice.NewClient(priceServiceAddr)
	if err != nil {
		return fmt.Errorf("consumer: price service client setup: %w", err)
	}
	defer priceClient.Close()
	log.Println("price service client ready")

	store := order.NewPostgresOrderStore(db)
	consumer := execution.NewConsumer(conn, store, priceClient)
	if err := consumer.Run(ctx); err != nil {
		return fmt.Errorf("consumer returned an error while running: %w", err)
	}

	return nil
}
