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

	// Once the first signal cancels ctx, restore the default signal behavior,
	// so a second Ctrl+C kills the process immediately if shutdown gets stuck.
	context.AfterFunc(ctx, stop)

	log.Println("consumer starting")

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

	consumer := execution.NewConsumer(conn)
	if err := consumer.Run(ctx); err != nil {
		return fmt.Errorf("consumer returned an error while running: %w", err)
	}

	return nil
}
