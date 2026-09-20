package main

import (
	"log"
	"os"

	"github.com/GabrielHKGodinho/investment-engine/internal/execution"
	"github.com/GabrielHKGodinho/investment-engine/internal/rabbitmq"
)

func main() {
	log.Println("consumer starting")

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		log.Fatal("RABBITMQ_URL environment variable is required")
	}
	conn, err := rabbitmq.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("failed to connect to rabbitmq: %v", err)
	}
	defer conn.Close()
	log.Println("connected to rabbitmq")

	consumer := execution.NewConsumer(conn)
	err = consumer.Run()
	if err != nil {
		log.Fatalf("consumer returned an error while running: %v", err)
	}

}
