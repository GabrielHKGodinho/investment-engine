package main

import (
	"context"
	"log"
	"net/http"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/GabrielHKGodinho/investment-engine/internal/order"
	"github.com/GabrielHKGodinho/investment-engine/internal/postgres"
	"github.com/GabrielHKGodinho/investment-engine/internal/rabbitmq"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	db, err := postgres.Connect(context.Background(), dsn)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()
	log.Println("connected to database")

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

	setupChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open rabbitmq setup channel: %v", err)
	}
	if err := order.SetupMessaging(setupChannel); err != nil {
		log.Fatalf("failed to set up order messaging: %v", err)
	}
	_ = setupChannel.Close()

	store := order.NewPostgresOrderStore(db)
	publisher := order.NewPublisher(conn)
	handler := order.NewHandler(store, publisher)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders", handler.ListOrders)
	mux.HandleFunc("POST /orders", handler.CreateOrder)

	addr := ":8080"
	log.Printf("api listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
