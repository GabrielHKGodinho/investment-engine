package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/GabrielHKGodinho/investment-engine/internal/order"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("failed to open database connection: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to reach database: %v", err)
	}
	log.Println("connected to database")

	store := order.NewPostgresOrderStore(db)
	handler := order.NewHandler(store)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders", handler.ListOrders)

	addr := ":8080"
	log.Printf("api listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
