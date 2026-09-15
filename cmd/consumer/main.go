package main

import (
	"log"
	"time"
)

func main() {
	// TODO(14/09): connect to RabbitMQ and start consuming from the
	// "order created" queue. For now this is a placeholder so the
	// binary compiles and runs as part of the system's skeleton.
	log.Println("consumer starting (not yet processing messages)")

	for {
		time.Sleep(time.Hour)
	}
}
