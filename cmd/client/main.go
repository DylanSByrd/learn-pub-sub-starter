package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const connectionStr = "amqp://guest:guest@localhost:5672/"

	fmt.Println("Starting Peril client...")
	conn, err := amqp.Dial(connectionStr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	fmt.Println("Peril game client connected to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Failed to create username: %v", err)
	}

	pauseQueue := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	_ , queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, pauseQueue, routing.PauseKey, pubsub.SimpleQueueTransient)
	if err != nil {
		log.Fatalf("Could not subscribe to pause: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)
	
	// Intercept ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down client...")
}
