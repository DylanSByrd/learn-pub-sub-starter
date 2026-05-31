package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const connectionStr = "amqp://guest:guest@localhost:5672/"

	fmt.Println("Starting Peril server...")
	conn, err := amqp.Dial(connectionStr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to create message channel: %v", err)
	}

	const gameLogsRoutingKey = routing.GameLogSlug + ".*"
	err = pubsub.SubscribeGob(conn, routing.ExchangePerilTopic, routing.GameLogSlug, gameLogsRoutingKey, pubsub.SimpleQueueDurable, handlerLog())
	if err != nil {
		log.Fatalf("Could not bind game_logs queue: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		command := strings.ToLower(input[0])
		switch command {
		case "pause":
			fmt.Println("...Pausing...")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			if err != nil {
				log.Printf("Failed to pause: %v", err)
			}
		case "resume":
			fmt.Println("Resuming!")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			if err != nil {
				log.Printf("Failed to resume: %v", err)
			}
		case "quit":
			fmt.Println("Shutting down server...")
			return
		default:
			fmt.Println("Command not recognized")
		}
	}
}
