package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func publishGameLog(channel *amqp.Channel, player, logMsg string) error {
	routingKey := routing.GameLogSlug + "." + player
	err := pubsub.PublishGob(channel, routing.ExchangePerilTopic, routingKey, routing.GameLog{
		CurrentTime: time.Now(),
		Message: logMsg,
		Username: player,
	})
	return err
}


func main() {
	const connectionStr = "amqp://guest:guest@localhost:5672/"

	fmt.Println("Starting Peril client...")
	conn, err := amqp.Dial(connectionStr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	fmt.Println("Peril game client connected to RabbitMQ!")

	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to create message channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Failed to create username: %v", err)
	}

	gs := gamelogic.NewGameState(username)
	pauseQueueName := routing.PauseKey + "." + username
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, pauseQueueName, routing.PauseKey, pubsub.SimpleQueueTransient, handlerPause(gs))
	if err != nil {
		log.Fatalf("Could not subscribe to pause queue: %v", err)
	}

	const moveRoutingKey = routing.ArmyMovesPrefix + ".*"
	moveQueueName := routing.ArmyMovesPrefix + "." + username
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, moveQueueName, moveRoutingKey, pubsub.SimpleQueueTransient, handlerMove(gs, channel))
	if err != nil {
		log.Fatalf("Could not subscribe to move queue: %v", err)
	}

	warRoutingKey := routing.WarRecognitionsPrefix + ".*"
	const warQueueName = routing.WarRecognitionsPrefix
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, warQueueName, warRoutingKey, pubsub.SimpleQueueDurable, handlerWar(gs, channel))
	if err != nil {
		log.Fatalf("Could not subscribe to war queue: %v", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		command := strings.ToLower(input[0])
		switch command {
		case "spawn":
			err := gs.CommandSpawn(input)
			if err != nil {
				fmt.Printf("spawn error: %v\n", err)
			}
		case "move":
			move, err := gs.CommandMove(input)
			if err != nil {
				fmt.Printf("move error: %v\n", err)
				continue
			}
			
			err = pubsub.PublishJSON(channel, routing.ExchangePerilTopic, moveRoutingKey, move) 
			if err != nil {
				fmt.Printf("move publish error: %v\n", err)
			}
			fmt.Println("Moved %v units to %s\n", len(move.Units), move.ToLocation)
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(input) < 2 {
				fmt.Println("Please provide a number of messages to spam")
				continue
			}

			num, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Printf("Error parsing spam count: %v\n", err)
				continue
			}

			for range(num) {
				spamLog := gamelogic.GetMaliciousLog()
				err = publishGameLog(channel, username, spamLog) 
				if err != nil {
					fmt.Printf("spam log publish error: %v\n", err)
				}
			}
			fmt.Printf("Published %v malicious logs\n", num)
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Command not recognized")
		}
	}
}
