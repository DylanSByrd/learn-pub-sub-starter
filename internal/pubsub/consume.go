package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType uint
const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func DeclareAndBind(conn* amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (channel *amqp.Channel, queue amqp.Queue, err error) {
	if conn == nil {
		return nil, amqp.Queue{}, errors.New("Cannot bind to a nil connection")
	}

	channel, err = conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Failed to create new channel: %v", err)
	}
	defer func() {
		if err != nil {
			channel.Close()
		}
	}()

	isDurable := queueType == SimpleQueueDurable
	shouldAutoDelete := !isDurable
	isExclusive := !isDurable
	queue, err = channel.QueueDeclare(queueName, isDurable, shouldAutoDelete, isExclusive, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Failed to declare queue %v: %v", queueName, err)
	}

	err = channel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Failed to bind queue %v: %v", queueName, err)
	}

	fmt.Printf("Queue %v declared and bound!\n", queueName)
	return channel, queue, nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T)) (err error) {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("Failed to subscribe to %v: %v", queueName, err)
	}

	deliveryChan, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Failed to subscribe to %v: %v", queueName, err)
	}

	go func() {
		defer channel.Close()
		for msg := range deliveryChan {
			var data T
			if err = json.Unmarshal(msg.Body, &data); err != nil {
				fmt.Printf("Error unmsrhalling json: %v\n", err)
				continue
			}
			handler(data)
			if err = msg.Ack(false); err != nil {
				fmt.Printf("Error ack'ing message: %v\n", err)
			}
		}
	}()

	return nil
}
