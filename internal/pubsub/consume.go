package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType uint
const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

type AckType uint
const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func DeclareAndBind(conn* amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (channel *amqp.Channel, queue amqp.Queue, err error) {
	if conn == nil {
		return nil, amqp.Queue{}, errors.New("Cannot bind to a nil connection")
	}

	channel, err = conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Failed to create new channel: %v", err)
	}

	args := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}

	isDurable := queueType == SimpleQueueDurable
	shouldAutoDelete := !isDurable
	isExclusive := !isDurable
	queue, err = channel.QueueDeclare(queueName, isDurable, shouldAutoDelete, isExclusive, false, args)
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

func subscribe[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType, unmarshaller func([]byte) (T, error)) (err error) {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("Failed to subscribe to %v: %v", queueName, err)
	}

	err = channel.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("Could not limit prefetch: %v", err)
	}

	deliveryChan, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Failed to subscribe to %v: %v", queueName, err)
	}

	go func() {
		defer channel.Close()
		for msg := range deliveryChan {
			data, err := unmarshaller(msg.Body) 
			if err != nil {
				fmt.Printf("Error unmarshalling data: %v\n", err)
				continue
			}

			ackType := handler(data)
			switch ackType {
			case Ack:
				msg.Ack(false)
				log.Println("Message ack'd.")
			case NackRequeue:
				msg.Nack(false, true)
				log.Println("Message n'ack'd and requeued.")
			case NackDiscard:
				msg.Nack(false, false)
				log.Println("Message n'ack'd and discarded.")
			}
		}
	}()

	return nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) (err error) {
	unmarshaller := func(data []byte) (T, error) {
		var val T
		err := json.Unmarshal(data, &val)
		return val, err
	}
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaller)
}

func SubscribeGob[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) (err error) {
	unmarshaller := func(data []byte) (T, error) {
		var val T
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		err := dec.Decode(&val)
		return val, err
	}
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaller)
}
