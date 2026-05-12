package pubsub

import (
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
	defer func() {
		if err != nil {
			channel.QueueDelete(queueName, true, true, true)
		}
	}()

	err = channel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Failed to bind queue %v: %v", queueName, err)
	}

	return channel, queue, nil
}
