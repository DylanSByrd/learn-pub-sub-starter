package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Failed to marshal json while publishing to %v: %v", exchange, err)
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, /*mandatory*/ false, /*immediate*/ false, amqp.Publishing{
		ContentType: "application/json",
		Body: data,
	})
	if err != nil {
		return fmt.Errorf("Error publishing json to %v: %v", exchange, err)
	}

	return nil
}

