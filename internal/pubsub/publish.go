package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TODO: extract body into shared publish func
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
		return fmt.Errorf("Error publishing json to %v: vw", exchange, err)
	}

	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buff bytes.Buffer
	encoder := gob.NewEncoder(&buff)
	err := encoder.Encode(val)
	if err != nil {
		return fmt.Errorf("Failed to encode data to gob while publishing to %v: %v", exchange, err)
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body: buff.Bytes(),
	})
	if err != nil {
		return fmt.Errorf("Error publishing gob to %v: %v", exchange, err)
	}

	return nil
}
