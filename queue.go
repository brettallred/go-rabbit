package rabbit

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
)

func createQueue(channel *amqp.Channel, subscriber *Subscriber) (*amqp.Queue, error) {
	if channel == nil {
		errorMessage := "Failed to declare a queue: no connection"
		log.Printf(errorMessage)
		return nil, errors.New(errorMessage)
	}
	queue, err := channel.QueueDeclare(
		subscriber.Queue,   // name
		subscriber.Durable, // durable
		false,              // delete when usused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		logError(err, "Failed to declare an queue")
	}
	return &queue, err
}

func bindQueue(channel *amqp.Channel, subscriber *Subscriber) error {
	err := channel.QueueBind(
		subscriber.Queue,      // queue name
		subscriber.RoutingKey, // routing key
		subscriber.Exchange,   // exchange
		false,
		nil)

	if err != nil {
		logError(err, "Failed to declare an queue")
		return err
	}
	return nil
}

func deleteQueue(channel *amqp.Channel, subscriber *Subscriber) error {
	_, err := channel.QueueDelete(subscriber.Queue, false, false, false)
	return err
}
