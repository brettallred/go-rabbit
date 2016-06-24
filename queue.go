package rabbit

import (
	"github.com/streadway/amqp"
)

func createQueue(channel *amqp.Channel, subscriber *Subscriber) (amqp.Queue, error) {
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
	return queue, err
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
