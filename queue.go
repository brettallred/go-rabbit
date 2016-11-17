package rabbit

import (
	"github.com/streadway/amqp"
)

func createQueue(channel *amqp.Channel, subscriber *Subscriber) error {
	_, err := channel.QueueDeclare(
		subscriber.Queue,      // name
		subscriber.Durable,    // durable
		subscriber.AutoDelete, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	return err
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
