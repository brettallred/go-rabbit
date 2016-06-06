package rabbit

import (
	"github.com/streadway/amqp"
)

func createQueue(channel *amqp.Channel, subscriber *Subscriber) amqp.Queue {
	queue, err := channel.QueueDeclare(
		subscriber.Queue,   // name
		subscriber.Durable, // durable
		false,              // delete when usused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare an queue")
	return queue
}

func bindQueue(channel *amqp.Channel, subscriber *Subscriber) {
	err := channel.QueueBind(
		subscriber.Queue,      // queue name
		subscriber.RoutingKey, // routing key
		subscriber.Exchange,   // exchange
		false,
		nil)

	failOnError(err, "Failed to declare an queue")
}
