package rabbit

import (
	"github.com/streadway/amqp"
)

func createExchange(channel *amqp.Channel, subscriber *Subscriber) error {
	err := channel.ExchangeDeclare(
		subscriber.Exchange,
		"topic", // type
		false,   // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare an exchange")
	return err
}
