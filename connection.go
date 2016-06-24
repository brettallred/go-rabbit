package rabbit

import (
	"github.com/streadway/amqp"
	"os"
)

func connect() *amqp.Connection {
	c, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		connection = nil
		logError(err, "Failed to connect to RabbitMQ")
		return nil
	}
	connection = c
	return connection
}
