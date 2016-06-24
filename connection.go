package rabbit

import (
	"github.com/streadway/amqp"
	"os"
)

func connect() *amqp.Connection {
	connection, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		logError(err, "Failed to connect to RabbitMQ")
		return nil
	}
	return connection
}
