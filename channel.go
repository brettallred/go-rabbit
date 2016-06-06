package rabbit

import (
	"github.com/streadway/amqp"
)

func createChannel(conn *amqp.Connection) *amqp.Channel {
	channel, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	return channel
}
