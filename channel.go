package rabbit

import (
	"github.com/streadway/amqp"
)

func createChannel(conn *amqp.Connection) *amqp.Channel {
	channel, err := conn.Channel()
	if err != nil {
		logError(err, "Failed to open a channel")
		return nil
	}
	return channel
}
