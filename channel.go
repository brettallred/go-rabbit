package rabbit

import (
	"github.com/streadway/amqp"
	"log"
)

func createChannel(conn *amqp.Connection) *amqp.Channel {
	if conn == nil {
		log.Printf("Failed to open a channel: no connection")
		return nil
	}
	channel, err := conn.Channel()
	if err != nil {
		logError(err, "Failed to open a channel")
		return nil
	}
	return channel
}
