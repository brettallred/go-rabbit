package rabbit

import (
	"github.com/streadway/amqp"
)

var popConnection *amqp.Connection
var popChannel *amqp.Channel

func InitPop() {
	if popConnection == nil {
		popConnection = connect()
	}

	if popChannel == nil {
		popChannel = createChannel(popConnection)
	}
}

func Pop(subscriber *Subscriber) string {
	InitPop()

	message, _, err := popChannel.Get(
		subscriber.Queue, // queue
		true,             // auto ack
	)
	failOnError(err, "Failed while consumeing message")

	return string(message.Body)
}
