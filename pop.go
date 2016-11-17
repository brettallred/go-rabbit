package rabbit

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
)

var popConnection *amqp.Connection
var popChannel *amqp.Channel

// InitPop intializes the RabbitMQ Connection and Channel for popping messages off of a queue.
func InitPop() {
	if popConnection == nil {
		popConnection = connect()
	}

	if popChannel == nil {
		popChannel, _ = popConnection.Channel()
	}
}

// Pop returns a single item from a RabbitMQ queue. It uses the Subscriber to know which
// queue to pop the item off.  This is currently a helper function for the tests so you can
// pop a message off the queue and test it.
func Pop(subscriber *Subscriber) (string, error) {
	InitPop()

	if popChannel == nil {
		errorMessage := "Can't consume message: no channel"
		log.Printf(errorMessage)
		return "", errors.New(errorMessage)
	}

	createQueue(popChannel, subscriber)
	bindQueue(popChannel, subscriber)

	message, _, err := popChannel.Get(
		subscriber.Queue, // queue
		true,             // auto ack
	)
	logError(err, "Failed while consuming message")

	if err == nil {
		return string(message.Body), nil
	}
	return "", err
}
