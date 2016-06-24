package rabbit

import (
	"github.com/streadway/amqp"
)

var publishingConnection *amqp.Connection
var publishingChannel *amqp.Channel

// InitPublisher initializes the RabbitMQ Connection and Channel for Publishing messages.
func InitPublisher() {
	if publishingConnection == nil {
		publishingConnection = connect()
	}

	if publishingChannel == nil {
		publishingChannel = createChannel(publishingConnection)
	}
}

// InitPublisher reinitializes the RabbitMQ Connection and Channel for Publishing messages.
func ReInitPublisher() {
	publishingConnection = connect()
	publishingChannel = createChannel(publishingConnection)
}

// ConfirmPublish enables reliable mode for the publisher.
func ConfirmPublish(wait bool) error {
	InitPublisher()
	return publishingChannel.Confirm(wait)
}

// NotifyPublish registers a listener for reliable publishing.
func NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation {
	InitPublisher()
	return publishingChannel.NotifyPublish(c)
}

// Publish pushes items on to a RabbitMQ Queue.
func Publish(message string, subscriber *Subscriber) {
	PublishBytes([]byte(message), subscriber)
}

// PublishBytes is the same as Publish but accepts a []byte instead of a string
func PublishBytes(message []byte, subscriber *Subscriber) {
	InitPublisher()

	publishingChannel.Publish(
		subscriber.Exchange,   // exchange
		subscriber.RoutingKey, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         message,
			DeliveryMode: amqp.Transient,
		})
}
