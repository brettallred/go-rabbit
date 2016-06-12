package rabbit

import (
	"github.com/streadway/amqp"
)

var publishingConnection *amqp.Connection
var publishingChannel *amqp.Channel

func InitPublisher() {
	if publishingConnection == nil {
		publishingConnection = connect()
	}

	if publishingChannel == nil {
		publishingChannel = createChannel(publishingConnection)
	}
}

func Publish(message string, subscriber *Subscriber) {
	PublishBytes([]byte(message), subscriber)
}

func PublishBytes(message []byte, subscriber *Subscriber) {
	InitPublisher()

	publishingChannel.Publish(
		subscriber.Exchange,   // exchange
		subscriber.RoutingKey, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		})
}
