package rabbit

import (
	"github.com/streadway/amqp"
)

var publishingChannel *amqp.Channel

func InitPublisher() {
	if connection == nil {
		connection = connect()
	}

	publishingChannel = createChannel(connection)
}

func Publish(message string, subscriber *Subscriber) {
	InitPublisher()
	createExchange(publishingChannel, subscriber)

	publishingChannel.Publish(
		subscriber.Exchange,   // exchange
		subscriber.RoutingKey, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(message),
		})
}
