package rabbit

import (
	"github.com/streadway/amqp"
)

func createConsumer(channel *amqp.Channel, subscriber *Subscriber) {
	messages, err := channel.Consume(
		subscriber.Queue, // queue
		"",               // consumer
		false,            // auto ack
		false,            // exclusive
		false,            // no local
		false,            // no wait
		nil,              // args
	)
	logError(err, "Failed while trying to consume messages from channel")

	handler := Handlers[subscriber.RoutingKey]

	for i := 0; i < subscriber.Concurrency; i++ {
		go func(i int) {
			for message := range messages {
				ack := handler(message.Body)
				message.Ack(ack)
			}
		}(i)
	}
}
