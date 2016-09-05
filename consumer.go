package rabbit

import (
	"github.com/streadway/amqp"
)

func createConsumer(channel *amqp.Channel, subscriber *Subscriber) error {
	channel.Qos(subscriber.PrefetchCount, 0, false)

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
	if err != nil {
		return err
	}

	handler := Handlers[subscriber.RoutingKey]
	go func() {
		for message := range messages {
			ack := handler(message.Body)
			message.Ack(ack)
		}
	}()

	return nil
}
