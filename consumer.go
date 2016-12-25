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

	if err != nil {
		return err
	}

	handler := Handlers[subscriber.RoutingKey]

	go consumeMessages(messages, handler, subscriber.ManualAck)

	return nil
}

func consumeMessages(messages <-chan amqp.Delivery, handler func(*amqp.Delivery) bool, manualAck bool) {
	for message := range messages {
		result := handler(&message)
		if !manualAck {
			if result {
				message.Ack(false)
			} else {
				message.Nack(false, true)
			}
		}
	}
}
