package rabbit

import (
	"log"

	"github.com/streadway/amqp"
)

func createConsumer(channel *amqp.Channel, subscriber *Subscriber) {
	messages, err := channel.Consume(
		subscriber.Queue,   // queue
		"",                 // consumer
		subscriber.AutoAck, // auto ack
		false,              // exclusive
		false,              // no local
		false,              // no wait
		nil,                // args
	)
	failOnError(err, "Failed to declare an exchange")

	handler := handlers[subscriber.RoutingKey]

	for i := 0; i < subscriber.Concurrency; i++ {
		go func(i int) {
			log.Printf("Consumer #%d started for %s", i, subscriber.Queue)
			for message := range messages {
				log.Printf("Consumer # %d for %s received %s", i, subscriber.Queue, message.Body)
				ack := handler(message.Body)
				message.Ack(ack)
			}
		}(i)
	}
}
