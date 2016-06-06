package rabbit

import (
	"github.com/streadway/amqp"
	"log"
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

	//handler := handlers[subscriber.RoutingKey]

	go func() {
		for message := range messages {
			log.Printf(" [x] %s", message.Body)
			//handler(message.Body)
		}
	}()
}
