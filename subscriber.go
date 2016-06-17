package rabbit

import (
	"log"
)

var (
	// Subscribers is a map of all of the registered Subscribers
	Subscribers map[string]Subscriber
	// Handlers is a map of all of the registered Subscriber Handlers
	Handlers map[string]func(b []byte) bool
)

// Subscriber contains all of the necessary data for Publishing and Subscriber to RabbitMQ Topics
type Subscriber struct {
	AutoAck     bool
	Concurrency int
	Durable     bool
	Exchange    string
	Queue       string
	RoutingKey  string
}

// StartSubscribers spins up all of the registered Subscribers and consumes messages on their
// respective queues.
func StartSubscribers() {
	if connection == nil {
		connection = connect()
		defer connection.Close()
	}

	for _, subscriber := range Subscribers {
		log.Printf(`Starting subscriber
		AutoAck:    %t
		Durable:    %t 
		Exchange:   %s 
		Queue:      %s 
		RoutingKey: %s 
		`,
			subscriber.AutoAck,
			subscriber.Durable,
			subscriber.Exchange,
			subscriber.Queue,
			subscriber.RoutingKey,
		)

		channel := createChannel(connection)
		createExchange(channel, &subscriber)
		createQueue(channel, &subscriber)
		bindQueue(channel, &subscriber)
		createConsumer(channel, &subscriber)
	}

	forever := make(chan bool)
	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}

// Register adds a subscriber and handler to the subscribers pool
func Register(s Subscriber, handler func(b []byte) bool) {
	if Subscribers == nil {
		Subscribers = make(map[string]Subscriber)
		Handlers = make(map[string]func(b []byte) bool)
	}

	if Handlers == nil {
		Handlers = make(map[string]func(b []byte) bool)
	}

	Subscribers[s.RoutingKey] = s
	Handlers[s.RoutingKey] = handler
}
