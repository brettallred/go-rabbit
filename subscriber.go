package rabbit

import (
	"log"
)

var (
	subscribers map[string]Subscriber
	handlers    map[string]func(b []byte) bool
)

type Subscriber struct {
	AutoAck     bool
	Concurrency int
	Durable     bool
	Exchange    string
	Queue       string
	RoutingKey  string
}

func StartSubscribers() {
	if connection == nil {
		connection = connect()
		defer connection.Close()
	}

	for _, subscriber := range subscribers {
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

// Adds a subscriber to the subscribers pool
func RegisterSubscriber(s Subscriber, handler func(b []byte) bool) {
	if subscribers == nil {
		subscribers = make(map[string]Subscriber)
		handlers = make(map[string]func(b []byte) bool)
	}

	if handlers == nil {
		handlers = make(map[string]func(b []byte) bool)
	}

	subscribers[s.RoutingKey] = s
	handlers[s.RoutingKey] = handler
}

func Subscribers() map[string]Subscriber {
	return subscribers
}
