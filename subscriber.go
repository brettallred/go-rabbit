package rabbit

import (
	"log"
)

var (
	subscribers map[string]Subscriber
	handlers    map[string]func(s string)
)

type Subscriber struct {
	AutoAck    bool
	Durable    bool
	Exchange   string
	Queue      string
	RoutingKey string
}

func StartSubscribers() {
	// create a Connection, Ensure they close
	connection := connect()
	defer connection.Close()

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
func RegisterSubscriber(s Subscriber, handler func(s string)) {
	if subscribers == nil {
		subscribers = make(map[string]Subscriber)
		handlers = make(map[string]func(s string))
	}

	if handlers == nil {
		handlers = make(map[string]func(s string))
	}

	subscribers[s.RoutingKey] = s
	handlers[s.RoutingKey] = handler
}

func Subscribers() map[string]Subscriber {
	return subscribers
}
