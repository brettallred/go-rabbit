package rabbit

import (
	"errors"
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
	Concurrency   int
	Durable       bool
	Exchange      string
	Queue         string
	RoutingKey    string
	PrefetchCount int
}

// StartSubscribers spins up all of the registered Subscribers and consumes messages on their
// respective queues.
func StartSubscribers() error {
	if connection == nil {
		connect()
	}
	if connection == nil {
		errorMessage := "Can't start subscribers: no connection"
		log.Printf(errorMessage)
		return errors.New(errorMessage)
	}

	for _, subscriber := range Subscribers {
		log.Printf(`Starting subscriber
		Durable:    %t
		Exchange:   %s
		Queue:      %s
		RoutingKey: %s
		`,
			subscriber.Durable,
			subscriber.Exchange,
			subscriber.Queue,
			subscriber.RoutingKey,
		)

		channel := createChannel(connection)
		if err := createExchange(channel, &subscriber); err != nil {
			log.Printf("Failed to start subscriber: %v", err.Error())
			return err
		}
		if _, err := createQueue(channel, &subscriber); err != nil {
			log.Printf("Failed to start subscriber: %v", err.Error())
			return err
		}
		if err := bindQueue(channel, &subscriber); err != nil {
			log.Printf("Failed to start subscriber: %v", err.Error())
			return err
		}
		if err := createConsumer(channel, &subscriber); err != nil {
			log.Printf("Failed to start subscriber: %v", err.Error())
			return err
		}
	}
	return nil
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

func CloseSubscribers() {
	if connection != nil {
		connection.Close()
	}
}
