package rabbit

import (
	"errors"
	"log"
	"sync"
)

var (
	// Subscribers is a map of all of the registered Subscribers
	Subscribers map[string]Subscriber
	// Handlers is a map of all of the registered Subscriber Handlers
	Handlers           map[string]func(b []byte) bool
	subscribersStarted = false
	lock               sync.RWMutex
)

// Subscriber contains all of the necessary data for Publishing and Subscriber to RabbitMQ Topics
type Subscriber struct {
	Concurrency   int
	Durable       bool
	Exchange      string
	Queue         string
	RoutingKey    string
	PrefetchCount int
	AutoDelete    bool
}

// StartSubscribers spins up all of the registered Subscribers and consumes messages on their
// respective queues.
func StartSubscribers() error {
	conn := connection()
	if conn == nil {
		errorMessage := "Can't start subscribers: no connection"
		log.Printf(errorMessage)
		return errors.New(errorMessage)
	}

	subscribersStarted = true
	for _, subscriber := range Subscribers {
		for i := 0; i < subscriber.Concurrency; i++ {
			log.Printf(`Starting subscriber
		                Durable:    %t
		                Exchange:   %s
		                Queue:      %s
		                RoutingKey: %s
		                AutoDelete: %v
		                `,
				subscriber.Durable,
				subscriber.Exchange,
				subscriber.Queue,
				subscriber.RoutingKey,
				subscriber.AutoDelete,
			)

			channel := createChannel(conn)
			if channel == nil {
				return errors.New("Failed to start subscriber: can't create a channel")
			}
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
	lock.Lock()
	defer lock.Unlock()
	subscribersStarted = false
	Subscribers = nil
	Handlers = nil
	if _connection != nil {
		c := _connection
		_connection = nil
		go c.Close()
	}
}

func DeleteQueue(s Subscriber) error {
	conn := connection()
	if conn == nil {
		errorMessage := "Can't delete queue: no connection"
		log.Printf(errorMessage)
		return errors.New(errorMessage)
	}

	channel := createChannel(conn)
	if channel == nil {
		return errors.New("Can't delete a queue: can't create a channel")
	}
	return deleteQueue(channel, &s)
}
