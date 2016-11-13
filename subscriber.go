package rabbit

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/user"
	"sync"

	"github.com/streadway/amqp"
)

var (
	// Subscribers is a map of all of the registered Subscribers
	Subscribers map[string]Subscriber
	// Handlers is a map of all of the registered Subscriber Handlers
	Handlers           map[string]func(b []byte) bool
	subscribersStarted = false
	lock               sync.RWMutex
	nonDevEnvironments = []string{"production", "prod", "staging", "stage"}
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
	lock.Lock()
	defer lock.Unlock()
	conn := connectionWithoutLock()
	if conn == nil {
		errorMessage := "Can't start subscribers: no connection"
		log.Printf(errorMessage)
		return errors.New(errorMessage)
	}

	subscribersStarted = true
	return startSubscribers(conn)
}

func startSubscribers(conn *amqp.Connection) error {
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

			channel := createChannel(conn, true)
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

// CloseSubscribers removes all subscribers, handlers, and closes the amqp connection
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

//DeleteQueue does what it says, deletes a queue in rabbit
func DeleteQueue(s Subscriber) error {
	conn := connection()
	if conn == nil {
		errorMessage := "Can't delete queue: no connection"
		log.Printf(errorMessage)
		return errors.New(errorMessage)
	}

	channel := createChannel(conn, false)
	if channel == nil {
		return errors.New("Can't delete a queue: can't create a channel")
	}
	return deleteQueue(channel, &s)
}

// PrefixQueueInDev will prefix the queue name with the name of your current user if of the APP_ENV variable is set
// to a non production value ("production", "prod", "staging", "stage").
// This is used for running a worker in your local environment but connecting to a stage
// or prodution rabbit server.
func (s *Subscriber) PrefixQueueInDev() {
	env := appEnv()

	if !IsDevelopmentEnv() {
		return
	}

	username := currentUsersName()

	if env == "test" {
		username = "test_" + username
	}

	s.Queue = fmt.Sprintf("%s_%s", username, s.Queue)
}

// AutoDeleteInDev will set the Subscribers AutoDelete setting to true as long as you are in a development environement.
// Non production environements have a APP_ENV value that isn't ("production", "prod", "staging", "stage").
// This is used for running a worker in your local environment but connecting to a stage
// or prodution rabbit server. You want to ensure the Subscriber gets AutoDeleted on the remote server.
func (s *Subscriber) AutoDeleteInDev() {
	if IsDevelopmentEnv() {
		s.AutoDelete = true
	}
}

// IsDevelopmentEnv tells you if you are currently running in a development environment
func IsDevelopmentEnv() bool {
	env := appEnv()
	return !stringInSlice(env, nonDevEnvironments)
}

func appEnv() string {
	env := os.Getenv("APP_ENV")

	// Check PLATFORM_ENV for backwards compatibility
	if len(env) == 0 {
		env = os.Getenv("PLATFORM_ENV")
	}
	return env
}

func currentUsersName() string {
	username := "unknown"

	if userData, err := user.Current(); err == nil {
		username = userData.Username
	}

	return username
}
