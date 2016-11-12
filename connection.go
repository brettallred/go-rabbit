package rabbit

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

func connection() *amqp.Connection {
	lock.Lock()
	defer lock.Unlock()
	return connectionWithoutLock()
}

func connectionWithoutLock() *amqp.Connection {
	if _connection == nil {
		connect()
	}
	return _connection
}

func connect() *amqp.Connection {
	var c *amqp.Connection
	var err error
	for {
		log.Printf("RabbitMQ: Dialing to %s", os.Getenv("RABBITMQ_URL"))
		c, err = amqp.Dial(os.Getenv("RABBITMQ_URL"))
		if err != nil {
			_connection = nil
			logError(err, "Failed to connect to RabbitMQ, we will redial")
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}

	_connection = c
	errorChannel := make(chan *amqp.Error)
	errorHandler := func(myConnection *amqp.Connection) {
		for {
			time.Sleep(100 * time.Millisecond)
			if myConnection != _connection {
				myConnection.Close()
				return
			}
			select {
			case <-errorChannel:
				lock.Lock()
				if _connection != nil {
					go log.Printf("RabbitMQ connection failed, we will redial")
					c := _connection
					_connection = nil
					defer c.Close()
					connectionWithoutLock()
					if _connection != nil && subscribersStarted {
						err := StartSubscribers()
						if err != nil {
							go log.Printf("Erron on subscribing to RabbitMQ: %s", err.Error())
							c := _connection
							_connection = nil
							defer c.Close()
						}
					}
				}
				lock.Unlock()
				return
			default:
				lock.RLock()
				if _connection == nil {
					lock.RUnlock()
					return
				}
				lock.RUnlock()
			}
		}
	}
	_connection.NotifyClose(errorChannel)
	go errorHandler(_connection)
	return _connection
}

// Connection represents an autorecovering connection
type Connection struct {
	connection *amqp.Connection
	lock       sync.RWMutex
}

// Close closes a connection
func (connection *Connection) Close() {
	connection.lock.Lock()
	defer connection.lock.Unlock()
	if connection.connection != nil {
		connection.connection.Close()
		connection.connection = nil
	}
}

// GetConnection returns an amqp.Connection stored in Connection. It establishes a new connection if needed.
func (connection *Connection) GetConnection() *amqp.Connection {
	connection.lock.Lock()
	defer connection.lock.Unlock()
	if connection.connection != nil {
		return connection.connection
	}
	for {
		if err := connection.connect(); err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	return connection.connection
}

func (connection *Connection) connect() error {
	connection.connection = nil
	c, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		logError(err, "Failed to connect to RabbitMQ")
		return err
	}

	connection.connection = c
	errorChannel := make(chan *amqp.Error)
	errorHandler := func() {
		for {
			select {
			case <-errorChannel:
				connection.lock.Lock()
				defer connection.lock.Unlock()
				if connection.connection != nil {
					go log.Printf("Error: RabbitMQ connection failed, we will redial")
					c := connection.connection
					connection.connection = nil
					if c != nil {
						go c.Close()
					}
				}
				return
			default:
				connection.lock.RLock()
				if connection.connection == nil {
					connection.lock.RUnlock()
					return
				}
				connection.lock.RUnlock()
				time.Sleep(1 * time.Second)
			}
		}
	}
	connection.connection.NotifyClose(errorChannel)
	go errorHandler()
	return nil
}
