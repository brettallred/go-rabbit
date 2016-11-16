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

func handleConnectionError(myConnection *amqp.Connection) {
	log.Printf("RabbitMQ connection failed, we will redial")
	lock.Lock()
	if myConnection == _connection {
		_connection = nil
	}
	myConnection.Close()
	if _connection == nil {
		connectionWithoutLock()
		if _connection != nil && subscribersStarted {
			err := StartSubscribers()
			if err != nil {
				log.Printf("Erron on subscribing to RabbitMQ: %s", err.Error())
				c := _connection
				defer c.Close()
			}
		}
	}
	lock.Unlock()
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
	var errorChannel chan *amqp.Error
	errorHandler := func(myConnection *amqp.Connection) {
		select {
		case <-errorChannel:
			handleConnectionError(myConnection)
			return
		}
	}
	errorChannel = _connection.NotifyClose(make(chan *amqp.Error, 1))
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
	connection.connect()
	return connection.connection
}

func handlePublisherConnectionError(connection *Connection, myConnection *amqp.Connection) {
	log.Printf("RabbitMQ Publisher's connection failed, we will redial")
	defer myConnection.Close()
	connection.lock.Lock()
	defer connection.lock.Unlock()
	if myConnection == connection.connection {
		connection.connection = nil
	}
	if connection.connection == nil {
		connection.connect()
	}
}

func (connection *Connection) connect() {
	connection.connection = nil
	var c *amqp.Connection
	var err error
	for {
		log.Printf("Creating a new RabbitMQ connection for publisher")
		c, err = amqp.Dial(os.Getenv("RABBITMQ_URL"))
		if err != nil {
			connection.connection = nil
			logError(err, "Failed to connect to RabbitMQ")
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
	log.Printf("RabbitMQ publisher's connection created")

	connection.connection = c
	errorChannel := make(chan *amqp.Error)
	errorHandler := func(myConnection *amqp.Connection) {
		for {
			select {
			case <-errorChannel:
				handlePublisherConnectionError(connection, c)
				return
			}
		}
	}
	connection.connection.NotifyClose(errorChannel)
	go errorHandler(c)
}
