package rabbit

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
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
