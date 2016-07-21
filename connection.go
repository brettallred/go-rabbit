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
	if _connection == nil {
		connect()
		if _connection != nil && subscribersStarted {
			err := StartSubscribers()
			if err != nil {
				c := _connection
				_connection = nil
				go c.Close()
			}
		}
	}
	return _connection
}

func connect() *amqp.Connection {
	c, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		_connection = nil
		logError(err, "Failed to connect to RabbitMQ")
		return nil
	}

	_connection = c
	errorChannel := make(chan *amqp.Error)
	errorHandler := func() {
		for {
			select {
			case <-errorChannel:
				lock.Lock()
				defer lock.Unlock()
				if _connection != nil {
					go log.Printf("RabbitMQ connection failed, we will redial")
					c := _connection
					_connection = nil
					go c.Close()
				}
				return
			default:
				lock.RLock()
				if _connection == nil {
					lock.RUnlock()
					return
				}
				lock.RUnlock()
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	_connection.NotifyClose(errorChannel)
	go errorHandler()
	return _connection
}
