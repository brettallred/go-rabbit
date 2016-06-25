package rabbit

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

func connect() *amqp.Connection {
	c, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		connection = nil
		logError(err, "Failed to connect to RabbitMQ")
		return nil
	}

	connection = c
	errorChannel := make(chan *amqp.Error)
	errorHandler := func() {
		for {
			select {
			case <-errorChannel:
				if connection != nil {
					log.Printf("RabbitMQ connection failed, we will redial")
					connection = nil
				}
				return
			default:
				if connection == nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	connection.NotifyClose(errorChannel)
	go errorHandler()
	return connection
}
