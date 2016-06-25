package rabbit

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

type Publisher struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

// NewPublisher constructs a new Publisher instance.
func NewPublisher() *Publisher {
	return &Publisher{}
}

// init initializes the RabbitMQ Connection and Channel for Publishing messages.
func (p *Publisher) init() {
	if p.connection == nil {
		p.connect()
	}

	if p.channel == nil && p.connection != nil {
		p.channel = createChannel(p.connection)
	}
}

func (p *Publisher) connect() error {
	p.connection = nil
	p.channel = nil
	c, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		logError(err, "Failed to connect to RabbitMQ")
		return err
	}

	p.connection = c
	errorChannel := make(chan *amqp.Error)
	errorHandler := func() {
		for {
			select {
			case <-errorChannel:
				if p.connection != nil {
					log.Printf("RabbitMQ connection failed, we will redial")
					p.connection = nil
					p.channel = nil
				}
				return
			default:
				if p.connection == nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	c.NotifyClose(errorChannel)
	go errorHandler()
	return nil
}

// Confirm enables reliable mode for the publisher.
func (p *Publisher) Confirm(wait bool) error {
	p.init()
	if p.channel != nil {
		return p.channel.Confirm(wait)
	} else {
		return errors.New("Cannot enable reliable mode: no channel")
	}
}

// NotifyPublish registers a listener for reliable publishing.
func (p *Publisher) NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation {
	p.init()
	if p.channel != nil {
		return p.channel.NotifyPublish(c)
	}
	return nil
}

// Publish pushes items on to a RabbitMQ Queue.
func (p *Publisher) Publish(message string, subscriber *Subscriber) error {
	return p.PublishBytes([]byte(message), subscriber)
}

// PublishBytes is the same as Publish but accepts a []byte instead of a string
func (p *Publisher) PublishBytes(message []byte, subscriber *Subscriber) error {
	p.init()

	if p.channel == nil {
		return errors.New("Can't publish: no publishingChannel")
	}

	return p.channel.Publish(
		subscriber.Exchange,   // exchange
		subscriber.RoutingKey, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         message,
			DeliveryMode: amqp.Transient,
		})
}

func (p *Publisher) Close() {
	if p.connection != nil {
		c := p.connection
		p.connection = nil
		p.channel = nil
		c.Close()
	}
}
