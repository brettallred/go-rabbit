package rabbit

import (
	"errors"
	"github.com/streadway/amqp"
)

type Publisher struct {
	publishingConnection *amqp.Connection
	publishingChannel    *amqp.Channel
}

// NewPublisher constructs a new Publisher instance.
func NewPublisher() *Publisher {
	return &Publisher{}
}

// init initializes the RabbitMQ Connection and Channel for Publishing messages.
func (p *Publisher) init() {
	if p.publishingConnection == nil {
		p.publishingConnection = connect()
	}

	if p.publishingChannel == nil && p.publishingConnection != nil {
		p.publishingChannel = createChannel(p.publishingConnection)
	}
}

// Confirm enables reliable mode for the publisher.
func (p *Publisher) Confirm(wait bool) error {
	p.init()
	if p.publishingChannel != nil {
		return p.publishingChannel.Confirm(wait)
	} else {
		return errors.New("Cannot enable reliable mode: no channel")
	}
}

// NotifyPublish registers a listener for reliable publishing.
func (p *Publisher) NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation {
	p.init()
	if p.publishingChannel != nil {
		return p.publishingChannel.NotifyPublish(c)
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

	if p.publishingChannel == nil {
		return errors.New("Can't publish: no publishingChannel")
	}

	return p.publishingChannel.Publish(
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
	if p.publishingConnection != nil {
		p.publishingConnection.Close()
		p.publishingConnection = nil
	}
}
