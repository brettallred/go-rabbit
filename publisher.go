package rabbit

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"time"
)

var publisherConnection = &Connection{}

//Publisher allows you to publish events to RabbitMQ
type Publisher struct {
	_channel          *amqp.Channel
	_notifyPublish    []chan amqp.Confirmation
	_reliableMode     bool
	_reliableModeWait bool
}

// NewPublisher constructs a new Publisher instance.
func NewPublisher() *Publisher {
	return &Publisher{}
}

// GetChannel returns a publisher's channel. It opens a new channel if needed.
func (p *Publisher) GetChannel() *amqp.Channel {
	if p._channel != nil {
		return p._channel
	}
	for {
		if err := p.openChannel(); err == nil {
			break
		} else {
			logError(err, "Can't open a new RabbitMQ channel for publisher")
		}
		time.Sleep(1 * time.Second)
	}
	return p._channel
}

func (p *Publisher) openChannel() error {
	c := publisherConnection.GetConnection()
	p._channel = createChannel(c, false)
	if p._channel == nil {
		log.Printf("Can't create a RabbitMQ channel for publisher")
		return errors.New("Can't create channel for publisher")
	}
	for i := range p._notifyPublish {
		p._channel.NotifyPublish(p._notifyPublish[i])
	}
	if p._reliableMode {
		if err := p._channel.Confirm(p._reliableModeWait); err != nil {
			p.Close()
			log.Printf("Error on enabling RabbitMQ confirmation on reconnecting: %s", err.Error())
			return err
		}
	}

	return nil
}

// Confirm enables reliable mode for the publisher.
func (p *Publisher) Confirm(wait bool) error {
	p._reliableMode = true
	p._reliableModeWait = wait
	channel := p.GetChannel()
	err := channel.Confirm(wait)
	if err != nil {
		log.Printf("Error on enabling RabbitMQ confirmation (will disconnect publisher): %s", err.Error())
		p.Close()
	}
	return err
}

// NotifyPublish registers a listener for reliable publishing.
func (p *Publisher) NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation {
	channel := p.GetChannel()
	p._notifyPublish = append(p._notifyPublish, c)
	channel.NotifyPublish(c)
	return c
}

// Publish pushes items on to a RabbitMQ Queue.
func (p *Publisher) Publish(message string, subscriber *Subscriber) error {
	return p.PublishBytes([]byte(message), subscriber)
}

// PublishBytes is the same as Publish but accepts a []byte instead of a string
func (p *Publisher) PublishBytes(message []byte, subscriber *Subscriber) error {
	channel := p.GetChannel()

	return channel.Publish(
		subscriber.Exchange,   // exchange
		subscriber.RoutingKey, // routing key
		false, // mandatoryy
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         message,
			DeliveryMode: amqp.Persistent,
		})
}

//Close will close the connection and channel for the Publisher
func (p *Publisher) Close() {
	if p._channel != nil {
		p._channel.Close()
		p._channel = nil
	}
}
