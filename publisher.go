package rabbit

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

var (
	publisherConnection = &Connection{}
)

//Publisher allows you to publish events to RabbitMQ
type Publisher struct {
	connection        *Connection
	_channel          *amqp.Channel
	_notifyPublish    []notifyPublishSpec
	_reliableMode     bool
	_reliableModeWait bool
	lock              sync.Mutex
}

type notifyPublishSpec struct {
	channel chan amqp.Confirmation
	size    int
}

// NewPublisher constructs a new Publisher instance.
func NewPublisher() *Publisher {
	return &Publisher{connection: publisherConnection}
}

// NewPublisherWithConnection constructs a new Publisher instance with a custom connection
func NewPublisherWithConnection(connection *Connection) *Publisher {
	return &Publisher{connection: connection}
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
			p.Close()
		}
		time.Sleep(1 * time.Second)
	}
	return p._channel
}

func (p *Publisher) openChannel() (err error) {
	c := p.connection.GetConnection()
	p._channel, err = c.Channel()

	if err != nil {
		errorMsg := fmt.Sprintf("Can't create a RabbitMQ channel for publisher: %+#v", err)
		log.Print(errorMsg)
		return errors.New(errorMsg)
	}
	for i := range p._notifyPublish {
		ch := make(chan amqp.Confirmation, p._notifyPublish[i].size)
		p._notifyPublish[i].channel = ch
		p._channel.NotifyPublish(ch)
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
func (p *Publisher) NotifyPublish(size int) chan amqp.Confirmation {
	channel := p.GetChannel()
	notifyChannel := make(chan amqp.Confirmation, size)
	p._notifyPublish = append(p._notifyPublish, notifyPublishSpec{notifyChannel, size})
	channel.NotifyPublish(notifyChannel)
	return notifyChannel
}

// Publish pushes items on to a RabbitMQ Queue.
func (p *Publisher) Publish(message string, subscriber *Subscriber) error {
	return p.PublishBytes([]byte(message), subscriber)
}

// PublishBytes is the same as Publish but accepts a []byte instead of a string
func (p *Publisher) PublishBytes(message []byte, subscriber *Subscriber) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.publishBytesWithoutLock(message, subscriber)
}

// PublishBytes is the same as Publish but accepts a []byte instead of a string
func (p *Publisher) publishBytesWithoutLock(message []byte, subscriber *Subscriber) error {
	channel := p.GetChannel()

	return channel.Publish(
		subscriber.Exchange,   // exchange
		subscriber.RoutingKey, // routing key
		false, // mandatory
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
	if p.connection != nil {
		p.connection.Close()
	} else if publisherConnection != nil {
		publisherConnection.Close()
	}
}
