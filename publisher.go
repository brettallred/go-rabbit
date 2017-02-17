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
	cancelChannel     <-chan bool
	publicMethodsLock sync.Mutex
}

type notifyPublishSpec struct {
	channel chan amqp.Confirmation
	size    int
}

// NewPublisher constructs a new Publisher instance.
func NewPublisher(cancelChannel <-chan bool) *Publisher {
	return &Publisher{connection: publisherConnection, cancelChannel: cancelChannel}
}

// NewPublisherWithConnection constructs a new Publisher instance with a custom connection
func NewPublisherWithConnection(connection *Connection, cancelChannel <-chan bool) *Publisher {
	return &Publisher{connection: connection, cancelChannel: cancelChannel}
}

// GetChannel returns a publisher's channel. It opens a new channel if needed.
func (p *Publisher) GetChannel() *amqp.Channel {
	p.publicMethodsLock.Lock()
	defer p.publicMethodsLock.Unlock()
	return p.getChannelWithoutLock()
}

func (p *Publisher) getChannelWithoutLock() *amqp.Channel {
	lock.Lock()
	if p._channel != nil {
		defer lock.Unlock()
		return p._channel
	}
	lock.Unlock()
	for {
		p.lock.Lock()
		if err := p.openChannel(); err == nil {
			p.lock.Unlock()
			break
		} else {
			logError(err, "Can't open a new RabbitMQ channel for publisher")
		}
		p.lock.Unlock()
		select {
		case <-p.cancelChannel:
			return nil
		case <-time.After(1 * time.Second):
		}
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	return p._channel
}

func (p *Publisher) openChannel() (err error) {
	c := p.connection.GetConnection()
	p._channel, err = c.Channel()

	if err != nil {
		errorMsg := fmt.Sprintf("Can't create a RabbitMQ channel for publisher: %+#v", err)
		return errors.New(errorMsg)
	}
	for i := range p._notifyPublish {
		ch := make(chan amqp.Confirmation, p._notifyPublish[i].size)
		p._notifyPublish[i].channel = ch
		p._channel.NotifyPublish(ch)
	}
	if p._reliableMode {
		if err := p._channel.Confirm(p._reliableModeWait); err != nil {
			log.Printf("Error on switching to reliable mode: closing channel")
			p.closeWithoutLock()
			return err
		}
	}
	return nil
}

// Confirm enables reliable mode for the publisher.
func (p *Publisher) Confirm(wait bool) error {
	p.publicMethodsLock.Lock()
	defer p.publicMethodsLock.Unlock()

	p.lock.Lock()
	defer p.lock.Unlock()
	channel := p.getChannelWithoutLock()
	if channel == nil {
		return errors.New("Cancelled")
	}
	p._reliableMode = true
	p._reliableModeWait = wait
	err := channel.Confirm(wait)
	if err != nil {
		log.Printf("Error on enabling RabbitMQ confirmation (will close channel): %s", err.Error())
		p.closeWithoutLock()
	}
	return err
}

// NotifyPublish registers a listener for reliable publishing.
func (p *Publisher) NotifyPublish(size int) chan amqp.Confirmation {
	p.publicMethodsLock.Lock()
	defer p.publicMethodsLock.Unlock()

	channel := p.getChannelWithoutLock()
	if channel == nil {
		return nil
	}
	notifyChannel := make(chan amqp.Confirmation, size)
	p.lock.Lock()
	p._notifyPublish = append(p._notifyPublish, notifyPublishSpec{notifyChannel, size})
	p.lock.Unlock()
	channel.NotifyPublish(notifyChannel)
	return notifyChannel
}

// Publish pushes items on to a RabbitMQ Queue.
func (p *Publisher) Publish(message string, subscriber *Subscriber) error {
	return p.PublishBytes([]byte(message), subscriber)
}

// PublishBytes is the same as Publish but accepts a []byte instead of a string
func (p *Publisher) PublishBytes(message []byte, subscriber *Subscriber) error {
	p.publicMethodsLock.Lock()
	defer p.publicMethodsLock.Unlock()

	return p.publishBytesWithoutLock(message, subscriber)
}

// PublishBytes is the same as Publish but accepts a []byte instead of a string
func (p *Publisher) publishBytesWithoutLock(message []byte, subscriber *Subscriber) error {
	channel := p.getChannelWithoutLock()
	if channel == nil {
		return errors.New("Cancelled")
	}

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
	p.publicMethodsLock.Lock()
	defer p.publicMethodsLock.Unlock()
	p.closeWithoutLock()
}

func (p *Publisher) closeWithoutLock() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p._channel != nil {
		p._channel.Close()
		p._channel = nil
	}
}
