package rabbit

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

var publisherConnection = &Connection{}

//Publisher allows you to publish events to RabbitMQ
type Publisher struct {
	_channel          *amqp.Channel
	_notifyPublish    []chan amqp.Confirmation
	_reliableMode     bool
	_reliableModeWait bool
	connection        *Connection
	lock              sync.RWMutex
}

// NewPublisher constructs a new Publisher instance.
func NewPublisher() *Publisher {
	return &Publisher{}
}

// GetChannel returns a publisher's channel. It opens a new channel if needed.
func (p *Publisher) GetChannel() *amqp.Channel {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p._channel != nil {
		return p._channel
	}
	for {
		if err := p.openChannel(); err == nil {
			break
		} else {
			logError(err, "Can't open a new RabbitMQ channel")
		}
		time.Sleep(1)
	}
	return p._channel
}

func (p *Publisher) openChannel() error {
	c := publisherConnection.GetConnection()
	p._channel = createChannel(c)
	if p._channel == nil {
		publisherConnection.Close()
		return errors.New("Can't create channel")
	}
	oldChannel := p._channel
	errorChannel := make(chan *amqp.Error)
	errorHandler := func() {
		for {
			select {
			case <-errorChannel:
				log.Printf("RabbitMQ connection error")
				p.lock.Lock()
				defer p.lock.Unlock()
				if oldChannel == p._channel {
					p._channel = nil
				}
				return
			default:
				p.lock.RLock()
				if p._channel != oldChannel {
					p.lock.RUnlock()
					return
				}
				p.lock.RUnlock()
				time.Sleep(1 * time.Second)
			}
		}
	}
	c.NotifyClose(errorChannel)
	go errorHandler()
	for i := range p._notifyPublish {
		p._channel.NotifyPublish(p._notifyPublish[i])
	}
	if p._reliableMode {
		if err := p._channel.Confirm(p._reliableModeWait); err != nil {
			p._channel = nil
			publisherConnection.Close()
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
		publisherConnection.Close()
	}
	return err
}

// NotifyPublish registers a listener for reliable publishing.
func (p *Publisher) NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation {
	channel := p.GetChannel()
	p.lock.Lock()
	p._notifyPublish = append(p._notifyPublish, c)
	p.lock.Unlock()
	channel.NotifyPublish(c)
	return c
}

// Publish pushes items on to a RabbitMQ Queue.
func (p *Publisher) Publish(message string, subscriber *Subscriber) error {
	return p.PublishBytes([]byte(message), subscriber)
}

// PublishBytes is the same as Publish but accepts a []byte instead of a string
func (p *Publisher) PublishBytes(message []byte, subscriber *Subscriber) error {
	log.Printf("PublishBytes")
	defer log.Printf("Done PublishBytes")
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
	p.lock.Lock()
	defer p.lock.Unlock()
	if p._channel != nil {
		p._channel.Close()
		p._channel = nil
	}
}
