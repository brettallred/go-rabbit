package rabbit

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"os"
	"sync"
	"time"
)

type Publisher struct {
	_connection       *amqp.Connection
	_channel          *amqp.Channel
	_notifyPublish    []chan amqp.Confirmation
	_reliableMode     bool
	_reliableModeWait bool
	lock              sync.RWMutex
}

// NewPublisher constructs a new Publisher instance.
func NewPublisher() *Publisher {
	return &Publisher{}
}

func (p *Publisher) connection_and_channel() (*amqp.Connection, *amqp.Channel, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p._connection == nil {
		err := p.connect()
		if err != nil {
			p._connection = nil
			p._channel = nil
			return nil, nil, err
		}
	}
	if p._channel == nil {
		p._channel = createChannel(p._connection)
		if p._channel == nil {
			c := p._connection
			p._connection = nil
			if c != nil {
				go c.Close()
			}
			return nil, nil, errors.New("Can't create channel")
		}
		for i, _ := range p._notifyPublish {
			p._channel.NotifyPublish(p._notifyPublish[i])
		}
		if p._reliableMode {
			if err := p._channel.Confirm(p._reliableModeWait); err != nil {
				c := p._connection
				p._connection = nil
				if c != nil {
					go c.Close()
				}
				return nil, nil, err
			}
		}
	}

	return p._connection, p._channel, nil
}

func (p *Publisher) connect() error {
	p._connection = nil
	p._channel = nil
	c, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		logError(err, "Failed to connect to RabbitMQ")
		return err
	}

	p._connection = c
	errorChannel := make(chan *amqp.Error)
	errorHandler := func() {
		for {
			select {
			case <-errorChannel:
				p.lock.Lock()
				defer p.lock.Unlock()
				if p._connection != nil {
					go log.Printf("RabbitMQ connection failed, we will redial")
					c := p._connection
					p._connection = nil
					p._channel = nil
					if c != nil {
						go c.Close()
					}
				}
				return
			default:
				p.lock.RLock()
				if p._connection == nil {
					p.lock.RUnlock()
					return
				}
				p.lock.RUnlock()
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
	_, channel, err := p.connection_and_channel()
	if err != nil {
		return err
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	err = channel.Confirm(wait)
	if err != nil {
		p._reliableMode = true
		p._reliableModeWait = wait
	}
	return err
}

// NotifyPublish registers a listener for reliable publishing.
func (p *Publisher) NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation {
	_, channel, _ := p.connection_and_channel()
	p.lock.Lock()
	defer p.lock.Unlock()
	p._notifyPublish = append(p._notifyPublish, c)
	if channel != nil {
		channel.NotifyPublish(c)
		return c
	}
	return nil
}

// Publish pushes items on to a RabbitMQ Queue.
func (p *Publisher) Publish(message string, subscriber *Subscriber) error {
	return p.PublishBytes([]byte(message), subscriber)
}

// PublishBytes is the same as Publish but accepts a []byte instead of a string
func (p *Publisher) PublishBytes(message []byte, subscriber *Subscriber) error {
	_, channel, err := p.connection_and_channel()
	if err != nil {
		return err
	}

	return channel.Publish(
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
	p.lock.Lock()
	defer p.lock.Unlock()
	if p._connection != nil {
		c := p._connection
		p._connection = nil
		p._channel = nil
		c.Close()
	}
}
