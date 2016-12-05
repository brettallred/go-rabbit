package rabbit

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

//AssuredPublisher allows you to publish events to RabbitMQ with implicit delivery confirmation
type AssuredPublisher struct {
	Publisher
}

func (p *AssuredPublisher) construct() {
	p.NotifyPublish(make(chan amqp.Confirmation, 1))
	for err := p.Confirm(false); err != nil; err = p.Confirm(false) {
		logError(err, "Can't setup confirmations for a publisher")
		time.Sleep(1)
	}
}

// NewAssuredPublisher constructs a new AssuredPublisher instance
func NewAssuredPublisher() *AssuredPublisher {
	publisher := &AssuredPublisher{Publisher{connection: publisherConnection}}
	publisher.construct()
	return publisher
}

// NewAssuredPublisherWithConnection constructs a new AssuredPublisher instance
func NewAssuredPublisherWithConnection(connection *Connection) *AssuredPublisher {
	publisher := &AssuredPublisher{Publisher{connection: connection}}
	publisher.construct()
	return publisher
}

// Publish pushes items on to a RabbitMQ Queue.
// For AssuredPublisher it waits for delivery confirmaiton and retries on failures
func (p *AssuredPublisher) Publish(message string, subscriber *Subscriber, cancel <-chan bool) bool {
	for {
		if err := (&p.Publisher).Publish(message, subscriber); err != nil {
			log.Printf("Error on pushing into RabbitMQ: %v", err)
			select {
			case <-cancel:
				return false
			default:
			}
			continue
		}
		break
	}
	if !p.waitForConfirmation(cancel) {
		p.Close()
		return false
	}
	return true
}

// PublishBytes is the same as Publish but accepts a []byte instead of a string.
// For AssuredPublisher it waits for delivery confirmaiton and retries on failures
func (p *AssuredPublisher) PublishBytes(message []byte, subscriber *Subscriber, cancel <-chan bool) bool {
	for {
		if err := (&p.Publisher).PublishBytes(message, subscriber); err != nil {
			log.Printf("Error on pushing into RabbitMQ: %v", err)
			select {
			case <-cancel:
				return false
			default:
			}
			continue
		}
		break
	}
	if !p.waitForConfirmation(cancel) {
		p.Close()
		return false
	}
	return true
}

func (p *AssuredPublisher) waitForConfirmation(cancel <-chan bool) bool {
	timeout := time.After(10 * time.Second)
	select {
	case confirmed := <-p._notifyPublish[0]:
		if confirmed.Ack {
			return true
		}
		log.Printf("Unknown Error (RabbitMQ Ack is false)")
		return false
	case <-timeout:
		log.Printf("Error: RabbitMQ Timeout")
		return false
	case <-cancel:
		return false
	}
}
