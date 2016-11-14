package rabbit

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

//AssuredPublisher allows you to publish events to RabbitMQ with implicit delivery confirmation
type AssuredPublisher struct {
	Publisher
}

// NewAssuredPublisher constructs a new AssuredPublisher instance
func NewAssuredPublisher() *AssuredPublisher {
	publisher := &AssuredPublisher{}
	publisher.NotifyPublish(make(chan amqp.Confirmation, 1))
	for err := publisher.Confirm(false); err != nil; err = publisher.Confirm(false) {
		logError(err, "Can't setup confirmations for a publisher")
		time.Sleep(1)
	}
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
