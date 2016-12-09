package rabbit

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

const unconfirmedMessagesMaxCount = 1000

//AssuredPublisher allows you to publish events to RabbitMQ with implicit delivery confirmation
type AssuredPublisher struct {
	Publisher

	unconfirmedMessages     map[uint64]*unconfirmedMessageSpec
	sequenceNumber          uint64
	waitAfterEachPublishing bool
	closeChannel            chan *amqp.Error
}

type unconfirmedMessageSpec struct {
	message    string
	subscriber *Subscriber
}

func (p *AssuredPublisher) construct() {
	p.initNewChannel()
	p.NotifyPublish(unconfirmedMessagesMaxCount)
	for err := p.Confirm(false); err != nil; err = p.Confirm(false) {
		logError(err, "Can't setup confirmations for a publisher")
		time.Sleep(1)
	}
}

func (p *AssuredPublisher) initNewChannel() {
	log.Print("Init new channel")
	channel := p.GetChannel()
	p.closeChannel = channel.NotifyClose(make(chan *amqp.Error, 1))
	p.sequenceNumber = 0
}

func (p *AssuredPublisher) ensureChannel(cancel <-chan bool) {
	p.GetChannel()
	select {
	case <-p.closeChannel:
		p.reconnect(cancel)
	default:
	}
}

func (p *AssuredPublisher) reconnect(cancel <-chan bool) bool {
	p.receiveAllConfirmations()
	p.Close()
	p.initNewChannel()
	return p.republishAllMessages(cancel)
}

func (p *AssuredPublisher) republishAllMessages(cancel <-chan bool) bool {
	messages := make([]*unconfirmedMessageSpec, 0, len(p.unconfirmedMessages))
	for _, message := range p.unconfirmedMessages {
		messages = append(messages, message)
	}
	p.unconfirmedMessages = map[uint64]*unconfirmedMessageSpec{}
	for _, message := range messages {
		if !p.Publish(message.message, message.subscriber, cancel) { // if cancelled
			return false
		}
	}
	return true
}

// NewAssuredPublisher constructs a new AssuredPublisher instance
func NewAssuredPublisher() *AssuredPublisher {
	publisher := &AssuredPublisher{Publisher: Publisher{connection: publisherConnection}, unconfirmedMessages: map[uint64]*unconfirmedMessageSpec{}, waitAfterEachPublishing: true}
	publisher.construct()
	return publisher
}

// NewAssuredPublisherWithConnection constructs a new AssuredPublisher instance
func NewAssuredPublisherWithConnection(connection *Connection) *AssuredPublisher {
	publisher := &AssuredPublisher{Publisher: Publisher{connection: connection}, unconfirmedMessages: map[uint64]*unconfirmedMessageSpec{}, waitAfterEachPublishing: true}
	publisher.construct()
	return publisher
}

// SetExplicitWaiting disables implicit waiting for a confirmation after each publishing
func (p *AssuredPublisher) SetExplicitWaiting() {
	p.waitAfterEachPublishing = false
}

// Publish pushes items on to a RabbitMQ Queue.
// For AssuredPublisher it waits for delivery confirmaiton and retries on failures
func (p *AssuredPublisher) Publish(message string, subscriber *Subscriber, cancel <-chan bool) bool {
	return p.PublishBytes([]byte(message), subscriber, cancel)
}

// PublishBytes is the same as Publish but accepts a []byte instead of a string.
// For AssuredPublisher it waits for delivery confirmaiton and retries on failures
func (p *AssuredPublisher) PublishBytes(message []byte, subscriber *Subscriber, cancel <-chan bool) bool {
	for {
		p.ensureChannel(cancel)
		if len(p.unconfirmedMessages) >= unconfirmedMessagesMaxCount {
			if !p.waitForConfirmation(cancel) {
				return false
			}
		}
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
	p.sequenceNumber++
	p.unconfirmedMessages[p.sequenceNumber] = &unconfirmedMessageSpec{string(message), subscriber}
	if p.waitAfterEachPublishing && !p.waitForConfirmation(cancel) {
		return false
	}
	return true
}

func (p *AssuredPublisher) waitForConfirmation(cancel <-chan bool) bool {
	timeout := time.After(10 * time.Second)
	select {
	case confirmed := <-p._notifyPublish[0].channel:
		if confirmed.Ack {
			delete(p.unconfirmedMessages, confirmed.DeliveryTag)
			return true
		}
		log.Printf("Unknown Error (RabbitMQ Ack is false)")
		p.receiveAllConfirmations()
		return p.reconnect(cancel)
	case <-timeout:
		log.Printf("Error: RabbitMQ Timeout")
		return p.reconnect(cancel)
	case <-cancel:
		return false
	}
}

func (p *AssuredPublisher) receiveAllConfirmations() bool {
	if len(p.unconfirmedMessages) == 0 {
		return true
	}
	result := true
	for {
		select {
		case confirmed := <-p._notifyPublish[0].channel:
			if confirmed.Ack {
				delete(p.unconfirmedMessages, confirmed.DeliveryTag)
			} else {
				nilConfirmation := amqp.Confirmation{}
				if nilConfirmation == confirmed {
					return result
				}
				log.Printf("Unknown Error (RabbitMQ Ack is false)")
				result = false
			}
		default:
			return result
		}
	}
}

// WaitForAllConfirmations waits for all confirmations and retries publishing if needed
func (p *AssuredPublisher) WaitForAllConfirmations(cancel <-chan bool) bool {
	for {
		if len(p.unconfirmedMessages) == 0 {
			return true
		}
		if !p.waitForConfirmation(cancel) {
			return false
		}
	}
}
