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

	unconfirmedMessages     map[uint64]*unconfirmedMessage
	sequenceNumber          uint64
	waitAfterEachPublishing bool
	closeChannel            chan *amqp.Error
	confirmationHandler     func(amqp.Confirmation, interface{})
	doNotRepublish          bool
}

type unconfirmedMessage struct {
	message    []byte
	subscriber *Subscriber
	arg        interface{}
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
	messages := map[uint64]*unconfirmedMessage{}
	for deliveryTag, message := range p.unconfirmedMessages {
		messages[deliveryTag] = message
	}
	p.unconfirmedMessages = map[uint64]*unconfirmedMessage{}
	for deliveryTag, message := range messages {
		if !p.doNotRepublish {
			if !p.publishBytesWithArgWithoutLock(message.message, message.subscriber, message.arg, cancel) { // if cancelled
				return false
			}
		} else {
			if p.confirmationHandler != nil {
				p.confirmationHandler(amqp.Confirmation{Ack: false, DeliveryTag: deliveryTag}, message.arg)
			}
		}
	}
	return true
}

// NewAssuredPublisher constructs a new AssuredPublisher instance
func NewAssuredPublisher() *AssuredPublisher {
	publisher := &AssuredPublisher{Publisher: Publisher{connection: publisherConnection}, unconfirmedMessages: map[uint64]*unconfirmedMessage{}, waitAfterEachPublishing: true}
	publisher.construct()
	return publisher
}

// NewAssuredPublisherWithConnection constructs a new AssuredPublisher instance
func NewAssuredPublisherWithConnection(connection *Connection) *AssuredPublisher {
	publisher := &AssuredPublisher{Publisher: Publisher{connection: connection}, unconfirmedMessages: map[uint64]*unconfirmedMessage{}, waitAfterEachPublishing: true}
	publisher.construct()
	return publisher
}

// SetExplicitWaiting disables implicit waiting for a confirmation after each publishing
func (p *AssuredPublisher) SetExplicitWaiting() {
	p.waitAfterEachPublishing = false
}

// DisableRepublishing disables messages republishing
func (p *AssuredPublisher) DisableRepublishing() {
	p.doNotRepublish = true
}

// SetConfirmationHandler sets the handler which is called for every confirmation received
func (p *AssuredPublisher) SetConfirmationHandler(confirmationHandler func(amqp.Confirmation, interface{})) {
	p.confirmationHandler = confirmationHandler
}

// Publish pushes items on to a RabbitMQ Queue.
// For AssuredPublisher it waits for delivery confirmaiton and retries on failures
func (p *AssuredPublisher) Publish(message string, subscriber *Subscriber, cancel <-chan bool) bool {
	return p.PublishBytes([]byte(message), subscriber, cancel)
}

// PublishWithArg pushes items on to a RabbitMQ Queue. The argument will be stored for passing into the confirmation handler.
// For AssuredPublisher it waits for delivery confirmaiton and retries on failures
func (p *AssuredPublisher) PublishWithArg(message string, subscriber *Subscriber, arg interface{}, cancel <-chan bool) bool {
	return p.PublishBytesWithArg([]byte(message), subscriber, arg, cancel)
}

// PublishBytes is the same as Publish but accepts a []byte instead of a string.
// For AssuredPublisher it waits for delivery confirmaiton and retries on failures
func (p *AssuredPublisher) PublishBytes(message []byte, subscriber *Subscriber, cancel <-chan bool) bool {
	return p.PublishBytesWithArg(message, subscriber, nil, cancel)
}

// PublishBytesWithArg is the same as Publish but accepts a []byte instead of a string.
// The argument will be stored for passing into the confirmation handler.
// For AssuredPublisher it waits for delivery confirmaiton and retries on failures
func (p *AssuredPublisher) PublishBytesWithArg(message []byte, subscriber *Subscriber, arg interface{}, cancel <-chan bool) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.publishBytesWithArgWithoutLock(message, subscriber, arg, cancel)
}

func (p *AssuredPublisher) publishBytesWithArgWithoutLock(message []byte, subscriber *Subscriber, arg interface{}, cancel <-chan bool) bool {
	for {
		p.ensureChannel(cancel)
		p.receiveAllConfirmations()
		if len(p.unconfirmedMessages) >= unconfirmedMessagesMaxCount {
			if !p.waitForConfirmation(cancel) {
				return false
			}
		}
		if err := (&p.Publisher).publishBytesWithoutLock(message, subscriber); err != nil {
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
	var body []byte
	if !p.doNotRepublish {
		body = message
	}
	p.unconfirmedMessages[p.sequenceNumber] = &unconfirmedMessage{body, subscriber, arg}
	if p.waitAfterEachPublishing && !p.waitForConfirmation(cancel) {
		return false
	}
	return true
}

func (p *AssuredPublisher) waitForConfirmation(cancel <-chan bool) bool {
	timeout := time.After(10 * time.Second)
	select {
	case confirmed := <-p._notifyPublish[0].channel:
		if p.confirmationHandler != nil {
			p.confirmationHandler(confirmed, p.unconfirmedMessages[confirmed.DeliveryTag].arg)
		}
		if confirmed.Ack {
			p.ForgetMessage(confirmed.DeliveryTag)
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
	for {
		select {
		case confirmed := <-p._notifyPublish[0].channel:
			if p.confirmationHandler != nil {
				p.confirmationHandler(confirmed, p.unconfirmedMessages[confirmed.DeliveryTag].arg)
			}
			if confirmed.Ack {
				p.ForgetMessage(confirmed.DeliveryTag)
			} else {
				log.Printf("Unknown Error (RabbitMQ Ack is false)")
				return false
			}
		default:
			return true
		}
	}
}

// ForgetMessage removes a message from the internal storage
func (p *AssuredPublisher) ForgetMessage(deliveryTag uint64) {
	delete(p.unconfirmedMessages, deliveryTag)
}

// WaitForAllConfirmations waits for all confirmations and retries publishing if needed.
// Returns false only if is cancelled.
func (p *AssuredPublisher) WaitForAllConfirmations(cancel <-chan bool) bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	for {
		if len(p.unconfirmedMessages) == 0 {
			return true
		}
		if !p.waitForConfirmation(cancel) {
			return false
		}
	}
}
