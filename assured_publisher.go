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
		select {
		case <-p.cancelChannel:
			return
		case <-time.After(1 * time.Second):
		}
	}
}

// should be within locked block
func (p *AssuredPublisher) initNewChannel() {
	channel := p.getChannelWithoutLock()
	closeChannel := channel.NotifyClose(make(chan *amqp.Error, 1))
	p.lock.Lock()
	p.closeChannel = closeChannel
	p.sequenceNumber = 0
	p.lock.Unlock()
	go p.continuoslyReceiveConfirmations()
}

// should be within locked block
func (p *AssuredPublisher) ensureChannel() {
	p.getChannelWithoutLock()
	p.lock.Lock()
	closeChannel := p.closeChannel
	p.lock.Unlock()
	select {
	case e := <-closeChannel:
		log.Printf("Error: publisher's channel has been closed (%+#v). Recreating it", e)
		p.lock.Lock()
		p._channel = nil
		p.lock.Unlock()
		p.reconnect()
	default:
	}
}

// should be within locked block
func (p *AssuredPublisher) reconnect() bool {
	p.receiveAllConfirmations()
	p.initNewChannel()
	return p.republishAllMessages()
}

func (p *AssuredPublisher) republishAllMessages() bool {
	messages := map[uint64]*unconfirmedMessage{}
	p.lock.Lock()
	for deliveryTag, message := range p.unconfirmedMessages {
		messages[deliveryTag] = message
	}
	p.unconfirmedMessages = map[uint64]*unconfirmedMessage{0: &unconfirmedMessage{}}
	doNotRepublish := p.doNotRepublish
	confirmationHandler := p.confirmationHandler
	p.lock.Unlock()
	for deliveryTag, message := range messages {
		if deliveryTag == 0 {
			continue
		}
		if !doNotRepublish {
			if !p.publishBytesWithArgWithoutLock(message.message, message.subscriber, message.arg) { // if cancelled
				return false
			}
		} else {
			if confirmationHandler != nil {
				confirmationHandler(amqp.Confirmation{Ack: false, DeliveryTag: deliveryTag}, message.arg)
			}
		}
	}
	p.lock.Lock()
	delete(p.unconfirmedMessages, 0)
	p.lock.Unlock()
	return true
}

// NewAssuredPublisher constructs a new AssuredPublisher instance
func NewAssuredPublisher(cancelChannel <-chan bool) *AssuredPublisher {
	publisher := &AssuredPublisher{Publisher: Publisher{connection: publisherConnection, cancelChannel: cancelChannel},
		unconfirmedMessages: map[uint64]*unconfirmedMessage{}, waitAfterEachPublishing: true}
	publisher.construct()
	return publisher
}

// NewAssuredPublisherWithConnection constructs a new AssuredPublisher instance
func NewAssuredPublisherWithConnection(connection *Connection, cancelChannel <-chan bool) *AssuredPublisher {
	publisher := &AssuredPublisher{Publisher: Publisher{connection: connection, cancelChannel: cancelChannel},
		unconfirmedMessages: map[uint64]*unconfirmedMessage{}, waitAfterEachPublishing: true}
	publisher.construct()
	return publisher
}

// SetExplicitWaiting disables implicit waiting for a confirmation after each publishing
func (p *AssuredPublisher) SetExplicitWaiting() {
	p.publicMethodsLock.Lock()
	defer p.publicMethodsLock.Unlock()
	p.lock.Lock()
	p.waitAfterEachPublishing = false
	p.lock.Unlock()
}

// DisableRepublishing disables messages republishing
func (p *AssuredPublisher) DisableRepublishing() {
	p.publicMethodsLock.Lock()
	defer p.publicMethodsLock.Unlock()
	p.lock.Lock()
	p.doNotRepublish = true
	p.lock.Unlock()
}

// SetConfirmationHandler sets the handler which is called for every confirmation received
func (p *AssuredPublisher) SetConfirmationHandler(confirmationHandler func(amqp.Confirmation, interface{})) {
	p.publicMethodsLock.Lock()
	defer p.publicMethodsLock.Unlock()
	p.lock.Lock()
	p.confirmationHandler = confirmationHandler
	p.lock.Unlock()
}

// Publish pushes items on to a RabbitMQ Queue.
// For AssuredPublisher it waits for delivery confirmaiton and retries on failures
func (p *AssuredPublisher) Publish(message string, subscriber *Subscriber) bool {
	return p.PublishBytes([]byte(message), subscriber)
}

// PublishWithArg pushes items on to a RabbitMQ Queue. The argument will be stored for passing into the confirmation handler.
// For AssuredPublisher it waits for delivery confirmaiton and retries on failures
func (p *AssuredPublisher) PublishWithArg(message string, subscriber *Subscriber, arg interface{}) bool {
	return p.PublishBytesWithArg([]byte(message), subscriber, arg)
}

// PublishBytes is the same as Publish but accepts a []byte instead of a string.
// For AssuredPublisher it waits for delivery confirmaiton and retries on failures
func (p *AssuredPublisher) PublishBytes(message []byte, subscriber *Subscriber) bool {
	return p.PublishBytesWithArg(message, subscriber, nil)
}

// PublishBytesWithArg is the same as Publish but accepts a []byte instead of a string.
// The argument will be stored for passing into the confirmation handler.
// For AssuredPublisher it waits for delivery confirmaiton and retries on failures
func (p *AssuredPublisher) PublishBytesWithArg(message []byte, subscriber *Subscriber, arg interface{}) bool {
	p.publicMethodsLock.Lock()
	defer p.publicMethodsLock.Unlock()
	return p.publishBytesWithArgWithoutLock(message, subscriber, arg)
}

func (p *AssuredPublisher) publishBytesWithArgWithoutLock(message []byte, subscriber *Subscriber, arg interface{}) bool {
	for {
		p.ensureChannel()
		p.receiveAllConfirmations()
		p.lock.Lock()
		unconfirmedCount := len(p.unconfirmedMessages)
		p.lock.Unlock()
		if unconfirmedCount >= unconfirmedMessagesMaxCount {
			if !p.waitForConfirmation() {
				return false
			}
		}
		if err := (&p.Publisher).publishBytesWithoutLock(message, subscriber); err != nil {
			log.Printf("Error on pushing into RabbitMQ: %v", err)
			select {
			case <-p.cancelChannel:
				return false
			case <-time.After(1 * time.Second):
			}
			continue
		}
		break
	}
	p.lock.Lock()
	p.sequenceNumber++
	var body []byte
	if !p.doNotRepublish {
		body = message
	}
	p.unconfirmedMessages[p.sequenceNumber] = &unconfirmedMessage{body, subscriber, arg}
	p.lock.Unlock()
	if p.waitAfterEachPublishing && !p.waitForConfirmation() {
		return false
	}
	return true
}

func (p *AssuredPublisher) waitForConfirmation() bool {
	for {
		p.lock.Lock()
		ch := p._notifyPublish[0].channel
		p.lock.Unlock()
		select {
		case confirm := <-ch:
			if !p.handleConfirmationMessage(confirm) {
				p.lock.Lock()
				if len(p.unconfirmedMessages) == 0 {
					p.lock.Unlock()
					return true
				}
				p.lock.Unlock()
			} else {
				return true
			}
		case <-time.After(10 * time.Second):
			log.Printf("Error: RabbitMQ Timeout")
		case <-p.cancelChannel:
			return false
		}
		log.Printf("Next in waitForConfirmation")
	}
}

// ReceiveAllConfirmations explicitly receives all awaiting confirmations
func (p *AssuredPublisher) ReceiveAllConfirmations() bool {
	p.publicMethodsLock.Lock()
	defer p.publicMethodsLock.Unlock()
	return p.receiveAllConfirmations()
}

func (p *AssuredPublisher) continuoslyReceiveConfirmations() {
	for {
		if !p.receiveAllConfirmations() {
			return
		}
		select {
		case <-p.cancelChannel:
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (p *AssuredPublisher) receiveAllConfirmations() bool {
	p.lock.Lock()
	count := len(p.unconfirmedMessages)
	if len(p._notifyPublish) < 1 {
		p.lock.Unlock()
		return true
	}
	channel := p._notifyPublish[0].channel
	p.lock.Unlock()
	if count == 0 {
		return true
	}
	for {
		select {
		case confirm := <-channel:
			if !p.handleConfirmationMessage(confirm) {
				return false
			}
		default:
			return true
		}
	}
}

func (p *AssuredPublisher) handleConfirmationMessage(confirm amqp.Confirmation) bool {
	if confirm.DeliveryTag == 0 {
		log.Printf("Unknown Error (RabbitMQ Publisher's confirmation channel has been closed)")
		return false
	}
	p.lock.Lock()
	confirmationHandler := p.confirmationHandler
	arg := p.unconfirmedMessages[confirm.DeliveryTag].arg
	p.lock.Unlock()
	if confirmationHandler != nil {
		confirmationHandler(confirm, arg)
	}
	if confirm.Ack || p.doNotRepublish {
		p.forgetMessage(confirm.DeliveryTag)
	}
	if !confirm.Ack {
		log.Printf("Unknown Error (RabbitMQ Ack is false)")
	}
	return true
}

// forgetMessage removes a message from the internal storage
func (p *AssuredPublisher) forgetMessage(deliveryTag uint64) {
	p.lock.Lock()
	delete(p.unconfirmedMessages, deliveryTag)
	p.lock.Unlock()
}

// WaitForAllConfirmations waits for all confirmations and retries publishing if needed.
// Returns false only if is cancelled.
func (p *AssuredPublisher) WaitForAllConfirmations() bool {
	p.publicMethodsLock.Lock()
	defer p.publicMethodsLock.Unlock()
	for {
		p.lock.Lock()
		if len(p.unconfirmedMessages) == 0 {
			p.lock.Unlock()
			return true
		}
		p.lock.Unlock()
		select {
		case <-p.cancelChannel:
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}
