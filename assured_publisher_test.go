package rabbit_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/brettallred/go-rabbit"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestPublishAssured(t *testing.T) {
	var subscriber = rabbit.Subscriber{
		Concurrency: 5,
		Durable:     true,
		Exchange:    "events_test",
		Queue:       "test.assuredpublishsample.event.created",
		RoutingKey:  "assuredpublishsample.event.created",
	}
	assert := assert.New(t)

	message := "Test Message"
	publisher := rabbit.NewAssuredPublisher()
	err := rabbit.CreateQueue(publisher.GetChannel(), &subscriber)
	assert.Nil(err)
	ok := publisher.Publish(message, &subscriber, make(chan bool))

	var result string
	result, _ = rabbit.Pop(&subscriber)
	assert.Equal(message, result)
	assert.True(ok)
}

func TestPublishWithExplicitWaiting(t *testing.T) {
	var subscriber = rabbit.Subscriber{
		Concurrency: 5,
		Durable:     true,
		Exchange:    "events_test",
		Queue:       "test.assuredpublishsampleexpl.event.created",
		RoutingKey:  "publishsampleexp.event.created",
	}
	assert := assert.New(t)

	publisher := rabbit.NewAssuredPublisher()
	publisher.SetExplicitWaiting()
	err := rabbit.CreateQueue(publisher.GetChannel(), &subscriber)
	assert.Nil(err)

	publisher.GetChannel().QueueDelete(subscriber.Queue, true, false, false)

	messagesMap := map[string]bool{}
	doneReading := make(chan bool)
	messagesRead := 0
	lock := sync.Mutex{}

	subscriberHandler := func(delivery *amqp.Delivery) bool {
		lock.Lock()
		defer lock.Unlock()
		messagesMap[string(delivery.Body)] = true
		messagesRead++
		if len(messagesMap) == 10000 {
			close(doneReading)
		}
		return true
	}
	rabbit.Register(subscriber, subscriberHandler)
	rabbit.StartSubscribers()
	defer rabbit.CloseSubscribers()

	for i := 0; i < 10000; i++ {
		if i != 0 && i%999 == 0 {
			// kill the connection
			publisher.GetChannel().ExchangeDelete(subscriber.Exchange, true, true)
			publisher.GetChannel().ExchangeDeclare(subscriber.Exchange, "topic", false, false, false, false, nil)
		}
		ok := publisher.Publish(fmt.Sprintf("%d", i), &subscriber, make(chan bool))
		assert.True(ok)
	}
	done := make(chan bool)
	cancel := make(chan bool)
	go func() {
		result := publisher.WaitForAllConfirmations(cancel)
		assert.True(result)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(cancel)
		assert.Fail("Timeout while waiting for confirmations")
	}

	select {
	case <-doneReading:
	case <-time.After(10 * time.Second):
		close(cancel)
		assert.Fail(fmt.Sprintf("Timeout while reading messages. Messages read: %d", messagesRead))
	}
	assert.Len(messagesMap, 10000)
}
