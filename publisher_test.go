package rabbit_test

import (
	"testing"

	"github.com/brettallred/go-rabbit"
	"github.com/stretchr/testify/assert"
)

func TestPublish(t *testing.T) {
	var subscriber = rabbit.Subscriber{
		Concurrency: 5,
		Durable:     true,
		Exchange:    "events_test",
		Queue:       "test.publishsample.event.created",
		RoutingKey:  "publishsample.event.created",
	}
	assert := assert.New(t)

	recreateQueue(t, &subscriber)
	message := "Test Message"
	publisher := rabbit.NewPublisher()
	publisher.Publish(message, &subscriber)

	var result string
	result, _ = rabbit.Pop(&subscriber)
	assert.Equal(message, result)
}

func TestConfirm(t *testing.T) {
	var subscriber = rabbit.Subscriber{
		Concurrency: 5,
		Durable:     true,
		Exchange:    "events_test",
		Queue:       "test.confirmsample.event.created",
		RoutingKey:  "confirmsample.event.created",
	}
	publisher := rabbit.NewPublisher()
	confirms := publisher.NotifyPublish(1)
	publisher.Confirm(false)
	publisher.Publish("something", &subscriber)
	rabbit.Pop(&subscriber)
	<-confirms

	publisher.Close()
	publisher.Publish("something", &subscriber)
	rabbit.Pop(&subscriber)
	<-confirms
}
