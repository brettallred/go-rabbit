package rabbit_test

import (
	"testing"

	"github.com/brettallred/go-rabbit"
	"github.com/stretchr/testify/assert"
)

func TestPublishAssured(t *testing.T) {
	var subscriber = rabbit.Subscriber{
		Concurrency: 5,
		Durable:     true,
		Exchange:    "events",
		Queue:       "test.publishsample.event.created",
		RoutingKey:  "publishsample.event.created",
	}
	assert := assert.New(t)

	message := "Test Message"
	publisher := rabbit.NewAssuredPublisher()
	ok := publisher.Publish(message, &subscriber, make(chan bool))

	var result string
	result, _ = rabbit.Pop(&subscriber)
	assert.Equal(message, result)
	assert.True(ok)
}
