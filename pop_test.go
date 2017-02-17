package rabbit_test

import (
	"testing"

	"github.com/brettallred/go-rabbit"
	"github.com/stretchr/testify/assert"
)

func TestPop(t *testing.T) {
	var subscriber = rabbit.Subscriber{
		Concurrency: 5,
		Durable:     true,
		Exchange:    "events_test",
		Queue:       "poptest.popsample.event.created",
		RoutingKey:  "popsample.event.created",
	}
	recreateQueue(t, &subscriber)

	assert := assert.New(t)

	message := "Test Message"
	rabbit.NewPublisher(make(chan bool)).Publish(message, &subscriber)

	rabbit.ResetPopConnection()
	result, err := rabbit.Pop(&subscriber)
	assert.Nil(err)
	assert.Equal(message, result)
}
