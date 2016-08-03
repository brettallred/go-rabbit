package rabbit_test

import (
	"testing"

	"github.com/brettallred/rabbit-go"
	"github.com/stretchr/testify/assert"
)

func TestPop(t *testing.T) {
	var subscriber = rabbit.Subscriber{
		Concurrency: 5,
		Durable:     true,
		Exchange:    "events",
		Queue:       "poptest.popsample.event.created",
		RoutingKey:  "popsample.event.created",
	}

	assert := assert.New(t)

	message := "Test Message"
	rabbit.NewPublisher().Publish(message, &subscriber)

	var result string
	result, _ = rabbit.Pop(&subscriber)
	assert.Equal(message, result)
}
