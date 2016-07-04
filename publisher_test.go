package rabbit_test

import (
	"testing"

	"github.com/brettallred/rabbit"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestPublish(t *testing.T) {
	assert := assert.New(t)

	message := "Test Message"
	publisher := rabbit.NewPublisher()
	publisher.Publish(message, &subscriber)

	var result string
	result, _ = rabbit.Pop(&subscriber)
	assert.Equal(message, result)
}

func TestConfirm(t *testing.T) {
	publisher := rabbit.NewPublisher()
	confirms := publisher.NotifyPublish(make(chan amqp.Confirmation, 1))
	publisher.Confirm(false)
	publisher.Publish("something", &subscriber)
	rabbit.Pop(&subscriber)
	<-confirms
}
