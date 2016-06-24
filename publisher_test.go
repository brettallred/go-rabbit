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
	rabbit.Publish(message, &subscriber)

	assert.Equal(message, rabbit.Pop(&subscriber))
}

func TestConfirm(t *testing.T) {
	rabbit.ReInitPublisher()
	go rabbit.Pop(&subscriber)
	confirms := rabbit.NotifyPublish(make(chan amqp.Confirmation, 1))
	rabbit.ConfirmPublish(false)
	rabbit.Publish("something", &subscriber)
	<-confirms
}
