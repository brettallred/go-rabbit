package rabbit_test

import (
	"encoding/json"
	"log"
	"os"
	"testing"

	"github.com/brettallred/rabbit"
	"github.com/stretchr/testify/assert"
)

type TestEvent struct {
	Name  string `json: Name`
	Email string `json: Email`
}

func SampleEventsCreatedHandler(payload []byte) bool {
	event := TestEvent{}
	json.Unmarshal(payload, &event)
	log.Printf("%s", event)
	return true
}

/*
   TESTS
*/

var subscriber = rabbit.Subscriber{
	AutoAck:     false,
	Concurrency: 5,
	Durable:     true,
	Exchange:    "events",
	Queue:       "test.sample.event.created",
	RoutingKey:  "sample.test_event.created",
}

func TestMain(m *testing.M) {
	os.Setenv("RABBITMQ_URI", "amqp://guest:guest@localhost:5672/")
	os.Exit(m.Run())
}

func TestRegisterSubscriber(t *testing.T) {
	rabbit.RegisterSubscriber(subscriber, SampleEventsCreatedHandler)

	assert := assert.New(t)
	assert.Equal(1, len(rabbit.Subscribers()), "Expected 1 Subscriber")
	assert.Equal(1, len(rabbit.Handlers()), "Expected 1 Handler")
}

func TestStartingSubscribers(t *testing.T) {
	//rabbit.RegisterSubscriber(subscriber, SampleEventsCreatedHandler)
	//rabbit.StartSubscribers()
}
