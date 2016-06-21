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
	Name  string `json:"Name"`
	Email string `json:"Email"`
}

func sampleTestEventCreatedHandler(payload []byte) bool {
	event := TestEvent{}
	json.Unmarshal(payload, &event)
	log.Printf("%s", event)
	return true
}

/*
   TESTS
*/

var subscriber = rabbit.Subscriber{
	Concurrency: 5,
	Durable:     true,
	Exchange:    "events",
	Queue:       "test.sample.event.created",
	RoutingKey:  "sample.event.created",
}

func TestMain(m *testing.M) {
	os.Setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	os.Exit(m.Run())
}

func TestRegister(t *testing.T) {
	rabbit.Register(subscriber, sampleTestEventCreatedHandler)

	assert := assert.New(t)
	assert.Equal(1, len(rabbit.Subscribers()), "Expected 1 Subscriber")
	assert.Equal(1, len(rabbit.Handlers()), "Expected 1 Handler")
}

func TestStartingSubscribers(t *testing.T) {
	rabbit.Register(subscriber, sampleTestEventCreatedHandler)
	rabbit.StartSubscribers()
}
