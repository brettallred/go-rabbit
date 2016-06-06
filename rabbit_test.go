package rabbit_test

import (
	"github.com/brettallred/rabbit"
	"os"
	"testing"
)

func SampleEventsCreatedHandler(payload string) {

}

/*
   TESTS
*/

func TestMain(m *testing.M) {
	os.Setenv("RABBITMQ_URI", "amqp://guest:guest@localhost:5672/")
	os.Exit(m.Run())
}

func TestRegisterSubscriber(t *testing.T) {
	subscriber := rabbit.Subscriber{
		Queue:      "test.sample.event.created",
		RoutingKey: "sample.event.created",
		Exchange:   "events",
		AutoAck:    false,
		Durable:    true,
	}
	rabbit.RegisterSubscriber(subscriber, SampleEventsCreatedHandler)

	if len(rabbit.Subscribers()) != 1 {
		t.Error("Expected 1 Subscriber")
	}

	if len(rabbit.Handlers()) != 1 {
		t.Error("Expected 1 Handler")
	}
}

func TestStartingSubscribers(t *testing.T) {

	subscriber := rabbit.Subscriber{
		Queue:      "test.sample.event.created",
		RoutingKey: "sample.event.created",
		Exchange:   "events",
		AutoAck:    false,
		Durable:    true,
	}

	rabbit.RegisterSubscriber(subscriber, SampleEventsCreatedHandler)
	rabbit.StartSubscribers()
}
