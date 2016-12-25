package rabbit_test

import (
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"github.com/brettallred/go-rabbit"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

type TestEvent struct {
	Name  string `json:"Name"`
	Email string `json:"Email"`
}

func sampleTestEventCreatedHandler(delivery *amqp.Delivery) bool {
	event := TestEvent{}
	json.Unmarshal(delivery.Body, &event)
	log.Printf("%s", event)
	return true
}

/*
   TESTS
*/

var subscriber = rabbit.Subscriber{
	Concurrency: 5,
	Durable:     true,
	Exchange:    "events_test",
	Queue:       "test.sample.event.created",
	RoutingKey:  "sample.event.created",
}

func init() {
	os.Setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
}

func TestRegister(t *testing.T) {
	rabbit.Register(subscriber, sampleTestEventCreatedHandler)

	assert := assert.New(t)
	assert.Equal(1, len(rabbit.Subscribers), "Expected 1 Subscriber")
	assert.Equal(1, len(rabbit.Handlers), "Expected 1 Handler")
}

func TestNack(t *testing.T) {
	recreateQueue(t, &subscriber)
	counter := 0
	done := make(chan bool, 2)
	nackHandler := func(delivery *amqp.Delivery) bool {
		counter++
		done <- true
		if counter == 1 {
			return false
		}
		return true
	}
	rabbit.Register(subscriber, nackHandler)
	rabbit.StartSubscribers()
	rabbit.NewPublisher().Publish("{}", &subscriber)
	for i := 0; i < 2; i++ {
		select {
		case <-done:
			break
		case <-time.After(1 * time.Second):
			break
		}
	}
	rabbit.CloseSubscribers()
	assert.Equal(t, 2, counter)
}

func TestStartingSubscribers(t *testing.T) {
	rabbit.Register(subscriber, sampleTestEventCreatedHandler)
	rabbit.StartSubscribers()
}

func recreateQueue(t *testing.T, subscriber *rabbit.Subscriber) {
	done := make(chan bool)
	publisher := rabbit.NewPublisher()
	go func() {
		for {
			if _, err := publisher.GetChannel().QueueDelete(subscriber.Queue, false, false, true); err == nil {
				close(done)
				return
			} else {
				log.Printf("Error on removing queue: %+#v", err)
				publisher.Close()
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Can't delete queue")
		t.Fail()
		return
	}

	done = make(chan bool)
	go func() {
		for {
			if err := rabbit.CreateQueue(publisher.GetChannel(), subscriber); err == nil {
				close(done)
				return
			} else {
				log.Printf("Error on creating queue: %+#v", err)
				publisher.Close()
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Can't create queue")
		t.Fail()
		return
	}
}
