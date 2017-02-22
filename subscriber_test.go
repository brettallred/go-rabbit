package rabbit_test

import (
	"fmt"
	"os"
	"os/user"
	"testing"
	"time"

	"github.com/brettallred/go-rabbit"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestPrefixQueueInDev(t *testing.T) {
	var subscriber = rabbit.Subscriber{
		Concurrency: 5,
		Durable:     true,
		Exchange:    "events",
		Queue:       "test.sample.event.created",
		RoutingKey:  "sample.event.created",
	}

	os.Setenv("APP_ENV", "development")
	subscriber.PrefixQueueInDev()
	userData, _ := user.Current()

	assert.Equal(t, subscriber.Queue, fmt.Sprintf("%s_test.sample.event.created", userData.Username))
}

func TestAutoDeleteInDev(t *testing.T) {
	var subscriber = rabbit.Subscriber{
		Concurrency: 5,
		Durable:     true,
		Exchange:    "events",
		Queue:       "test.sample.event.created",
		RoutingKey:  "sample.event.created",
	}

	os.Setenv("APP_ENV", "development")
	assert.False(t, subscriber.AutoDelete)

	subscriber.AutoDeleteInDev()
	assert.True(t, subscriber.AutoDelete)
}

func TestIsDevelopmentEnv(t *testing.T) {
	os.Setenv("APP_ENV", "development")
	assert.True(t, rabbit.IsDevelopmentEnv())

	os.Setenv("APP_ENV", "production")
	assert.False(t, rabbit.IsDevelopmentEnv())

	os.Setenv("APP_ENV", "staging")
	assert.False(t, rabbit.IsDevelopmentEnv())

}

func TestSubscribersReconnection(t *testing.T) {
	var subscriber = rabbit.Subscriber{
		Concurrency: 5,
		Durable:     true,
		Exchange:    "events_test",
		Queue:       "test.sample.event.created",
		RoutingKey:  "sample.event.created",
	}
	rabbit.CloseSubscribers()
	rabbit.CreateQueue(rabbit.NewPublisher(make(chan bool)).GetChannel(), &subscriber)
	recreateQueue(t, &subscriber)
	rabbit.CloseSubscribers()
	done := make(chan bool, 100)
	handler := func(delivery amqp.Delivery) bool {
		go func() {
			time.Sleep(1 * time.Second)
			done <- true
		}()
		return true
	}
	rabbit.Register(subscriber, handler)
	rabbit.StartSubscribers()
	connection := rabbit.NewConnectionWithURL(os.Getenv("RABBITMQ_URL"))
	connection.ReplaceConnection(rabbit.ExposeSubscriberConnectionForTests())
	publisher := rabbit.NewPublisherWithConnection(connection, make(chan bool))
	publisher.Publish("test", &subscriber)
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Error("Timeout on waiting for subscriber")
		t.Fail()
	}
	publisher.Close() // the subscriber should reconnect
	timeoutChannel := time.After(5 * time.Second)
	publisher = rabbit.NewPublisher(make(chan bool))
	for {
		select {
		case <-done:
			return
		case <-timeoutChannel:
			t.Error("Timeout on waiting for subscriber")
			t.Fail()
			return
		default:
			err := publisher.Publish("test", &subscriber)
			assert.Nil(t, err)
			time.Sleep(1 * time.Second)
		}
	}
}

func TestSubscribersWithManualAck(t *testing.T) {
	var subscriber = rabbit.Subscriber{
		Concurrency: 5,
		Durable:     true,
		Exchange:    "events_test",
		Queue:       "test.sample.event.created",
		RoutingKey:  "sample.event.created",
		ManualAck:   true,
	}
	rabbit.CloseSubscribers()
	recreateQueue(t, &subscriber)
	done := make(chan bool)
	handler := func(delivery amqp.Delivery) bool {
		close(done)
		return true
	}
	rabbit.Register(subscriber, handler)
	rabbit.StartSubscribers()
	publisher := rabbit.NewPublisher(make(chan bool))
	publisher.Publish("test", &subscriber)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Timeout on waiting for subscriber")
		t.Fail()
	}
	rabbit.CloseSubscribers()
	done = make(chan bool)
	handlerWithAck := func(delivery amqp.Delivery) bool {
		delivery.Ack(false)
		close(done)
		return true
	}
	rabbit.Register(subscriber, handlerWithAck)
	rabbit.StartSubscribers()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Timeout on waiting for subscriber")
		t.Fail()
	}
	rabbit.CloseSubscribers()
	handlerGuard := func(delivery amqp.Delivery) bool {
		t.Error("Should not be called")
		return true
	}
	rabbit.Register(subscriber, handlerGuard)
	rabbit.StartSubscribers()
	select {
	case <-time.After(5 * time.Second):
	}
}
