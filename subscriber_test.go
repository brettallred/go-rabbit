package rabbit_test

import (
	"fmt"
	"os"
	"os/user"
	"testing"
	"time"

	"github.com/brettallred/go-rabbit"
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
	recreateQueue(t, &subscriber)
	rabbit.CloseSubscribers()
	done := make(chan bool, 100)
	handler := func(payload []byte) bool {
		done <- true
		return true
	}
	rabbit.Register(subscriber, handler)
	rabbit.StartSubscribers()
	connection := rabbit.NewConnectionWithURL(os.Getenv("RABBITMQ_URL"))
	connection.ReplaceConnection(rabbit.ExposeSubscriberConnectionForTests())
	publisher := rabbit.NewPublisherWithConnection(connection)
	publisher.Publish("test", &subscriber)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Timeout on waiting for subscriber")
		t.Fail()
	}
	publisher.GetChannel().QueueDelete(subscriber.Queue, false, false, false) // reconnect
	recreateQueue(t, &subscriber)
	timeoutChannel := time.After(15 * time.Second)
	publisher = rabbit.NewPublisher()
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
