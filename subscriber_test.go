package rabbit_test

import (
	"fmt"
	"os"
	"os/user"
	"testing"

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
