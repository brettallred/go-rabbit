package rabbit_test

import (
	"testing"

	"github.com/brettallred/rabbit"
	"github.com/stretchr/testify/assert"
)

func TestPop(t *testing.T) {
	assert := assert.New(t)

	message := "Test Message"
	rabbit.Publish(message, &subscriber)

	assert.Equal(message, rabbit.Pop(&subscriber))
}
