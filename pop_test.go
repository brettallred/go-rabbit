package rabbit_test

import (
	"testing"

	"github.com/brettallred/rabbit"
	"github.com/stretchr/testify/assert"
)

func TestPop(t *testing.T) {
	assert := assert.New(t)

	message := "Test Message"
	rabbit.NewPublisher().Publish(message, &subscriber)

	var result string
	result, _ = rabbit.Pop(&subscriber)
	assert.Equal(message, result)
}
