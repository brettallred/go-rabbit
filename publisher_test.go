package rabbit_test

import (
	"testing"

	"github.com/brettallred/rabbit"
)

func TestPublish(t *testing.T) {
	rabbit.Publish("Test Publishing", &subscriber)
}
