package rabbit

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

var _connection *amqp.Connection = nil

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func logError(err error, msg string) {
	if err != nil {
		log.Printf("ERROR: %s: %s", msg, err)
	}
}
