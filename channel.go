package rabbit

import "github.com/streadway/amqp"

func createConnectionClosingChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	channel, err := conn.Channel()

	if err != nil {
		return channel, err
	}

	return channel, nil
}
