package rabbit

import (
	"github.com/streadway/amqp"
)

func createAutoClosingChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	channel, err := conn.Channel()

	if err != nil {
		return channel, err
	}

	go closeConnectionOnChannelNotifyClose(channel)

	return channel, nil
}

func closeConnectionOnChannelNotifyClose(channel *amqp.Channel) {
	errorChannel := make(chan *amqp.Error, 1)
	channel.NotifyClose(errorChannel)

	select {
	case <-errorChannel:
		lock.Lock()
		defer lock.Unlock()
		_connection.Close()
		return
	}
}
