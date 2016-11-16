package rabbit

import (
	"github.com/streadway/amqp"
)

func createAutoClosingChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	channel, err := conn.Channel()
	if err != nil {
		return channel, err
	}

	errorChannel := make(chan *amqp.Error, 1)
	channel.NotifyClose(errorChannel)

	go closeConnectionOnChannelNotifyClose(errorChannel)

	return channel, nil
}

func closeConnectionOnChannelNotifyClose(errorChannel chan *amqp.Error) {
	select {
	case <-errorChannel:
		lock.Lock()
		defer lock.Unlock()
		_connection.Close()
		return
	}
}
