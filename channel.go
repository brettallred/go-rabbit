package rabbit

import "github.com/streadway/amqp"

func createConnectionClosingChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	channel, err := conn.Channel()

	if err != nil {
		return channel, err
	}

	errorChannel := make(chan *amqp.Error, 1)
	channel.NotifyClose(errorChannel)

	go closeConnectionOnChannelNotifyClose(errorChannel, conn)

	return channel, nil
}

func closeConnectionOnChannelNotifyClose(errorChannel chan *amqp.Error, conn *amqp.Connection) {
	select {
	case <-errorChannel:
		lock.Lock()
		defer lock.Unlock()
		if _connection == conn {
			_connection.Close()
			_connection = nil
		}
		return
	}
}
