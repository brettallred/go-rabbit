# rabbit for Go

RabbitMQ Topic Subscriber for Go.


## Installation

Dowload rabbit using go get

```sh
go get github.com/brettallred/rabbit
```

Import rabbit into your package

```go
import github.com/brettallred/rabbit
```


## Getting Started

rabbit for Go consists of Subscribers and Handlers.  First you need to create a Subscriber

```go
var subscriber = rabbit.Subscriber{
	Concurrency: 5,
	Durable:     true,
	Exchange:    "events",
	Queue:       "test.sample.event.created",
	RoutingKey:  "sample.test_event.created",
}
```

Next, you need to create a Handler that will handle the messages your subscriber receives

```go
func sampleTestEventCreatedHandler(payload []byte) bool {
	log.Printf("%s", payload)
	return true
}
```

Now, register your Subscriber and Handler with rabbit

```go
rabbit.Register(subscriber, sampleTestEventCreatedHandler)
```

Finally, fire up the subscribers

```go
rabbit.StartSubscribers()
```


## Publishing

rabbit includes a simple Publisher

```go
publisher := rabbit.NewPublisher()
publisher.Publish("My Message", subscriber)
```

or, if you are publishing something that isn't a string

```go
publisher.PublishBytes([]byte("My Message"), subscriber)
```

##Contributing

This is my first project in Go. Feedback and contributions are appreciated. Before submitting a Pull Request, please open an issue outlining the problem and the proposed enhancement. Please reference the issue in your Pull Request.


