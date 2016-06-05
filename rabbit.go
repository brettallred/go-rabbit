package rabbit

import(
  //"reflect"
)

var subscribers []RabbitSubscriber;

type RabbitSubscriber interface {
  Exchange() string
  RoutingKey() string
}

// Adds a subscriber to the subscribers pool
func RegisterSubscriber(subscriber RabbitSubscriber) {
  subscribers = append(subscribers, subscriber);
}

func Subscribers() []RabbitSubscriber {
  return subscribers;
}
