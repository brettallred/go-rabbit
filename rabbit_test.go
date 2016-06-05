package rabbit_test

import(
  "testing"
  "github.com/brettallred/rabbit"
)

type SampleSubscriber struct {
}

func (s *SampleSubscriber) RoutingKey() string {
  return "";
}

func (s *SampleSubscriber) Exchange() string {
  return "";
}

func TestRegisterSubscriber(t *testing.T) {
  s := new(SampleSubscriber);
  rabbit.RegisterSubscriber(s);

  if len(rabbit.Subscribers()) != 1 {
    t.Error("Expected 1 Subscriber");
  }
}
