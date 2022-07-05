package psub

import (
	"time"

	"cloud.google.com/go/pubsub"
)

type Boilerplate struct{}

func (Boilerplate) SubscriptionConfig(topic *pubsub.Topic) pubsub.SubscriptionConfig {
	return pubsub.SubscriptionConfig{
		Topic: topic,
		RetryPolicy: &pubsub.RetryPolicy{
			MinimumBackoff: 1 * time.Second,
			MaximumBackoff: 30 * time.Second,
		},
	}
}

func NewBoiler() Boilerplate {
	return Boilerplate{}
}
