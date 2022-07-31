package psub

import (
	"time"

	"cloud.google.com/go/pubsub"
)

type Boilerplate struct {
	SubConfig pubsub.SubscriptionConfig
}

func (b Boilerplate) SubscriptionConfig(topic *pubsub.Topic) pubsub.SubscriptionConfig {
	subCfg := b.SubConfig
	subCfg.Topic = topic

	return subCfg
}

func NewBoiler() Boilerplate {
	return Boilerplate{
		SubConfig: pubsub.SubscriptionConfig{
			Topic: nil,
			RetryPolicy: &pubsub.RetryPolicy{
				MinimumBackoff: 1 * time.Second,
				MaximumBackoff: 60 * time.Second,
			},
			AckDeadline: 30 * time.Second,
		},
	}
}
