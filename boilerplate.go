package psub

import (
	"time"

	"cloud.google.com/go/pubsub"
)

type Boilerplate struct {
	SubConfig pubsub.SubscriptionConfig
	client    *pubsub.Client
}

func (b Boilerplate) SubscriptionConfig(topicID string) pubsub.SubscriptionConfig {
	subCfg := b.SubConfig
	subCfg.Topic = b.client.Topic(topicID)

	return subCfg
}

func NewBoiler(c *pubsub.Client) Boilerplate {
	return Boilerplate{
		client: c,
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
