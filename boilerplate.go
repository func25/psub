package psub

import (
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/func25/slicesol/slicesol"
)

type boilerplate struct {
	client *pubsub.Client
}

func NewBoiler(c *pubsub.Client) boilerplate {
	b := boilerplate{
		client: c,
	}

	return b
}

func (b boilerplate) SubConfig(topicID string) pubsub.SubscriptionConfig {
	return pubsub.SubscriptionConfig{
		Topic: b.client.Topic(topicID),
		RetryPolicy: &pubsub.RetryPolicy{
			MinimumBackoff: 1 * time.Second,
			MaximumBackoff: 60 * time.Second,
		},
		AckDeadline: 30 * time.Second,
	}
}

func (b boilerplate) SubInfo(topicID string, subIDs []string) SubsInfo {
	return SubsInfo{
		Subs:          slicesol.Map(subIDs, func(s string) Sub { return Sub{ID: s} }),
		DefaultConfig: b.SubConfig(topicID),
	}
}
