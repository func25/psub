package psub

import (
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/func25/slicesol/slicesol"
)

type boilerplate struct {
	client *pubsub.Client

	// not using function receiver for custom purpose
	SubConfig func(topicID string) pubsub.SubscriptionConfig
	SubInfo   func(topicID string, subIDs []string) SubsInfo
	UpsertCmd func(topicID string, subIDs []string) UpsertCmd
}

func NewBoiler(c *pubsub.Client) boilerplate {
	b := &boilerplate{
		client: c,
	}

	b.SubConfig = boilerSubConfig(b)
	b.SubInfo = boilerSubInfo(b)
	b.UpsertCmd = boilerUpsertCmd(b)

	return *b
}

func (b *boilerplate) GetClient() *pubsub.Client {
	return b.client
}

func boilerSubConfig(b *boilerplate) func(topicID string) pubsub.SubscriptionConfig {
	return func(topicID string) pubsub.SubscriptionConfig {
		return pubsub.SubscriptionConfig{
			Topic: b.client.Topic(topicID),
			RetryPolicy: &pubsub.RetryPolicy{
				MinimumBackoff: 1 * time.Second,
				MaximumBackoff: 10 * time.Second,
			},
			AckDeadline: 30 * time.Second,
		}
	}
}

func boilerSubInfo(b *boilerplate) func(topicID string, subIDs []string) SubsInfo {
	return func(topicID string, subIDs []string) SubsInfo {
		return SubsInfo{
			Subs:          slicesol.Map(subIDs, func(s string) Sub { return Sub{ID: s} }),
			DefaultConfig: b.SubConfig(topicID),
		}
	}
}

func boilerUpsertCmd(b *boilerplate) func(topicID string, subIDs []string) UpsertCmd {
	return func(topicID string, subIDs []string) UpsertCmd {
		return UpsertCmd{
			TopicID:    topicID,
			UpsertSubs: b.SubInfo(topicID, subIDs),
		}
	}
}
