package psub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/func25/gop/goper"
	"google.golang.org/api/iterator"
)

type Pubsub struct {
	TopicID    string
	UpsertSubs UpsertSubsCommand
}

func (c *PsubClient) UpsertMany(ctx context.Context, psubs []Pubsub) error {
	topics := map[string]*pubsub.Topic{}

	// collect all topics
	topicIt := c.Topics(ctx)
	for {
		topic, err := topicIt.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		topics[topic.ID()] = topic
	}

	// collect all subs
	subs := map[string]*pubsub.Subscription{}
	subIt := c.Subscriptions(ctx)
	for {
		sub, err := subIt.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		subs[sub.ID()] = sub
	}

	paraller := goper.Paraller(100)

	// upsert un-existed topics and un-existed subs
	var err error
	for i := range psubs {
		elem := psubs[i]

		paraller.AddWorks(func() error {
			topic, ok := topics[elem.TopicID]
			if !ok {
				topic, err = c.UpsertTopic(ctx, elem.TopicID)
				if err != nil {
					return err
				}
			}

			// filter un-existed subs
			cmd := UpsertSubsCommand{DefaultConfig: elem.UpsertSubs.DefaultConfig}
			for _, v := range elem.UpsertSubs.Subs {
				if _, ok := subs[v.ID]; !ok {
					cmd.Subs = append(cmd.Subs, v)
				}
			}

			// if all subs are existed, then stop upsert subscriptions
			if len(cmd.Subs) == 0 {
				return err
			}

			// set default topic for subscription
			if elem.UpsertSubs.DefaultConfig.Topic == nil {
				elem.UpsertSubs.DefaultConfig.Topic = topic
			}
			if err := c.UpsertSubscriptions(ctx, cmd); err != nil {
				return err
			}

			return nil
		})
	}

	errs := paraller.Execute()
	if len(errs) > 0 {
		return errs[0].Err
	}

	return nil
}

func (c *PsubClient) Upsert(ctx context.Context, topicID string, subIDs []string) error {
	topic, err := c.UpsertTopic(ctx, topicID)
	if err != nil {
		return err
	}

	upsertSubs := make([]UpsertSub, len(subIDs))
	for i := range subIDs {
		upsertSubs[i] = UpsertSub{
			ID: subIDs[i],
		}
	}

	return c.UpsertSubscriptions(ctx, UpsertSubsCommand{
		DefaultConfig: NewBoiler().SubscriptionConfig(topic),
		Subs:          upsertSubs,
	})
}
