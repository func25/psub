package psub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/func25/gop/goper"
	"google.golang.org/api/iterator"
)

type UpsertCmd struct {
	TopicID    string
	UpsertSubs SubsInfo
}

func (c *PsubConnection) UpsertMany(ctx context.Context, psubs []UpsertCmd) error {
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
			cmd := SubsInfo{DefaultConfig: elem.UpsertSubs.DefaultConfig}
			for _, v := range elem.UpsertSubs.Subs {
				if _, ok := subs[v.ID]; !ok {
					cmd.Subs = append(cmd.Subs, v)
				}
			}

			// if all subs are existed, then stop upsert subscriptions
			if len(cmd.Subs) == 0 {
				return nil
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

func (c *PsubConnection) Upsert(ctx context.Context, cmd UpsertCmd) error {
	topic, err := c.UpsertTopic(ctx, cmd.TopicID)
	if err != nil {
		return err
	}

	upsertSubs := make([]Sub, len(cmd.UpsertSubs.Subs))
	for i := range cmd.UpsertSubs.Subs {
		upsertSubs[i] = Sub{
			ID: cmd.UpsertSubs.Subs[i].ID,
		}
	}

	return c.UpsertSubscriptions(ctx, SubsInfo{
		DefaultConfig: NewBoiler(c.Client).SubConfig(topic.ID()),
		Subs:          upsertSubs,
	})
}
