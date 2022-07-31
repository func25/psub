package psub

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/func25/gop"
)

type MsgHandler func(context.Context, *pubsub.Message) error

func (c *PsubClient) UpsertSubscriptions(ctx context.Context, cmd UpsertSubsCommand) error {
	for _, sub := range cmd.Subs {
		subscription := c.Subscription(sub.ID)

		exist, err := subscription.Exists(ctx)
		if err != nil {
			return err
		}

		if exist {
			continue
		}

		config := cmd.DefaultConfig
		if sub.Config != nil {
			config = *sub.Config
		}

		subscription, err = c.CreateSubscription(ctx, sub.ID, config)
		if err != nil {
			return err
		}
	}

	return nil
}

// Subscribe to the subscription
func (c *PsubClient) Subscribe(subID string, fn MsgHandler, opts ...*SubscribeOption) error {
	clone, err := c.newClientFunc()
	if err != nil {
		return err
	}

	sub := clone.Subscription(subID)

	ctx, cancel := context.WithCancel(context.Background())

	// apply options
	opt := mergeSubscribeOption(opts...)

	subscriber := &Subscriber{
		Sub:        sub,
		CancelFunc: cancel,
		cfg:        opt,
	}

	go gop.SafeGo(func() { newClient(clone).subscribe(ctx, subscriber, fn) })

	return nil
}

func (c *PsubClient) subscribe(ctx context.Context, subscriber *Subscriber, fn MsgHandler) error {
	id := subscriber.ID

	var err error = nil
	retry := false
	ackErr := false
	if subscriber.cfg.RetrySubscribe != nil {
		retry = *subscriber.cfg.RetrySubscribe
	}
	if subscriber.cfg.ACKErr != nil {
		ackErr = *subscriber.cfg.ACKErr
	}

	for ok := true; ok; {
		c.Log("[PSUB-debug] start pulling", id)
		err = subscriber.Sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			err := fn(ctx, msg)

			if err != nil {
				if ackErr {
					msg.Ack()
				} else {
					msg.Nack()
				}
			} else {
				msg.Ack()
			}
		})

		c.Log("[PSUB-debug] error while pulling message of", id, " - ", err)
		ok = retry
		time.Sleep(1 * time.Second)
	}

	return err
}
