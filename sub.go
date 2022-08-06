package psub

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/func25/gop"
)

type MsgHandler func(context.Context, *pubsub.Message) error

func (c *PsubConnection) UpsertSubscriptions(ctx context.Context, cmd SubsInfo) error {
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
func (c *PsubConnection) Subscribe(ctx context.Context, subID string, fn MsgHandler, opts ...*SubscribeOption) (*Subscriber, error) {
	clone, err := c.newClientFunc()
	if err != nil {
		return nil, err
	}

	sub := clone.Subscription(subID)

	ctx, cancel := context.WithCancel(ctx)

	// apply options
	opt := mergeSubscribeOption(opts...)

	subscriber := &Subscriber{
		Sub:        sub,
		CancelFunc: cancel,
		cfg:        opt,
	}

	go gop.SafeGo(func() { newClient(clone).subscribe(ctx, subscriber, fn) })

	return subscriber, nil
}

func (c *PsubConnection) subscribe(ctx context.Context, subscriber *Subscriber, fn MsgHandler) error {
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
	dedup := subscriber.cfg.DeduplicateFunc

	for ok := true; ok; {
		c.Log("[PSUB-debug] start pulling", id)
		err = subscriber.Sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			var err error
			if dedup != nil {
				var isDup bool
				isDup, err = dedup(ctx, msg)
				if isDup {
					msg.Ack()
					return
				}
			}

			if err == nil {
				err = fn(ctx, msg)
			} else {
				c.Log("[PSUB-error] error while deduplicating", id, err)
			}

			if err == nil || (err != nil && ackErr) {
				msg.Ack()
				return
			}

			msg.Nack()
		})

		c.Log("[PSUB-error] error while pulling message of", id, " - ", err)
		ok = retry
		time.Sleep(1 * time.Second)
	}

	return err
}
