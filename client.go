package psub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type PsubConnection struct {
	*pubsub.Client
	topics map[string]*pubsub.Topic // topics that have custom publishSetting
	isLog  bool

	newClientFunc func() (*pubsub.Client, error)
}

type Subscriber struct {
	ID         string
	Sub        *pubsub.Subscription
	CancelFunc context.CancelFunc
	cfg        *SubscribeOption
}

var Connection *PsubConnection // for singleton usage

func Connect(ctx context.Context, projectID string, opts ...option.ClientOption) (*PsubConnection, error) {
	var err error

	f := func() (*pubsub.Client, error) { return pubsub.NewClient(ctx, projectID, opts...) }

	c, err := f()
	if err != nil {
		return nil, err
	}

	if Connection == nil {
		Connection = &PsubConnection{
			Client:        c,
			newClientFunc: f,
		}
	}

	return &PsubConnection{
		Client:        c,
		topics:        make(map[string]*pubsub.Topic),
		newClientFunc: f,
	}, nil
}

func newClient(client *pubsub.Client) *PsubConnection {
	newClient := &PsubConnection{
		Client: client,
		topics: make(map[string]*pubsub.Topic),
	}

	return newClient
}

func (c *PsubConnection) SetLog(isLog bool) {
	c.isLog = isLog
}

func (c *PsubConnection) Log(a ...any) {
	if c.isLog {
		fmt.Println(a...)
	}
}
