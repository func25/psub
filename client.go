package psub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type PsubClient struct {
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

var Client *PsubClient // for singleton usage

func Connect(ctx context.Context, projectID string, opts ...option.ClientOption) (*PsubClient, error) {
	var err error

	f := func() (*pubsub.Client, error) { return pubsub.NewClient(ctx, projectID, opts...) }

	c, err := f()
	if err != nil {
		return nil, err
	}

	if Client == nil {
		Client = &PsubClient{
			Client:        c,
			newClientFunc: f,
		}
	}

	return &PsubClient{
		Client:        c,
		topics:        make(map[string]*pubsub.Topic),
		newClientFunc: f,
	}, nil
}

func ForceClient(client *pubsub.Client) *PsubClient {
	newClient := newClient(client)
	if Client == nil {
		Client = newClient
	}

	return newClient
}

func newClient(client *pubsub.Client) *PsubClient {
	newClient := &PsubClient{
		Client: client,
		topics: make(map[string]*pubsub.Topic),
	}

	return newClient
}

func (c *PsubClient) SetLog(isLog bool) {
	c.isLog = isLog
}

func (c *PsubClient) Log(a ...any) {
	if c.isLog {
		fmt.Println(a...)
	}
}
