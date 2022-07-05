package psub

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type PsubClient struct {
	*pubsub.Client
	mtx          sync.Mutex
	_subscribers map[string]*Subscriber
	topics       map[string]*pubsub.Topic
	isLog        bool
}

type Subscriber struct {
	ID         string
	Sub        *pubsub.Subscription
	CancelFunc context.CancelFunc
	cfg        *SubscribeOption
}

var Client PsubClient

func Connect(ctx context.Context, projectID string, opts ...option.ClientOption) (*PsubClient, error) {
	var err error
	c, err := pubsub.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, err
	}

	return &PsubClient{Client: c}, nil
}

func ForceClient(client *pubsub.Client) *PsubClient {
	return &PsubClient{
		Client: client,
	}
}

func (c *PsubClient) SetLog(isLog bool) {
	c.isLog = isLog
}

func (c *PsubClient) Log(a ...any) {
	if c.isLog {
		fmt.Println(a...)
	}
}

func (c *PsubClient) setSubcriber(subID string, s *Subscriber) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c._subscribers[subID] = s
}

func (c *PsubClient) removeSubcribers(subID string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	delete(c._subscribers, subID)
}
