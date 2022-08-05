package psub

import (
	"context"

	"cloud.google.com/go/pubsub"
)

type SubscribeOption struct {
	ACKErr          *bool
	RetrySubscribe  *bool                                                // default: true
	DeduplicateFunc func(context.Context, *pubsub.Message) (bool, error) // return true if message duplicated
}

func NewSubscribeOption() *SubscribeOption {
	retry := true
	return &SubscribeOption{
		RetrySubscribe: &retry,
	}
}

func (s *SubscribeOption) SetACKAll(ack bool) *SubscribeOption {
	s.ACKErr = &ack
	return s
}

func (s *SubscribeOption) SetRetry(retry bool) *SubscribeOption {
	s.RetrySubscribe = &retry
	return s
}

func (s *SubscribeOption) SetDeduplicate(isDuplicateFunc func(context.Context, *pubsub.Message) (bool, error)) {
	s.DeduplicateFunc = isDuplicateFunc
}

func mergeSubscribeOption(opts ...*SubscribeOption) *SubscribeOption {
	opt := NewSubscribeOption()
	for i := range opts {
		if opts[i].ACKErr != nil {
			opt.ACKErr = opts[i].ACKErr
		}

		if opts[i].RetrySubscribe != nil {
			opt.RetrySubscribe = opts[i].RetrySubscribe
		}

		if opts[i].DeduplicateFunc != nil {
			opt.DeduplicateFunc = opts[i].DeduplicateFunc
		}
	}

	return opt
}
