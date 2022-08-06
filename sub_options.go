package psub

import (
	"context"

	"cloud.google.com/go/pubsub"
)

type SubscribeOption struct {
	ACKErr          *bool
	RetrySubscribe  *bool                                                // default: true
	DeduplicateFunc func(context.Context, *pubsub.Message) (bool, error) // return true if message duplicated
	ACKHook         func(*pubsub.Message)
	NACKHook        func(*pubsub.Message)
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

// SetACKHook not apply for deduplicate ack()
func (s *SubscribeOption) SetACKHook(f func(*pubsub.Message)) {
	s.ACKHook = f
}

func (s *SubscribeOption) SetNACKHook(f func(*pubsub.Message)) {
	s.NACKHook = f
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
