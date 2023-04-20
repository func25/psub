package psub

import (
	"context"

	"cloud.google.com/go/pubsub"
)

type SubscribeOption struct {
	ACKErr          *bool
	RetrySubscribe  *bool                                                // default: true
	IsLog           *bool                                                //default: false
	DeduplicateFunc func(context.Context, *pubsub.Message) (bool, error) // return true if message duplicated
	ACKHook         func(*pubsub.Message)
	NACKHook        func(*pubsub.Message)
	ReceiveSettings *pubsub.ReceiveSettings
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

func (s *SubscribeOption) SetIsLog(isLog bool) *SubscribeOption {
	s.IsLog = &isLog
	return s
}

func (s *SubscribeOption) SetDeduplicate(isDuplicateFunc func(context.Context, *pubsub.Message) (bool, error)) *SubscribeOption {
	s.DeduplicateFunc = isDuplicateFunc
	return s
}

// SetACKHook not apply for deduplicate ack()
func (s *SubscribeOption) SetACKHook(f func(*pubsub.Message)) *SubscribeOption {
	s.ACKHook = f
	return s
}

func (s *SubscribeOption) SetNACKHook(f func(*pubsub.Message)) *SubscribeOption {
	s.NACKHook = f
	return s
}

func (s *SubscribeOption) SetReceiveSettings(settings pubsub.ReceiveSettings) *SubscribeOption {
	s.ReceiveSettings = &settings
	return s
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

		if opts[i].IsLog != nil {
			opt.IsLog = opts[i].IsLog
		}

		if opts[i].DeduplicateFunc != nil {
			opt.DeduplicateFunc = opts[i].DeduplicateFunc
		}

		if opts[i].ACKHook != nil {
			opt.ACKHook = opts[i].ACKHook
		}

		if opts[i].NACKHook != nil {
			opt.NACKHook = opts[i].NACKHook
		}

		if opts[i].ReceiveSettings != nil {
			opt.ReceiveSettings = opts[i].ReceiveSettings
		}
	}

	return opt
}
