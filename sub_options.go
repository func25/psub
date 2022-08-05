package psub

type SubscribeOption struct {
	ACKErr         *bool
	RetrySubscribe *bool // default: true
}

func NewSubscribeOption() *SubscribeOption {
	retry := true
	return &SubscribeOption{
		RetrySubscribe: &retry,
	}
}

func (s *SubscribeOption) ACKAll(ack bool) *SubscribeOption {
	s.ACKErr = &ack
	return s
}

func (s *SubscribeOption) Retry(retry bool) *SubscribeOption {
	s.RetrySubscribe = &retry
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
	}

	return opt
}
