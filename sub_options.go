package psub

type SubscribeOption struct {
	ACKErr         *bool
	RetrySubscribe *bool
}

func NewSubscribeOption() *SubscribeOption {
	return &SubscribeOption{}
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
	opt := &SubscribeOption{}
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
