package psub

import (
	"strconv"

	"cloud.google.com/go/pubsub"
)

type Message struct {
	*pubsub.Message
}

const (
	_version = "version"
	_feature = "feature"
)

func NewMessage(data []byte) *Message {
	return &Message{
		Message: &pubsub.Message{Data: data},
	}
}

func (m *Message) SetVersion(v int) *Message {
	if m.Attributes == nil {
		m.Attributes = make(map[string]string, 1)
	}

	m.Attributes[_version] = strconv.Itoa(v)

	return m
}

func (m *Message) GetVersion() int {
	if m.Attributes == nil {
		return 0
	}

	v, _ := strconv.Atoi(m.Attributes[_version])
	return v
}

func (m *Message) SetFeature(f string) *Message {
	if m.Attributes == nil {
		m.Attributes = make(map[string]string, 1)
	}

	m.Attributes[_feature] = f

	return m
}

func (m *Message) GetFeature() string {
	if m.Attributes == nil {
		return ""
	}

	return m.Attributes[_feature]
}

func (m *Message) SetDeliveryOrder(order string) *Message {
	m.OrderingKey = order

	return m
}

func (m *Message) SetStr(key, value string) *Message {
	if m.Attributes == nil {
		m.Attributes = make(map[string]string, 1)
	}

	m.Attributes[key] = value

	return m
}

func (m *Message) Get(key string) string {
	return m.Attributes[key]
}
