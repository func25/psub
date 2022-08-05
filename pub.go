package psub

import (
	"context"

	"cloud.google.com/go/pubsub"
)

type SubsInfo struct {
	Subs          []Sub
	DefaultConfig pubsub.SubscriptionConfig
}

type Sub struct {
	ID     string
	Config *pubsub.SubscriptionConfig
}

func (c *PsubConnection) UpsertTopic(ctx context.Context, topicID string) (*pubsub.Topic, error) {
	topic := c.Topic(topicID)

	exist, err := topic.Exists(ctx)
	if err != nil || exist {
		return topic, err
	}

	topic, err = c.CreateTopic(ctx, topicID)
	if err != nil {
		return nil, err
	}

	c.topics[topicID] = topic
	return topic, nil
}

func (c *PsubConnection) SettingTopic(ctx context.Context, topicID string, setting pubsub.PublishSettings) {
	v, exists := c.topics[topicID]
	if exists {
		v.PublishSettings = setting
	}

	topic := c.Topic(topicID)
	topic.PublishSettings = setting

	c.topics[topicID] = topic
}

func (c *PsubConnection) PublishRaw(ctx context.Context, topicID string, data []byte) error {
	if data == nil {
		c.Log("[PSUB-debug] Publish topic", topicID, "failed: data is nil")
		return nil
	}

	topic, exist := c.topics[topicID]
	if !exist {
		topic = c.Topic(topicID)
	}

	message := pubsub.Message{
		Data: data,
	}

	result := topic.Publish(ctx, &message)
	_, err := result.Get(ctx)
	if err != nil {
		return err
	}

	c.Log("[PSUB-debug] Publish topic", topicID, "successfully:", string(message.Data))
	return nil
}

func (c *PsubConnection) Publish(ctx context.Context, topicID string, message Message) error {
	if message.Data == nil {
		c.Log("[PSUB-debug] Publish topic", topicID, "failed: data is nil")
		return nil
	}

	topic, exist := c.topics[topicID]
	if !exist {
		topic = c.Topic(topicID)
	}

	result := topic.Publish(ctx, message.Message)
	_, err := result.Get(ctx)
	if err != nil {
		c.Log("[PSUB-debug] Publish topic", topicID, "failed:", err)
		return err
	}

	c.Log("[PSUB-debug] Publish topic", topicID, "successfully:", string(message.Message.Data))

	return nil
}
