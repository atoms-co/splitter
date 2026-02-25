package coordinator

import (
	"context"
	"fmt"
	"time"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/util/sessionx"
)

var (
	numMessages = metrics.NewCounter("go.atoms.co/splitter/coordinator_messages", "Coordinator messages", core.MessageTypeKey)
)

type Consumer struct {
	instance model.Instance
	joined   time.Time
	keys     []qualifiedDomainKeyWithName
	limit    int
}

type NewConsumerOption func(*Consumer)

func withKeys(keys ...qualifiedDomainKeyWithName) NewConsumerOption {
	return func(c *Consumer) {
		c.keys = keys
	}
}

func WithLimit(limit int) NewConsumerOption {
	return func(c *Consumer) {
		c.limit = limit
	}
}

func NewConsumer(instance model.Instance, joined time.Time, opts ...NewConsumerOption) *Consumer {
	ret := &Consumer{instance: instance, joined: joined}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func (c *Consumer) ID() model.InstanceID {
	return c.instance.ID()
}

func (c *Consumer) Instance() model.Instance {
	return c.instance
}

func (c *Consumer) Region() model.Region {
	return c.instance.Location().Region
}

func (c *Consumer) Keys() []model.QualifiedDomainKey {
	return slicex.Map(c.keys, qualifiedDomainKeyWithName.Key)
}

func (c *Consumer) KeyNames() []model.DomainKeyName {
	return slicex.Map(c.keys, qualifiedDomainKeyWithName.Name)
}

func (c *Consumer) Limit() int {
	return c.limit
}

func (c *Consumer) Joined() time.Time {
	return c.joined
}

func (c *Consumer) String() string {
	return fmt.Sprintf("%v[joined=%v, keys=%v]", c.instance, c.joined, c.keys)
}

type consumerSession struct {
	consumer   *Consumer
	draining   bool
	connection sessionx.Connection[model.ConsumerMessage]
	origin     location.Instance
	suspended  bool // prevents auto-resume when explicitly suspended via coordinator operations
	verbose    bool
}

func (c *consumerSession) TrySend(ctx context.Context, message model.ConsumerMessage) bool {
	if c.connection.Send(ctx, message) {
		if c.verbose {
			log.Debugf(ctx, "Sent message to %v: %v", c.consumer.Instance(), message)
		}
		numMessages.Increment(ctx, 1, core.MessageTypeTag(message.Type()))
		return true
	}
	log.Debugf(ctx, "Failed to send message to %v: %v", c.consumer.Instance(), message)
	return false
}

func (c *consumerSession) ID() model.ConsumerID {
	return c.consumer.ID()
}

func (c *consumerSession) String() string {
	return fmt.Sprintf("%v[consumer=%v, origin=%v]", c.connection.Sid(), c.consumer, c.origin)
}

type qualifiedDomainKeyWithName struct {
	key  model.QualifiedDomainKey
	name model.DomainKeyName
}

func (k qualifiedDomainKeyWithName) Key() model.QualifiedDomainKey {
	return k.key
}

func (k qualifiedDomainKeyWithName) Name() model.DomainKeyName {
	return k.name
}

func (k qualifiedDomainKeyWithName) String() string {
	return fmt.Sprintf("%v[%v]", k.key, k.name)
}
