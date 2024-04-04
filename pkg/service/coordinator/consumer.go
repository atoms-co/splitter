package coordinator

import (
	"context"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/util/sessionx"
	"fmt"
	"time"
)

var (
	numMessages = metrics.NewCounter("go.atoms.co/splitter/coordinator_messages", "Coordinator messages", core.MessageTypeKey)
)

type Consumer struct {
	instance   model.Instance
	joined     time.Time
	canaryKeys []model.QualifiedDomainKey
}

func NewConsumer(instance model.Instance, joined time.Time, keys ...model.QualifiedDomainKey) Consumer {
	return Consumer{instance: instance, joined: joined, canaryKeys: keys}
}

func (c Consumer) ID() model.InstanceID {
	return c.instance.ID()
}

func (c Consumer) Instance() model.Instance {
	return c.instance
}

func (c Consumer) Region() model.Region {
	return c.instance.Location().Region
}

func (c Consumer) IsCanary() bool {
	return len(c.canaryKeys) > 0
}

func (c Consumer) CanaryKeys() []model.QualifiedDomainKey {
	return slicex.Clone(c.canaryKeys)
}

func (c Consumer) Joined() time.Time {
	return c.joined
}

func (c Consumer) String() string {
	return fmt.Sprintf("%v[joined=%v]", c.instance, c.joined)
}

type consumerSession struct {
	consumer   Consumer
	draining   bool
	connection sessionx.Connection[model.ConsumerMessage]
}

func (c *consumerSession) TrySend(ctx context.Context, message model.ConsumerMessage) bool {
	if c.connection.Send(ctx, message) {
		numMessages.Increment(ctx, 1, core.MessageTypeTag(message.Type()))
		return true
	}
	return false
}

func (c *consumerSession) ID() model.ConsumerID {
	return c.consumer.ID()
}

func (c *consumerSession) String() string {
	return fmt.Sprintf("%v[consumer=%v]", c.connection.Sid(), c.consumer)
}
