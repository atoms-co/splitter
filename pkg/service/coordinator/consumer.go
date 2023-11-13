package coordinator

import (
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/util/sessionx"
	"fmt"
	"time"
)

type Consumer struct {
	instance   model.Instance
	joined     time.Time
	expiration time.Time
}

func NewConsumer(instance model.Instance, joined time.Time) Consumer {
	return Consumer{instance: instance, joined: joined}
}

func (c Consumer) ID() model.InstanceID {
	return c.instance.ID()
}

func (c Consumer) Instance() model.Instance {
	return c.instance
}

func (c Consumer) Region() model.Region {
	return model.Region(c.instance.Location().Region)
}

func (c Consumer) Joined() time.Time {
	return c.joined
}

func (c Consumer) Expiration() time.Time {
	return c.expiration
}

func (c Consumer) String() string {
	return fmt.Sprintf("consumer{id=%v, joined=%v, expiration=%v}", c.instance, c.joined, c.expiration)
}

type consumerSession struct {
	consumer   Consumer
	connection sessionx.Connection[model.ConsumerMessage]
}

func (c *consumerSession) ID() model.ConsumerID {
	return c.consumer.ID()
}

func (c *consumerSession) String() string {
	return fmt.Sprintf("session{consumer=%v, connection=%v}", c.consumer, c.connection)
}
