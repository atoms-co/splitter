package leader

import (
	"context"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/util/sessionx"
	"fmt"
)

var (
	numMessages = metrics.NewCounter("go.atoms.co/splitter/leader_messages", "Leader messages", core.MessageTypeKey)
)

type workerSession struct {
	instance   model.Instance
	draining   bool
	connection sessionx.Connection[Message]
}

func (w *workerSession) TrySend(ctx context.Context, message Message) bool {
	if w.connection.Send(ctx, message) {
		numMessages.Increment(ctx, 1, core.MessageTypeTag(message.Type()))
		return true
	}
	return false
}

func (w *workerSession) String() string {
	return fmt.Sprintf("session{instance=%v, connection=%v}", w.instance, w.connection)
}
