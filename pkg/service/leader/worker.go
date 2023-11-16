package leader

import (
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/util/sessionx"
	"fmt"
)

type workerSession struct {
	instance   model.Instance
	draining   bool
	connection sessionx.Connection[Message]
}

func (w *workerSession) String() string {
	return fmt.Sprintf("session{instance=%v, connection=%v}", w.instance, w.connection)
}
