package leader

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/iox"
)

// Leader centralizes tenant and storage coordination. All updates go through the leader, which is
// dynamically selected and may be present at different nodes at different times.
type Leader struct {
	iox.AsyncCloser

	cl       clock.Clock
	location location.Location

	inject chan func()

	initialized, draining iox.AsyncCloser
}

func NewLeader(ctx context.Context, cl clock.Clock, location location.Location) *Leader {
	ret := &Leader{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		inject:      make(chan func()),
		initialized: iox.NewAsyncCloser(),
		draining:    iox.NewAsyncCloser(),
	}

	return ret
}

func (l *Leader) init(ctx context.Context) {
	defer l.Close()
	defer l.draining.Close()
	defer l.initialized.Close()

	log.Infof(ctx, "Initializing leader")

	l.process(ctx)

}

func (l *Leader) process(ctx context.Context) {

}
