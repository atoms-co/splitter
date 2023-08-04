package leader

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pb/private"
)

type worker struct {
	id model.Instance

	out chan<- JoinMessage
}

// Leader centralizes tenant and storage coordination. All updates go through the global leader, which is
// dynamically selected and may be present at different nodes at different times.
type Leader struct {
	iox.AsyncCloser

	cl clock.Clock
	id location.Instance
	db *storage.Storage

	workers map[location.InstanceID]*worker

	inject chan func()

	initialized, draining iox.AsyncCloser
}

func NewLeader(ctx context.Context, cl clock.Clock, loc location.Location, db *storage.Storage) *Leader {
	ret := &Leader{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		id:          location.NewInstance(loc),
		db:          db,
		workers:     map[location.InstanceID]*worker{},
		inject:      make(chan func()),
		initialized: iox.NewAsyncCloser(),
		draining:    iox.WithQuit(ctx.Done(), iox.NewAsyncCloser()), // context cancel => drain
	}

	return ret
}

func (l *Leader) Join(ctx context.Context, sid session.ID, id model.Instance, grants []Grant, in <-chan JoinMessage) (<-chan JoinMessage, error) {
	return nil, nil
}

func (l *Leader) Handle(ctx context.Context, request HandleRequest) (*internal_v1.LeaderHandleResponse, error) {
	return nil, nil
}

func (l *Leader) init(ctx context.Context) {
	defer l.Close()
	defer l.draining.Close()
	defer l.initialized.Close()

	log.Infof(ctx, "Leader created: %v", l.id)

	// load

	l.initialized.Close()
	log.Infof(ctx, "Leader initialized: %v", l.id)

	l.process(ctx)

	log.Infof(ctx, "Leader exited: %v", l.id)
}

func (l *Leader) process(ctx context.Context) {

}
