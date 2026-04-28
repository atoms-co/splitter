package coordinator

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/iox"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
)

type observer struct {
	instance model.Instance
	service  model.QualifiedServiceName
	joined   time.Time
}

func newObserver(instance model.Instance, service model.QualifiedServiceName, joined time.Time) observer {
	return observer{
		instance: instance,
		service:  service,
		joined:   joined,
	}
}

func (o observer) ID() model.InstanceID {
	return o.instance.ID()
}

func (o observer) Instance() model.Instance {
	return o.instance
}

func (o observer) Service() model.QualifiedServiceName {
	return o.service
}

func (o observer) Joined() time.Time {
	return o.joined
}

func (o observer) String() string {
	return fmt.Sprintf("%v[service=%v, joined=%v]", o.instance, o.service, o.joined)
}

type observerSession struct {
	iox.AsyncCloser

	observer observer
	sid      session.ID
	out      chan<- core.ObserverServerMessage
	origin   location.Instance
	closed   atomic.Bool
}

func newObserverSession(sid session.ID, observer observer, out chan<- core.ObserverServerMessage, origin location.Instance, in <-chan core.ObserverClientMessage) *observerSession {
	s := &observerSession{
		AsyncCloser: iox.NewAsyncCloser(),
		observer:    observer,
		sid:         sid,
		out:         out,
		origin:      origin,
	}
	go func() {
		defer s.Close()
		for {
			select {
			case _, ok := <-in:
				if !ok {
					return
				}
			case <-s.Closed():
				return
			}
		}
	}()
	return s
}

func (o *observerSession) TrySend(ctx context.Context, message core.ObserverServerMessage) bool {
	if o.IsClosed() {
		return false
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case o.out <- message:
		log.Debugf(ctx, "Sent message to observer %v: %v", o.observer.Instance(), message)
		return true
	case <-o.Closed():
		return false
	case <-timer.C:
		o.Disconnect()
		return false
	}
}

func (o *observerSession) ID() model.InstanceID {
	return o.observer.ID()
}

func (o *observerSession) Disconnect() {
	o.Close()
	if o.closed.CompareAndSwap(false, true) {
		close(o.out)
	}
}

func (o *observerSession) String() string {
	return fmt.Sprintf("%v[observer=%v, origin=%v]", o.sid, o.observer, o.origin)
}
