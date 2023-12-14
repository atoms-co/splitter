package model

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/clockx"
	"go.atoms.co/lib/iox"
	"fmt"
	"sync"
	"time"
)

// LeaseState represents the current state of a grant.
type LeaseState string

const (
	// LeaseActive represents that the grant is active and under the (renewed) lease.
	LeaseActive LeaseState = "active"
	// LeaseRevoked represents that the grant has been revoked by the coordinator. Lease is detached.
	LeaseRevoked LeaseState = "revoked"
	// LeaseStale represents that the grant is active, but will lapse due to a coordinator disconnect. Most
	// grants will go back to active, but that is the coordinator's decision.
	LeaseStale LeaseState = "stale"
)

// grant holds a grant and its metadata and bookkeeping.
type grant struct {
	Grant      Grant // holds original lease
	Lease      *clockx.Timer
	LeaseState LeaseState
	Handler    *handler
}

func (g *grant) ToUpdated() Grant {
	return NewGrant(g.Grant.ID(), g.Grant.Shard(), g.Grant.State(), g.Lease.Ttl(), g.Grant.Assigned())
}

func (g *grant) String() string {
	return fmt.Sprintf("grant=%v, lease_state=%v, ttl=%v", g.Grant, g.LeaseState, g.Lease.Ttl().Unix())
}

type handler struct {
	iox.AsyncCloser

	cl  clock.Clock
	own *ownership

	drain iox.AsyncCloser
}

func newHandler(ctx context.Context, cl clock.Clock, grant Grant, expiration time.Time, handlerFn Handler) *handler {
	h := &handler{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		own:         newOwnership(cl, grant.State(), expiration),
		drain:       iox.NewAsyncCloser(),
	}

	hctx, cancel := context.WithCancel(ctx)
	go func() {
		defer h.Close()

		handlerFn(hctx, grant.ID(), grant.Shard(), h.own)
	}()

	go func() {
		defer cancel()
		defer h.Ownership().Expire()

		select {
		case <-h.drain.Closed():
		case <-h.Closed():
		}
	}()

	return h
}

func (h *handler) Ownership() *ownership {
	return h.own
}

func (h *handler) Drain(timeout time.Duration) {
	h.drain.Close()
	h.cl.AfterFunc(timeout, h.Close)
}

type ownership struct {
	cl      clock.Clock
	active  iox.AsyncCloser
	revoked iox.AsyncCloser
	expired iox.AsyncCloser

	mu         sync.RWMutex
	expiration time.Time
}

func newOwnership(cl clock.Clock, state GrantState, expiration time.Time) *ownership {
	ret := &ownership{
		cl:         cl,
		active:     iox.NewAsyncCloser(),
		revoked:    iox.NewAsyncCloser(),
		expired:    iox.NewAsyncCloser(),
		expiration: expiration,
	}

	// Initialize correct signals using GrantState
	switch state {
	case ActiveGrantState:
		ret.active.Close()
	case RevokedGrantState:
		ret.revoked.Close()
	}
	// If past expiration preload expiration signal
	if cl.Now().After(expiration) {
		ret.expired.Close()
	}

	return ret
}

func (o *ownership) Activate() {
	o.active.Close()
}

func (o *ownership) Revoke() {
	o.revoked.Close()
}

func (o *ownership) Expire() {
	o.expired.Close()
}

func (o *ownership) SetExpiration(expiration time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.expiration = expiration
}

func (o *ownership) Active() <-chan struct{} {
	return o.active.Closed()
}

func (o *ownership) Revoked() <-chan struct{} {
	return o.revoked.Closed()
}

func (o *ownership) Expired() <-chan struct{} {
	return o.expired.Closed()
}

func (o *ownership) IsActive() bool {
	return o.active.IsClosed()
}

func (o *ownership) IsRevoked() bool {
	return o.revoked.IsClosed()
}

func (o *ownership) IsExpired() bool {
	return o.expired.IsClosed()
}

func (o *ownership) Expiration() time.Time {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.expiration
}
