package model

import (
	"context"
	"fmt"
	"time"

	"go.atoms.co/lib/timex"
	"go.atoms.co/iox"
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
	Lease      *timex.Timer
	LeaseState LeaseState
	Handler    *handler
}

func (g *grant) Expiration() time.Time {
	return g.Lease.TTL()
}

func (g *grant) ToState(state GrantState) Grant {
	return NewGrant(g.Grant.ID(), g.Grant.Shard(), state, g.Lease.TTL(), g.Grant.Assigned())
}

func (g *grant) ToUpdated() Grant {
	return NewGrant(g.Grant.ID(), g.Grant.Shard(), g.Grant.State(), g.Lease.TTL(), g.Grant.Assigned())
}

func (g *grant) String() string {
	return fmt.Sprintf("grant=%v, lease_state=%v, ttl=%v", g.Grant, g.LeaseState, g.Lease.TTL().Unix())
}

type handler struct {
	iox.AsyncCloser

	ownership *ownership
}

func newHandler(ctx context.Context, grant Grant, expiration func() time.Time, loader *loader, unloader *unloader, handlerFn Handler) *handler {
	h := &handler{
		AsyncCloser: iox.NewAsyncCloser(),
		ownership:   newOwnership(grant.State(), expiration, loader, unloader),
	}

	hctx, cancel := context.WithCancel(ctx)
	go func() {
		defer h.Close()

		handlerFn(hctx, grant.ID(), grant.Shard(), h.ownership)
	}()

	go func() {
		<-h.Closed()

		cancel()
		h.ownership.expire()
	}()

	return h
}

func (h *handler) Ownership() Ownership {
	return h.ownership
}

func (h *handler) Drain(timeout time.Duration) {
	time.AfterFunc(timeout, h.Close)
}

type ownership struct {
	active          iox.AsyncCloser
	revokeRequested iox.AsyncCloser
	revoked         iox.AsyncCloser
	expired         iox.AsyncCloser
	loader          *loader
	unloader        *unloader
	expiration      func() time.Time
}

func newOwnership(state GrantState, expiration func() time.Time, loader *loader, unloader *unloader) *ownership {
	ret := &ownership{
		active:          iox.NewAsyncCloser(),
		revokeRequested: iox.NewAsyncCloser(),
		revoked:         iox.NewAsyncCloser(),
		expired:         iox.NewAsyncCloser(),
		loader:          loader,
		unloader:        unloader,
		expiration:      expiration,
	}

	// Initialize correct signals using GrantState
	switch state {
	case ActiveGrantState:
		ret.active.Close()
	case RevokedGrantState:
		ret.revoked.Close()
	}
	// If past expiration preload expiration signal
	if time.Now().After(expiration()) {
		ret.expired.Close()
	}

	return ret
}

func (o *ownership) Active() iox.RAsyncCloser {
	return o.active
}

func (o *ownership) Revoked() iox.RAsyncCloser {
	return o.revoked
}

func (o *ownership) Expired() iox.RAsyncCloser {
	return o.expired
}

func (o *ownership) activate() {
	o.active.Close()
}

func (o *ownership) revoke() {
	o.revoked.Close()
}

func (o *ownership) expire() {
	o.expired.Close()
}

func (o *ownership) Loader() Loader {
	return o.loader
}

func (o *ownership) Unloader() Unloader {
	return o.unloader
}

func (o *ownership) Expiration() time.Time {
	return o.expiration()
}

func (o *ownership) RequestRevoke() {
	o.revokeRequested.Close()
}

type loader struct {
	unloaded iox.AsyncCloser
	load     iox.AsyncCloser
}

func newLoader() *loader {
	return &loader{
		unloaded: iox.NewAsyncCloser(),
		load:     iox.NewAsyncCloser(),
	}
}

func (l *loader) Unloaded() iox.RAsyncCloser {
	return l.unloaded
}

func (l *loader) Load() {
	l.load.Close()
}

func (l *loader) unload() {
	l.unloaded.Close()
}

func (l *loader) loaded() iox.RAsyncCloser {
	return l.load
}

type unloader struct {
	loaded iox.AsyncCloser
	unload iox.AsyncCloser
}

func newUnloader() *unloader {
	return &unloader{
		loaded: iox.NewAsyncCloser(),
		unload: iox.NewAsyncCloser(),
	}
}

func (u *unloader) Loaded() iox.RAsyncCloser {
	return u.loaded
}

func (u *unloader) Unload() {
	u.unload.Close()
}

func (u *unloader) load() {
	u.loaded.Close()
}

func (u *unloader) unloaded() iox.RAsyncCloser {
	return u.unload
}
