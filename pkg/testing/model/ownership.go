package model

import (
	"time"

	"go.atoms.co/lib/iox"
	splitter "go.atoms.co/splitter/pkg/model"
)

type Ownership struct {
	active          iox.AsyncCloser
	waitingActive   iox.AsyncCloser
	revokeRequested iox.AsyncCloser
	revoked         iox.AsyncCloser
	waitingRevoked  iox.AsyncCloser
	expired         iox.AsyncCloser
	waitingExpired  iox.AsyncCloser

	loader   splitter.Loader
	unloader splitter.Unloader

	expiration time.Time
}

type OwnershipOption func(*Ownership)

func OwnershipWithLoader(loader splitter.Loader) OwnershipOption {
	return func(o *Ownership) {
		o.loader = loader
	}
}

func OwnershipWithUnloader(unloader splitter.Unloader) OwnershipOption {
	return func(o *Ownership) {
		o.unloader = unloader
	}
}

func OwnershipWithExpiration(expiration time.Time) OwnershipOption {
	return func(o *Ownership) {
		o.expiration = expiration
	}
}

func NewOwnership(opts ...OwnershipOption) *Ownership {
	o := &Ownership{
		active:          iox.NewAsyncCloser(),
		waitingActive:   iox.NewAsyncCloser(),
		revokeRequested: iox.NewAsyncCloser(),
		revoked:         iox.NewAsyncCloser(),
		waitingRevoked:  iox.NewAsyncCloser(),
		expired:         iox.NewAsyncCloser(),
		waitingExpired:  iox.NewAsyncCloser(),
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func (o *Ownership) Active() iox.RAsyncCloser {
	o.waitingActive.Close()
	return o.active
}

func (o *Ownership) Activate() {
	o.active.Close()
}

func (o *Ownership) WaitingForActive() iox.RAsyncCloser {
	return o.waitingActive
}

func (o *Ownership) Revoked() iox.RAsyncCloser {
	o.waitingRevoked.Close()
	return o.revoked
}

func (o *Ownership) Revoke() {
	o.revoked.Close()
}

func (o *Ownership) WaitingForRevoked() iox.RAsyncCloser {
	return o.waitingRevoked
}

func (o *Ownership) Expired() iox.RAsyncCloser {
	o.waitingExpired.Close()
	return o.expired
}

func (o *Ownership) Expire() {
	o.expired.Close()
}

func (o *Ownership) WaitingForExpired() iox.RAsyncCloser {
	return o.waitingExpired
}

func (o *Ownership) Loader() splitter.Loader {
	return o.loader
}

func (o *Ownership) Unloader() splitter.Unloader {
	return o.unloader
}

func (o *Ownership) Expiration() time.Time {
	return o.expiration
}

func (o *Ownership) RequestRevoke() {
	o.revokeRequested.Close()
}
