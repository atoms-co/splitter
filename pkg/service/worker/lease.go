package worker

import (
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/chanx"
	"time"
)

// Lease is a lease, possibly renewable.
type Lease interface {
	Expiration() time.Time
}

// RenewableLease is a renewable lease with an expiration timer. Not thread-safe.
type RenewableLease struct {
	ttl    time.Time
	expire *clock.Timer
}

func NewRenewableLease(ttl time.Time, expire *clock.Timer) *RenewableLease {
	ret := &RenewableLease{}
	ret.Renew(ttl, expire)
	return ret
}

func (s *RenewableLease) Expiration() time.Time {
	return s.ttl
}

func (s *RenewableLease) Renew(ttl time.Time, expire *clock.Timer) {
	s.stop()
	s.ttl = ttl
	s.expire = expire
}

func (s *RenewableLease) stop() {
	if s.expire != nil {
		if s.expire.Stop() {
			chanx.Clear(s.expire.C)
		}
	}
	s.expire = nil
}
