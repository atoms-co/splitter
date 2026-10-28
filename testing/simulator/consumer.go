package simulator

import (
	"time"

	"go.atoms.co/splitter/pkg/model"
)

type consumer struct {
	region     string
	slot       int
	instance   model.Instance
	in         chan model.ConsumerMessage
	connected  bool
	generation int
	lease      time.Time
	grants     map[model.GrantID]*grant
}

type grant struct {
	assignment model.Grant
	state      model.GrantState
	lease      time.Time
	ready      bool
	loading    bool
	serving    bool
	peerLoaded bool
	// confirmedGeneration is set only by Assign on the current connection.
	// Retained grants must expire on their old lease until reconfirmed.
	confirmedGeneration int
	expiry              *time.Timer
}

func (g *grant) value() model.Grant {
	return model.NewGrant(g.assignment.ID(), g.assignment.Shard(), g.state, g.lease, g.assignment.Assigned())
}

func (s *simulation) handle(c *consumer, msg model.ConsumerMessage) {
	client, ok := msg.ClientMessage()
	if !ok {
		return
	} // Cluster broadcasts do not drive consumer ownership.
	switch {
	case client.IsExtend():
		extend, _ := client.Extend()
		c.lease = extend.Lease()
		for _, g := range c.grants {
			if g.confirmedGeneration == c.generation && !model.IsRevokedOrUnloaded(g.state) {
				g.lease = c.lease
				g.expiry.Reset(max(time.Until(g.lease), 0))
			}
		}
	case client.IsAssign():
		assign, _ := client.Assign()
		for _, a := range assign.Grants() {
			g, exists := c.grants[a.ID()]
			if !exists {
				g = &grant{assignment: a, state: a.State(), lease: a.Lease()}
				c.grants[a.ID()] = g
				g.expiry = time.AfterFunc(max(time.Until(g.lease), 0), func() { s.post(func() { s.expire(c, a.ID(), g) }) })
				s.timers = append(s.timers, g.expiry)
				s.assigned(c, a)
			} else {
				g.state = a.State()
				g.lease = a.Lease()
				g.expiry.Reset(max(time.Until(g.lease), 0))
			}
			g.confirmedGeneration = c.generation
			if g.state == model.ActiveGrantState {
				s.load(c, g)
			} else if g.ready {
				g.state = model.LoadedGrantState
				s.send(c, model.NewUpdate(g.value()))
			}
		}
	case client.IsRevoke():
		revoke, _ := client.Revoke()
		for _, a := range revoke.Grants() {
			g, ok := c.grants[a.ID()]
			if !ok {
				continue
			}
			g.state = model.RevokedGrantState
			g.lease = a.Lease()
			g.expiry.Reset(max(time.Until(g.lease), 0))
			s.after(0, func() {
				if c.grants[a.ID()] != g || g.state != model.RevokedGrantState {
					return
				}
				g.state = model.UnloadedGrantState
				s.send(c, model.NewUpdate(g.value()))
				s.releaseIfReady(c, g)
			})
		}
	case client.IsNotify():
		notify, _ := client.Notify()
		g, ok := c.grants[notify.Target().ID()]
		if !ok {
			return
		}
		switch notify.Update().State() {
		case model.UnloadedGrantState:
			s.load(c, g)
		case model.LoadedGrantState:
			g.peerLoaded = true
			s.releaseIfReady(c, g)
		}
	case client.IsPromote():
		promote, _ := client.Promote()
		for _, a := range promote.Grants() {
			if g, ok := c.grants[a.ID()]; ok {
				g.state = model.ActiveGrantState
				s.load(c, g)
			}
		}
	}
}

func (s *simulation) load(c *consumer, g *grant) {
	if g.state == model.ActiveGrantState {
		// Record active assignment provenance before the queued initialization
		// callback: the consumer can quit before it starts serving.
		s.owners[g.assignment.Shard()] = owner{consumer: string(c.instance.ID()), grant: g.assignment.ID()}
	}
	if g.ready {
		s.serve(c, g)
		return
	}
	if g.loading {
		return
	}
	g.loading = true
	s.after(0, func() {
		if c.grants[g.assignment.ID()] != g || !time.Now().Before(g.lease) {
			return
		}
		g.ready = true
		if model.IsAllocatedOrLoaded(g.state) {
			g.state = model.LoadedGrantState
			s.send(c, model.NewUpdate(g.value()))
		}
		s.serve(c, g)
	})
}

func (s *simulation) serve(c *consumer, g *grant) {
	if !g.ready || g.state != model.ActiveGrantState || g.serving || !time.Now().Before(g.lease) {
		return
	}
	g.serving = true
	// An initialized active destination completes a transfer. Initial assignments
	// and retained grants on reconnect are deliberately excluded.
	if transfer := s.transfers[g.assignment.ID()]; transfer != nil && transfer.Status == "pending" {
		transfer.Status = "completed"
		at := s.elapsed()
		transfer.CompletedAt = &at
	}
}

func (s *simulation) releaseIfReady(c *consumer, g *grant) {
	if g.state == model.UnloadedGrantState && g.peerLoaded && c.connected {
		s.send(c, model.NewReleased(g.value()))
		s.stopGrant(c, g.assignment.ID(), "interrupted")
	}
}

func (s *simulation) expire(c *consumer, id model.GrantID, g *grant) {
	if c.grants[id] != g {
		return
	}
	if time.Now().Before(g.lease) {
		g.expiry.Reset(time.Until(g.lease))
		return
	}
	s.stopGrant(c, id, "expired")
}

func (s *simulation) stopGrant(c *consumer, id model.GrantID, status string) {
	if g, ok := c.grants[id]; ok {
		g.expiry.Stop()
		delete(c.grants, id)
	}
	if transfer := s.transfers[id]; transfer != nil && transfer.Status == "pending" {
		transfer.Status = status
	}
}
