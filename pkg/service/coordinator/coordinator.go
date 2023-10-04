package coordinator

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/randx"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/core/distribution"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/util/txnx"
	"fmt"
	"time"
)

var (
	// leaseDuration is the duration of a consumer lease.
	leaseDuration = 90 * time.Second
)

// Coordinator is responsible for managing a single tenant. It accepts incoming connections from consumers and
// distributes work among the consumers by assigning shards with leases.
type Coordinator struct {
	iox.AsyncCloser

	cl     clock.Clock
	inject chan func()
	drain  iox.AsyncCloser

	tenant model.TenantName

	state        core.State
	distribution *distribution.Distribution[model.Shard, model.Grant]
	consumers    map[session.ID]*consumerSession
	in           chan consumerMessage
}

func New(ctx context.Context, cl clock.Clock, tenant model.TenantName, state core.State, stateUpdates <-chan core.Update) *Coordinator {
	shards := createShards(ctx, state)
	c := &Coordinator{
		AsyncCloser: iox.WithQuit(ctx.Done(), iox.NewAsyncCloser()),
		cl:          cl,
		inject:      make(chan func()),
		drain:       iox.NewAsyncCloser(),

		tenant: tenant,

		state:        state,
		distribution: distribution.NewDistribution[model.Shard, model.Grant](shardManager{}, cl.Now().Add(leaseDuration), shards),
		consumers:    map[session.ID]*consumerSession{},
		in:           make(chan consumerMessage, 1000),
	}
	ctx = log.NewContext(ctx, log.String("tenant", string(tenant)))
	go c.process(ctx, stateUpdates)
	return c
}

func (c *Coordinator) Connect(ctx context.Context, sid session.ID, register model.RegisterMessage, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error) {
	consumer, err := syncx.Txn1(ctx, txnx.Txn(c, c.inject), func() (*consumerSession, error) {
		consumer, ok := c.consumers[sid]
		if ok {
			consumer.Close()
		}
		consumer = newConsumerSession(c.cl, c.Closed(), sid, register.Instance(), c.cl.Now())

		err := c.distribution.Connect(consumer, 1, c.cl.Now().Add(time.Minute*10))
		if err != nil {
			log.Errorf(ctx, "Coordination %v is unable to connect a consumer %v: %v", c, register, err)
			return nil, err
		}

		go func() {
			select {
			case <-c.Closed():
				consumer.Close()
			case <-consumer.Closed():
				_, _, _ = c.distribution.Disconnect(c.cl.Now(), consumer.location.Location().ID())
			}
		}()

		c.consumers[sid] = consumer
		log.Infof(ctx, "%v connected consumer %v", c, consumer)
		return consumer, nil
	})
	if err != nil {
		return nil, err
	}
	go c.forward(ctx, in, consumer)
	return consumer.out, nil
}

func (c *Coordinator) Drain(timeout time.Duration) {
	c.drain.Close()
	c.cl.AfterFunc(timeout, c.Close)
}

func (c *Coordinator) String() string {
	return fmt.Sprintf("coordinator{tenant=%v}", c.tenant)
}

func (c *Coordinator) forward(ctx context.Context, in <-chan model.ConsumerMessage, consumer *consumerSession) {
	defer consumer.Close()
	for {
		select {
		case msg, ok := <-in:
			if !ok {
				return
			}
			select {
			case c.in <- newConsumerMessage(consumer.sid, msg):
			case <-c.Closed():
				log.Warnf(ctx, "Coordinator %v is closed. Dropping message %v", c, msg)
				return
			}
		case <-c.Closed():
			return
		}
	}
}

func (c *Coordinator) process(ctx context.Context, stateUpdates <-chan core.Update) {
	defer c.Close()

	ticker := c.cl.NewTicker(10*time.Second + randx.Duration(time.Second))
	defer ticker.Stop()

	for {
		select {
		case msg := <-c.in:
			c.handleConsumerMessage(ctx, msg.sid, msg.msg)
		case <-ticker.C:
			// TODO (styurin, 9/29/23): emit metrics
		case fn := <-c.inject:
			fn()
		case update, ok := <-stateUpdates:
			if !ok {
				return
			}
			c.handleStateUpdate(ctx, update)
		case <-c.drain.Closed():
			// drain
		case <-c.Closed():
			return
		}
	}
}

func (c *Coordinator) handleConsumerMessage(ctx context.Context, sid session.ID, msg model.ConsumerMessage) {
	consumer, ok := c.consumers[sid]
	if !ok {
		log.Errorf(ctx, "Unable to find consumer session for message %v", msg)
		return
	}
	switch {
	case msg.IsDeregister():
		log.Infof(ctx, "Coordinator %v is disconnecting consumer %v", c, consumer)
		c.tearDown(ctx, sid)
	case msg.IsReleased():
		released, _ := msg.Released()
		c.handleReleased(ctx, consumer, released)
	}
}

func (c *Coordinator) handleReleased(ctx context.Context, consumer *consumerSession, msg model.ReleasedMessage) {
	grants, err := msg.ParseGrants()
	if err != nil {
		log.Errorf(ctx, "Unable to parse grants from %v: %v", msg, err)
		return
	}
	var updated bool
	for _, g := range grants {
		released, err := c.distribution.Release(consumer.ID(), distribution.GrantID(g), c.cl.Now())
		if err != nil {
			log.Errorf(ctx, "Coordinator %v is unable to release a grant %v for consumer %v: %v", c, g, consumer, err)
		}
		updated = updated || released
	}
	log.Infof(ctx, "Coordinator %v released %v grants from consumer %s", c, len(grants), consumer)
}

func (c *Coordinator) handleStateUpdate(ctx context.Context, update core.Update) {
	if c.tenant != update.Name() {
		log.Errorf(ctx, "%v received an update for mismatching tenant: %v", c, update)
	}
	if upd, ok := update.TenantUpdated(); ok {
		c.handleTenantUpdated(ctx, upd)
	}
	if upd := update.DomainsUpdated(); len(upd) > 0 {
		c.handleDomainUpdated(ctx, upd)
	}
	if upd := update.DomainsRemoved(); len(upd) > 0 {
		c.handleDomainRemoved(ctx, upd)
	}
	if upd := update.PlacementsUpdated(); len(upd) > 0 {
		c.handlePlacementsUpdated(ctx, upd)
	}
	if upd := update.PlacementsRemoved(); len(upd) > 0 {
		c.handlePlacementsRemoved(ctx, upd)
	}
}

func (c *Coordinator) handleTenantUpdated(ctx context.Context, info model.TenantInfo) {
	log.Debugf(ctx, "%v received tenant update: %v", c, info)
	// TODO: detect if default region and default sharding policy are different and apply the changes
}

func (c *Coordinator) handleDomainUpdated(ctx context.Context, updated []model.DomainInfo) {
	log.Debugf(ctx, "%v received domain updates: %v", c, updated)
	// TODO: detect if a domain is added and add new shards to the distribution
	// TODO: detect if domain's state has changed and add/remove shards
	// TODO: detect if config has changed and update affected shards
}

func (c *Coordinator) handleDomainRemoved(ctx context.Context, removed []model.QualifiedDomainName) {
	log.Debugf(ctx, "%v received domain removals: %v", c, removed)
	// TODO: remove shards of removed domain
}

func (c *Coordinator) handlePlacementsUpdated(ctx context.Context, updated []core.InternalPlacementInfo) {
	log.Debugf(ctx, "%v received placement updates: %v", c, updated)
	// TODO: update shards affected by updated placements
}

func (c *Coordinator) handlePlacementsRemoved(ctx context.Context, removed []model.QualifiedPlacementName) {
	log.Debugf(ctx, "%v received placement removals: %v", c, removed)
	// TODO: update shards affected by removed placements
}

func (c *Coordinator) notifyConsumers() {
	// TODO: send cluster updates to consumers
}

func (c *Coordinator) tearDown(ctx context.Context, sid session.ID) {
	consumer, ok := c.consumers[sid]
	if !ok {
		return
	}
	log.Infof(ctx, "Coordinator %v is disconnecting consumer %v", c, consumer)
	delete(c.consumers, sid)
	consumer.Close()
}

func createShards(ctx context.Context, state core.State) []model.Shard {
	tenant := state.Tenant().Tenant()
	placements := state.Placements()
	return slicex.FlatMap(state.Domains(), func(info model.DomainInfo) []model.Shard {
		return createDomainShards(ctx, tenant, info.Domain(), placements)
	})
}

func createDomainShards(ctx context.Context, tenant model.Tenant, domain model.Domain, placements []core.InternalPlacementInfo) []model.Shard {
	var shards []model.Shard

	if domain.State() == model.DomainSuspended {
		return shards
	}

	switch domain.Type() {
	case model.Unit:
		r, _ := tenant.Region()
		shards = append(shards, model.Shard{
			Region: r,
			Domain: domain.Name(),
			To:     model.ZeroKey,
			From:   model.MaxKey,
		})
	case model.Global:
		if name, ok := domain.Config().Placement(); ok {
			placement, ok := slicex.First(placements, func(info core.InternalPlacementInfo) bool {
				pname := info.InternalPlacement().Name()
				return pname.Tenant == tenant.Name() && pname.Placement == name
			})
			if ok {
				if placement.InternalPlacement().State() == core.PlacementActive {
					shards = append(shards, createPlacementShards(domain, placement.InternalPlacement())...)
				}
			}
		} else {
			r, _ := tenant.Region()
			shards = append(shards, model.NewShards(r, domain.Name(), shardingPolicy(tenant, domain).Shards())...)
		}
	case model.Regional:
		domainShards := slicex.FlatMap(domain.Config().Regions(), func(r model.Region) []model.Shard {
			return model.NewShards(r, domain.Name(), domain.Config().ShardingPolicy().Shards())
		})
		shards = append(shards, domainShards...)
	default:
		log.Errorf(ctx, "Ignoring unsupported domain type for domain: %v", domain)
	}

	return shards
}

func createPlacementShards(domain model.Domain, placement core.InternalPlacement) []model.Shard {
	var shards []model.Shard
	// TODO: implement
	return shards
}

func shardingPolicy(tenant model.Tenant, domain model.Domain) model.ShardingPolicy {
	if domain.Config().ShardingPolicy() == model.WrapShardingPolicy(nil) {
		return tenant.Config().DefaultShardingPolicy()
	}
	return domain.Config().ShardingPolicy()
}

type consumerSession struct {
	iox.AsyncCloser

	cl clock.Clock

	sid        session.ID
	location   model.Instance
	joined     time.Time
	expiration time.Time

	out chan model.ConsumerMessage
}

func newConsumerSession(cl clock.Clock, quit <-chan struct{}, sid session.ID, loc model.Instance, joined time.Time) *consumerSession {
	return &consumerSession{
		AsyncCloser: iox.WithQuit(quit, iox.NewAsyncCloser()), // quit => closer
		cl:          cl,
		sid:         sid,
		location:    loc,
		joined:      joined,
		out:         make(chan model.ConsumerMessage, 100),
	}
}

func (c *consumerSession) ID() model.InstanceID {
	return c.location.Location().ID()
}

func (c *consumerSession) Region() model.Region {
	return model.Region(c.location.Location().Location().Region)
}

func (c *consumerSession) Joined() time.Time {
	return c.joined
}

func (c *consumerSession) Expiration() time.Time {
	return c.expiration
}

func (c *consumerSession) trySend(ctx context.Context, msg model.ConsumerMessage) bool {
	timer := c.cl.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case c.out <- msg:
		return true
	case <-c.Closed():
		return false
	case <-timer.C:
		log.Errorf(ctx, "Consumer connection %v is stuck. Closing", c)
		c.Close()
		return false
	}
}

func (c *consumerSession) String() string {
	return fmt.Sprintf("consumer{session=%v, location=%v}", c.sid, c.location)
}

type consumerMessage struct {
	sid session.ID
	msg model.ConsumerMessage
}

func newConsumerMessage(sid session.ID, msg model.ConsumerMessage) consumerMessage {
	return consumerMessage{sid: sid, msg: msg}
}

type shardManager struct {
}

func (m shardManager) NewGrant(shard model.Shard, state distribution.GrantState, assigned time.Time, expiration time.Time) model.Grant {
	return model.Grant{
		ID:       model.NewGrantID(),
		Shard:    shard,
		Assigned: assigned,
		Lease:    expiration,
	}
}

func (m shardManager) GrantWithShard(g model.Grant, shard model.Shard) model.Grant {
	g.Shard = shard
	return g
}

func (m shardManager) GrantWithExpiration(g model.Grant, expiration time.Time) model.Grant {
	g.Lease = expiration
	return g
}

func (m shardManager) GrantWithState(g model.Grant, state distribution.GrantState) model.Grant {
	newState := model.InvalidGrant
	switch state {
	case distribution.AllocatedGrant:
		newState = model.AllocatedGrant
	case distribution.ActiveGrant:
		newState = model.ActiveGrant
	case distribution.RevokedGrant:
		newState = model.RevokedGrant
	}
	g.State = newState
	return g
}

func (m shardManager) GrantShard(g model.Grant) model.Shard {
	return g.Shard
}

func (m shardManager) GrantAssigned(g model.Grant) time.Time {
	return g.Assigned
}

func (m shardManager) GrantExpiration(g model.Grant) time.Time {
	return g.Lease
}

func (m shardManager) GrantState(g model.Grant) distribution.GrantState {
	switch g.State {
	case model.AllocatedGrant:
		return distribution.AllocatedGrant
	case model.ActiveGrant:
		return distribution.ActiveGrant
	case model.RevokedGrant:
		return distribution.RevokedGrant
	default:
		return distribution.UnknownGrant
	}
}

func (m shardManager) GrantID(g model.Grant) distribution.GrantID {
	return distribution.GrantID(g.ID)
}

func (m shardManager) ShardsEqual(s1 model.Shard, s2 model.Shard) bool {
	return s1 == s2
}
