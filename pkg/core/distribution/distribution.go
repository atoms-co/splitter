package distribution

import (
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"math/rand"
	"time"
)

var (
	ErrConsumerAlreadyPresent = errors.New("consumer already present")
	ErrNotFound               = errors.New("not found")
	ErrInvalidGrant           = errors.New("invalid grant")
)

// Consumer is an instance that can process a shard allocation.
// A consumer can be located in a single region and can handle shards from multiple domains.
type Consumer interface {
	ID() model.InstanceID
	Instance() model.Instance
	Region() model.Region
	Joined() time.Time
	Expiration() time.Time
}

// Adapter converts operations required by Distribution to application-specific ones.
type Adapter[S comparable, G comparable] interface {
	// NewGrant creates a new grant with a new ID
	NewGrant(shard S, state GrantState, assigned time.Time, expiration time.Time) G

	// GrantWithShard create a new grant by copying an existing grant and replacing its shard
	GrantWithShard(g G, shard S) G
	// GrantWithAssigned create a new grant by copying an existing grant and replacing its assigned time
	GrantWithAssigned(g G, assigned time.Time) G
	// GrantWithExpiration create a new grant by copying an existing grant and replacing its expiration time
	GrantWithExpiration(g G, expiration time.Time) G
	// GrantWithState create a new grant by copying an existing grant and replacing its state
	GrantWithState(g G, state GrantState) G

	GrantShard(g G) S
	GrantAssigned(g G) time.Time
	GrantExpiration(g G) time.Time
	GrantState(g G) GrantState
	GrantID(g G) GrantID

	ShardsEqual(s1 S, s2 S) bool
}

type GrantID uuid.UUID

// GrantState describes the state of a grant. A given shard can have multiple grants with different states at the same time.
type GrantState int

const (
	UnknownGrant GrantState = iota
	// ActiveGrant indicates a shard that is assigned to a consumer and is in a steady state. No other grants for this shard exist.
	ActiveGrant
	// AllocatedGrant indicates the future location of a shard in active state. This shard has another grant
	// that is being revoked.
	AllocatedGrant
	// RevokedGrant indicates a currently active shard that is planned to be moved to a different consumer,
	// indicated by another grant with Allocated state for the same shard.
	RevokedGrant
)

// Distribution manages assignment of fixed number of shards among dynamic number of consumers.
// Consumers are connected from multiple regions, while shards may or may not have a region.
// The distribution manages assignments for shards from multiple domains.
type Distribution[S comparable, G comparable] struct {
	adapter Adapter[S, G]

	consumers map[model.InstanceID]consumerInfo

	unassigned        map[S][]G                             // unassigned shards
	grantsByShard     map[S]map[GrantID]bool                // assigned grants organized by shard
	grantsByConsumer  map[model.InstanceID]map[GrantID]bool // assigned grants organized by consumer
	grants            map[GrantID]G                         // assigned grants
	consumersByGrants map[GrantID]model.InstanceID          // consumers organized by grant ID
	consumerLoad      map[model.InstanceID]int              // number of grants assigned to a consumer
}

// NewDistribution creates distribution with given shards. The shards are unassigned.
func NewDistribution[S comparable, G comparable](a Adapter[S, G], activation time.Time, shards []S) *Distribution[S, G] {
	d := Distribution[S, G]{
		adapter:          a,
		consumers:        map[model.InstanceID]consumerInfo{},
		unassigned:       map[S][]G{},
		grantsByShard:    map[S]map[GrantID]bool{},
		grantsByConsumer: map[model.InstanceID]map[GrantID]bool{},
		grants:           map[GrantID]G{},
		consumerLoad:     map[model.InstanceID]int{},
	}
	for _, s := range shards {
		d.unassigned[s] = append(d.unassigned[s], a.NewGrant(s, UnknownGrant, activation, time.Time{}))
	}
	return &d
}

// Consumers returns all connected consumers
func (d *Distribution[S, G]) Consumers() []Consumer {
	return mapx.MapValues(d.consumers, consumerInfo.Consumer)
}

// Consumer returns true if the consumer is connected
func (d *Distribution[S, G]) Consumer(consumer model.InstanceID) bool {
	_, ok := d.consumers[consumer]
	return ok
}

// Assigned returns the list of grants assigned to the consumer
func (d *Distribution[S, G]) Assigned(consumer model.InstanceID) []G {
	var grants []G
	for id := range d.grantsByConsumer[consumer] {
		grants = append(grants, d.grants[id])
	}
	return grants
}

func (d *Distribution[S, G]) Grant(id GrantID) (G, bool) {
	g, ok := d.grants[id]
	return g, ok
}

// Unassigned returns the list of unassigned shards with activation time.
func (d *Distribution[S, G]) Unassigned() []G {
	return slicex.Flatten(mapx.Values(d.unassigned))
}

// Connect adds a new consumer to the distribution. The consumer's capacity is limited to the given number of
// shards until the specified timestamp.
// None of the shards are distributed to this consumer in this operation; it must be requested explicitly.
// Does not change the distribution of shards.
func (d *Distribution[S, G]) Connect(consumer Consumer, limit int, limitCutoff time.Time) error {
	_, ok := d.consumers[consumer.ID()]
	if ok {
		return ErrConsumerAlreadyPresent
	}
	ci := consumerInfo{
		consumer:    consumer,
		limit:       limit,
		limitCutoff: limitCutoff,
	}
	d.consumers[consumer.ID()] = ci
	d.grantsByConsumer[consumer.ID()] = map[GrantID]bool{}
	d.consumerLoad[consumer.ID()] = 0
	return nil
}

// Disconnect removes the consumer from this distribution. All the shards assigned to this consumer are marked
// as unassigned and passed in the returned values. Also returns the flag indicating whether the consumer was
// disconnected before the limit cutoff timestamp.
// Changes the distribution of shards.
func (d *Distribution[S, G]) Disconnect(now time.Time, consumer model.InstanceID) ([]G, bool, error) {
	c, ok := d.consumers[consumer]
	if !ok {
		return nil, false, ErrNotFound
	}
	delete(d.consumers, consumer)
	grantIDs := d.grantsByConsumer[consumer]
	delete(d.grantsByConsumer, consumer)
	delete(d.consumerLoad, consumer)

	var grants []G
	for id := range grantIDs {
		grant := d.grants[id]
		grants = append(grants, grant)
		delete(d.grants, id)
		shard := d.adapter.GrantShard(grant)
		d.unassigned[shard] = append(d.unassigned[shard], grant)
	}
	return grants, c.limitCutoff.After(now), nil
}

// Revoke marks the given assigned grant as revoked and adds another grant for the same shard with allocated state.
// The revoked grant expires at allocated grant activation time or its own expiration, whichever is the earliest.
// Returns a flag indicating a new grant is created.
// Changes the distribution of shards.
func (d *Distribution[S, G]) Revoke(consumer model.InstanceID, grantID GrantID, expiration time.Time) (bool, error) {
	c, ok := d.consumers[consumer]
	if !ok {
		return false, fmt.Errorf("%w: consumer %v", ErrNotFound, consumer)
	}
	grant, ok := d.grants[grantID]
	if !ok {
		return false, fmt.Errorf("%w: grant %v", ErrNotFound, grantID)
	}

	if d.adapter.GrantState(grant) == RevokedGrant {
		if d.adapter.GrantExpiration(grant).After(expiration) {
			grant = d.adapter.GrantWithExpiration(grant, expiration)
			d.grants[grantID] = grant
		}
		return false, nil
	}

	consumerGrants := d.grantsByConsumer[consumer]
	if !consumerGrants[grantID] {
		return false, fmt.Errorf("%w: grant %v not assigned to %v", ErrInvalidGrant, grantID, consumer)
	}

	shard := d.adapter.GrantShard(grant)
	shardGrants := d.grantsByShard[shard]

	switch d.adapter.GrantState(grant) {
	case AllocatedGrant:
		// Unassign allocated grant
		delete(consumerGrants, grantID)
		delete(shardGrants, grantID)
		delete(d.grants, grantID)
		d.consumerLoad[consumer] -= 1
		d.unassigned[shard] = append(d.unassigned[shard], grant)
		return false, nil

	case ActiveGrant:
		// Mark as revoked
		grant = d.adapter.GrantWithState(grant, RevokedGrant)
		if d.adapter.GrantExpiration(grant).After(expiration) {
			grant = d.adapter.GrantWithExpiration(grant, expiration)
		} else {
			expiration = d.adapter.GrantExpiration(grant)
		}
		d.grants[grantID] = grant

		// Add allocated grant to unassigned
		allocated := d.adapter.NewGrant(shard, AllocatedGrant, expiration, c.Consumer().Expiration())
		d.unassigned[shard] = append(d.unassigned[shard], allocated)

		return true, nil

	default:
		return false, fmt.Errorf("%w: unsupported state %v", ErrInvalidGrant, d.adapter.GrantState(grant))
	}
}

// Release marks the given assigned grant as unassigned with the given expiration time. No op if the grant is
// already unassigned. If the grant is already revoked and pending assigned to an allocated grant, the revoked grant
// is removed and the allocated grant is converted to assigned.
// Changes the distribution of shards.
func (d *Distribution[S, G]) Release(consumer model.InstanceID, grantID GrantID, now time.Time) (bool, error) {
	_, ok := d.consumers[consumer]
	if !ok {
		return false, fmt.Errorf("%w: consumer %v", ErrNotFound, consumer)
	}
	grant, ok := d.grants[grantID]
	if !ok {
		return false, fmt.Errorf("%w: grant %v", ErrNotFound, grantID)
	}
	consumerGrants := d.grantsByConsumer[consumer]
	if !consumerGrants[grantID] {
		return false, fmt.Errorf("%w: %v not assigned to %v", ErrInvalidGrant, grantID, consumer)
	}
	shard := d.adapter.GrantShard(grant)
	shardGrants := d.grantsByShard[shard]

	switch d.adapter.GrantState(grant) {
	case AllocatedGrant, ActiveGrant:
		// Unassign grant
		delete(consumerGrants, grantID)
		delete(shardGrants, grantID)
		delete(d.grants, grantID)
		d.consumerLoad[consumer] -= 1
		d.unassigned[shard] = append(d.unassigned[shard], grant)
		return true, nil

	case RevokedGrant:
		// Move allocated grant to active
		var allocated G
		for id := range shardGrants {
			shardGrant := d.grants[id]
			if d.adapter.GrantState(shardGrant) == AllocatedGrant && d.adapter.ShardsEqual(d.adapter.GrantShard(shardGrant), shard) {
				allocated = shardGrant
				break
			}
		}
		var empty G
		if allocated == empty {
			return false, fmt.Errorf("%w: %v is revoked, but has no allocated grant", ErrInvalidGrant, grantID)
		}
		allocated = d.adapter.GrantWithState(allocated, ActiveGrant)
		allocated = d.adapter.GrantWithAssigned(allocated, now)
		d.grants[d.adapter.GrantID(allocated)] = allocated

		// Remove revoked grant
		delete(consumerGrants, grantID)
		delete(shardGrants, grantID)
		delete(d.grants, grantID)
		d.consumerLoad[consumer] -= 1

		return true, nil

	default:
		return false, fmt.Errorf("%w: unsupported state %v", ErrInvalidGrant, d.adapter.GrantState(grant))
	}
}

// ReplaceShard replaces an unassigned shard with a new shard. Can be used to update the region of a shard.
// Returns error if shard is not found or is not in unassigned state.
// Does not change the distribution of shards.
func (d *Distribution[S, G]) ReplaceShard(old S, new S) error {
	oldGrants, ok := d.unassigned[old]
	if !ok {
		return fmt.Errorf("%w: shard %v", ErrNotFound, old)
	}
	newGrants := d.unassigned[new]

	var updatedOldGrants []G
	for _, grant := range oldGrants {
		if d.adapter.ShardsEqual(d.adapter.GrantShard(grant), old) {
			grant = d.adapter.GrantWithShard(grant, new)
			newGrants = append(newGrants, grant)
		} else {
			updatedOldGrants = append(updatedOldGrants, grant)
		}
	}
	d.unassigned[new] = newGrants
	d.unassigned[old] = updatedOldGrants
	return nil
}

// Assign assigns an unallocated shard to the given consumer, ignoring activation time.
func (d *Distribution[S, G]) Assign(consumer model.InstanceID, shard S, now time.Time) error {
	c, ok := d.consumers[consumer]
	if !ok {
		return fmt.Errorf("%w: consumer %v", ErrNotFound, consumer)
	}
	unassigned, ok := d.unassigned[shard]
	if !ok {
		return fmt.Errorf("%w: shard %v", ErrNotFound, shard)
	}

	var toAssign G
	for i, shardGrant := range unassigned {
		if d.adapter.ShardsEqual(d.adapter.GrantShard(shardGrant), shard) {
			unassigned = append(unassigned[:i], unassigned[i+1:]...)
			d.unassigned[shard] = unassigned
			toAssign = shardGrant
			break
		}
	}
	var empty G
	if toAssign == empty {
		return fmt.Errorf("%w: cannot find shard %v in unassigned shards", ErrInvalidGrant, shard)
	}

	toAssign = d.adapter.NewGrant(d.adapter.GrantShard(toAssign), d.adapter.GrantState(toAssign), now, c.Consumer().Expiration())

	id := d.adapter.GrantID(toAssign)
	d.grants[id] = toAssign
	d.grantsByShard[shard][id] = true
	d.grantsByConsumer[consumer][id] = true
	d.consumersByGrants[id] = consumer
	d.consumerLoad[consumer] += 1
	return nil
}

// Extend extends the lease of the consumer.
func (d *Distribution[S, G]) Extend(consumer model.InstanceID) ([]G, error) {
	c, ok := d.consumers[consumer]
	if !ok {
		return nil, fmt.Errorf("%w: consumer %v", ErrNotFound, consumer)
	}
	expiration := c.Consumer().Expiration()
	var grants []G
	consumerGrants := d.grantsByConsumer[consumer]
	for id := range consumerGrants {
		grant := d.grants[id]
		if d.adapter.GrantExpiration(grant).Before(expiration) {
			grant = d.adapter.GrantWithExpiration(grant, expiration)
			d.grants[id] = grant
			grants = append(grants, grant)
		}
	}
	return grants, nil
}

// Expire frees all expired shards. Returns a list of original grants.
func (d *Distribution[S, G]) Expire(now time.Time) map[model.InstanceID][]G {
	grants := map[model.InstanceID][]G{}
	for id, grant := range d.grants {
		if d.adapter.GrantExpiration(grant).Before(now) {
			consumer := d.consumersByGrants[id]
			grants[consumer] = append(grants[consumer], grant)

			delete(d.grantsByConsumer[consumer], id)
			delete(d.grantsByShard[d.adapter.GrantShard(grant)], id)
			delete(d.grants, id)
			delete(d.consumersByGrants, id)
			d.consumerLoad[consumer] -= 1

			if d.adapter.GrantState(grant) != RevokedGrant {
				shard := d.adapter.GrantShard(grant)
				d.unassigned[shard] = append(d.unassigned[shard], grant)
			}
		}
	}
	return grants
}

// AssignAll distributes unassigned shards to consumers for optimal balance. The shards are allocated
// with the Assigned state.
// Changes the distribution of shards.
func (d *Distribution[S, G]) AssignAll(now time.Time) map[model.InstanceID][]G {
	assigned := map[model.InstanceID][]G{}
	if len(d.consumers) == 0 {
		return assigned
	}
	for key, grants := range d.unassigned {
		for _, grant := range grants {
			consumer, ok := d.pickConsumer(now)
			if !ok {
				break
			}
			grant = d.adapter.NewGrant(d.adapter.GrantShard(grant), d.adapter.GrantState(grant), d.adapter.GrantAssigned(grant), d.adapter.GrantExpiration(grant))
			id := d.adapter.GrantID(grant)
			d.grantsByConsumer[consumer][id] = true
			d.grantsByShard[d.adapter.GrantShard(grant)][id] = true
			d.grants[id] = grant
			d.consumersByGrants[id] = consumer
			d.consumerLoad[consumer] += 1
			assigned[consumer] = append(assigned[consumer], grant)
		}
		delete(d.unassigned, key)
	}
	return assigned
}

func (d *Distribution[S, G]) pickConsumer(now time.Time) (model.InstanceID, bool) {
	ids := mapx.Keys(d.consumers)
	for len(ids) > 0 {
		pick := ids[rand.Intn(len(ids))]
		c := d.consumers[pick]
		if c.limitCutoff.After(now) && d.consumerLoad[pick] >= c.limit {
			continue
		}
		return pick, true
	}
	return "", false
}

// Reassign changes the distribution of shards for optimal balance with minimal movement costs. The limit
// restrict the number of allowed movements. The deadline defines expiration for revoked grants and activation
// for allocated grants.
// Returns new grants.
func (d *Distribution[S, G]) Reassign(limit int, deadline time.Time) map[model.InstanceID][]G {
	// TODO: implement
	return nil
}

type consumerInfo struct {
	consumer    Consumer
	limit       int
	limitCutoff time.Time
}

func (i consumerInfo) Consumer() Consumer {
	return i.consumer
}
