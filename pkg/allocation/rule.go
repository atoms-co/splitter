package allocation

// TODO(herohde) 10/23/2023: do we need more holistic (but expensive) penalties? Such as
// contextual co-location or whole-world penalties. The problem with the latter is that
// we have no hint of how to find a good move/swap and have to search all possibilities.
// For now, we natively support global and domain-specific load-balancing.

// Rule is a unique allocation rule name. Informational only.
type Rule string

const (
	// NodeAffinityRule is the rule for placing work on a worker on the same node/pod.
	NodeAffinityRule Rule = "node-affinity"
	// RegionAffinityRule is the rule for placing work on a worker in the same region.
	RegionAffinityRule Rule = "region-affinity"
	// LoadBalanceRule is the built-in load balancing rule for even adjusted load on workers.
	LoadBalanceRule Rule = "load-balance"
)

// Placement is a rule that determines the additional load from assigning the given work to the worker.
// Returns false if such an assignment is not valid. An optimal assignment has zero extra load, excl.
// co-location factors. Used for worker constraints and affinity. Must be deterministic.
type Placement[T comparable] interface {
	ID() Rule
	TryPlace(worker Worker, work Work[T]) (Load, bool)
}

// Placements is a list of placement rules. Evaluation convenience.
type Placements[T comparable] struct {
	List []Placement[T]
}

func (p Placements[T]) TryPlace(worker Worker, work Work[T]) (Load, bool) {
	var ret Load
	for _, rule := range p.List {
		load, ok := rule.TryPlace(worker, work)
		if !ok {
			return 0, false
		}
		ret += load
	}
	return ret, true
}

// PredicateFn is a (worker, work) predicate.
type PredicateFn[T comparable] func(Worker, Work[T]) bool

func HasNodeAffinity[T comparable](worker Worker, work Work[T]) bool {
	return work.Location.Node != "" && worker.Location == work.Location
}

func HasRegionAffinity[T comparable](worker Worker, work Work[T]) bool {
	return work.Location.Region != "" && worker.Location.Region == work.Location.Region
}

type constraint[T comparable] struct {
	name Rule
	fn   PredicateFn[T]
}

// NewConstraint returns a hard placement constraint based on a predicate. Predicate must be deterministic.
func NewConstraint[T comparable](name Rule, fn PredicateFn[T]) Placement[T] {
	return &constraint[T]{name: name, fn: fn}
}

func (c *constraint[T]) ID() Rule {
	return c.name
}

func (c *constraint[T]) TryPlace(worker Worker, work Work[T]) (Load, bool) {
	return 0, c.fn(worker, work)
}

func (c *constraint[T]) String() string {
	return string(c.name)
}

type preference[T comparable] struct {
	name    Rule
	penalty Load
	fn      PredicateFn[T]
}

// NewPreference returns placement preference (or soft constraint) based on a predicate. Penalty is
// given if the predicate does not hold. Predicate must be deterministic.
func NewPreference[T comparable](name Rule, penalty Load, fn PredicateFn[T]) Placement[T] {
	return &preference[T]{name: name, penalty: penalty, fn: fn}
}

func (p *preference[T]) ID() Rule {
	return p.name
}

func (p *preference[T]) TryPlace(worker Worker, work Work[T]) (Load, bool) {
	if p.fn(worker, work) {
		return 0, true
	}
	return p.penalty, true
}

func (p *preference[T]) String() string {
	return string(p.name)
}

// Colocation is a rule that determines the addition load from colocation of the given work on a worker.
// Used for worker overload as well as affinity and anti-affinity across work, usually dependent on T.
// Must be deterministic.
type Colocation[T comparable] interface {
	ID() Rule
	Colocate(worker Worker, work map[T]Work[T]) map[T]Load
}

// Colocations is a list of Colocation rules. Evaluation convenience.
type Colocations[T comparable] struct {
	List []Colocation[T]
}

func (c Colocations[T]) Colocate(worker Worker, work map[T]Work[T]) (Load, map[T]Load) {
	if len(work) == 0 {
		return 0, nil
	}

	var sum Load
	ret := map[T]Load{}
	for _, rule := range c.List {
		for k, v := range rule.Colocate(worker, work) {
			sum += v
			ret[k] += v
		}
	}
	return sum, ret
}
