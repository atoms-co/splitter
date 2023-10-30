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
// co-location factors. Used for worker constraints and affinity. Must be semi-deterministic.
type Placement[T, D comparable] interface {
	ID() Rule
	TryPlace(worker Worker, work Work[T, D]) (Load, bool)
}

// Placements is a list of placement rules. Evaluation convenience.
type Placements[T, D comparable] struct {
	List []Placement[T, D]
}

func (p Placements[T, D]) TryPlace(worker Worker, work Work[T, D]) (Load, bool) {
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
type PredicateFn[T, D comparable] func(Worker, Work[T, D]) bool

func HasNodeAffinity[T, D comparable](worker Worker, work Work[T, D]) bool {
	return work.Location.Node != "" && worker.Location == work.Location
}

func HasRegionAffinity[T, D comparable](worker Worker, work Work[T, D]) bool {
	return work.Location.Region != "" && worker.Location.Region == work.Location.Region
}

type constraint[T, D comparable] struct {
	name Rule
	fn   PredicateFn[T, D]
}

// NewConstraint returns a hard placement constraint based on a predicate. Predicate must be semi-deterministic.
func NewConstraint[T, D comparable](name Rule, fn PredicateFn[T, D]) Placement[T, D] {
	return &constraint[T, D]{name: name, fn: fn}
}

func (c *constraint[T, D]) ID() Rule {
	return c.name
}

func (c *constraint[T, D]) TryPlace(worker Worker, work Work[T, D]) (Load, bool) {
	return 0, c.fn(worker, work)
}

func (c *constraint[T, D]) String() string {
	return string(c.name)
}

type preference[T, D comparable] struct {
	name    Rule
	penalty Load
	fn      PredicateFn[T, D]
}

// NewPreference returns placement preference (or soft constraint) based on a predicate. Penalty is
// given if the predicate does not hold. Predicate must be semi-deterministic.
func NewPreference[T, D comparable](name Rule, penalty Load, fn PredicateFn[T, D]) Placement[T, D] {
	return &preference[T, D]{name: name, penalty: penalty, fn: fn}
}

func (p *preference[T, D]) ID() Rule {
	return p.name
}

func (p *preference[T, D]) TryPlace(worker Worker, work Work[T, D]) (Load, bool) {
	if p.fn(worker, work) {
		return 0, true
	}
	return p.penalty, true
}

func (p *preference[T, D]) String() string {
	return string(p.name)
}

// Colocation is a rule that determines the addition load from colocation of the given work on a worker.
// Used for worker overload as well as affinity and anti-affinity across work, usually dependent on T.
// Must be semi-deterministic.
type Colocation[T, D comparable] interface {
	ID() Rule
	CoLocate(worker Worker, work map[T]Work[T, D]) map[T]Load
}
