package allocation

// TODO(herohde) 10/23/2023: do we need more holistic (but expensive) penalties? Such as
// contextual co-location or whole-world penalties. The problem with the latter is that
// we have no hint of how to find a good move/swap and have to search all possibilities.
// For now, we natively support global and domain-specific load-balancing.

// Rule is a unique allocation rule name. Informational only.
type Rule string

const (
	// LoadBalanceRule is the built-in load balancing rule for even adjusted load on workers.
	LoadBalanceRule Rule = "load-balance"
)

// Placement is a rule that determines the additional load from assigning the given work to the worker.
// Returns false if such an assignment is not valid. An optimal assignment has zero extra load, excl.
// co-location factors. Used for worker constraints and affinity. Must be deterministic.
type Placement[T comparable, W any, K comparable, V any] interface {
	ID() Rule
	TryPlace(worker Worker[K, V], work Work[T, W]) (Load, bool)
}

// Placements is a list of placement rules. Evaluation convenience.
type Placements[T comparable, W any, K comparable, V any] struct {
	List []Placement[T, W, K, V]
}

func (p Placements[T, W, K, V]) TryPlace(worker Worker[K, V], work Work[T, W]) (Load, bool) {
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
type PredicateFn[T comparable, W any, K comparable, V any] func(Worker[K, V], Work[T, W]) bool

// Fixed returns a predicate that always returns the given value.
func Fixed[T comparable, W any, K comparable, V any](v bool) PredicateFn[T, W, K, V] {
	return func(w Worker[K, V], w2 Work[T, W]) bool {
		return v
	}
}

type constraint[T comparable, W any, K comparable, V any] struct {
	name Rule
	fn   PredicateFn[T, W, K, V]
}

// NewConstraint returns a hard placement constraint based on a predicate. Predicate must be deterministic.
func NewConstraint[T comparable, W any, K comparable, V any](name Rule, fn PredicateFn[T, W, K, V]) Placement[T, W, K, V] {
	return &constraint[T, W, K, V]{name: name, fn: fn}
}

func (c *constraint[T, W, K, V]) ID() Rule {
	return c.name
}

func (c *constraint[T, W, K, V]) TryPlace(worker Worker[K, V], work Work[T, W]) (Load, bool) {
	return 0, c.fn(worker, work)
}

func (c *constraint[T, W, K, V]) String() string {
	return string(c.name)
}

type preference[T comparable, W any, K comparable, V any] struct {
	name    Rule
	penalty Load
	fn      PredicateFn[T, W, K, V]
}

// NewPreference returns placement preference (or soft constraint) based on a predicate. Penalty is
// given if the predicate does not hold. Predicate must be deterministic.
func NewPreference[T comparable, W any, K comparable, V any](name Rule, penalty Load, fn PredicateFn[T, W, K, V]) Placement[T, W, K, V] {
	return &preference[T, W, K, V]{name: name, penalty: penalty, fn: fn}
}

func (p *preference[T, W, K, V]) ID() Rule {
	return p.name
}

func (p *preference[T, W, K, V]) TryPlace(worker Worker[K, V], work Work[T, W]) (Load, bool) {
	if p.fn(worker, work) {
		return 0, true
	}
	return p.penalty, true
}

func (p *preference[T, W, K, V]) String() string {
	return string(p.name)
}

// Colocation is a rule that determines the additional load from colocation of the given work on a worker.
// Used for worker overload as well as affinity and anti-affinity across work, usually dependent on T.
// Must be deterministic.
type Colocation[T comparable, W any, K comparable, V any] interface {
	ID() Rule
	Colocate(worker Worker[K, V], work map[T]Work[T, W]) map[T]Load
}

// Colocations is a list of Colocation rules. Evaluation convenience.
type Colocations[T comparable, W any, K comparable, V any] struct {
	List []Colocation[T, W, K, V]
}

func (c Colocations[T, W, K, V]) Colocate(worker Worker[K, V], work map[T]Work[T, W]) (Load, map[T]Load) {
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

func (c Colocations[T, W, K, V]) IsEmpty() bool {
	return len(c.List) == 0
}
