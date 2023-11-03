package allocation

import "fmt"

// ActionKind enumerates allocation actions.
type ActionKind string

const (
	Activate ActionKind = "activate" // * -> Active
	Move     ActionKind = "move"     // Active -> Revoked + Allocated
	Swap     ActionKind = "swap"     // 2x Move
)

// Action holds an allocation or load-balancing action. No grant validation is performed.
type Action[T, D comparable] struct {
	kind          ActionKind
	grant         *Grant[T, D]
	move, counter *Transition[T, D]
}

func NewActivate[T, D comparable](grant Grant[T, D]) Action[T, D] {
	return Action[T, D]{
		kind:  Activate,
		grant: &grant,
	}
}

func NewMove[T, D comparable](move Transition[T, D]) Action[T, D] {
	return Action[T, D]{
		kind: Move,
		move: &move,
	}
}

func NewSwap[T, D comparable](move, counter Transition[T, D]) Action[T, D] {
	return Action[T, D]{
		kind:    Swap,
		move:    &move,
		counter: &counter,
	}
}

func (a Action[T, D]) Kind() ActionKind {
	return a.kind
}

func (a Action[T, D]) Activate() (Grant[T, D], bool) {
	if a.kind != Activate {
		return Grant[T, D]{}, false
	}
	return *a.grant, true
}

func (a Action[T, D]) Move() (Transition[T, D], bool) {
	if a.kind != Move {
		return Transition[T, D]{}, false
	}
	return *a.move, true
}

func (a Action[T, D]) Swap() (Transition[T, D], Transition[T, D], bool) {
	if a.kind != Swap {
		return Transition[T, D]{}, Transition[T, D]{}, false
	}
	return *a.move, *a.counter, true
}

func (a Action[T, D]) String() string {
	return fmt.Sprintf("%v", a.kind)
}
