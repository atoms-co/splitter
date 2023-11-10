package allocation

import "fmt"

// Move holds a grant movement with a Revoked from grant and an Allocated to grant.
type Move[T comparable] struct {
	From, To Grant[T]
}

func (m Move[T]) String() string {
	return fmt.Sprintf("%v[%v@%v->%v@%v]", m.From.Unit, m.From.ID, m.From.Worker, m.To.ID, m.To.Worker)
}
