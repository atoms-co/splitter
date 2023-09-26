package session

import (
	"go.atoms.co/lib/chanx"
)

// Connect creates a client-side session envelope for a message stream of type T, with Establish and Closed
// delimiting the joined stream. The type T must be able to embed session messages.
func Connect[T any](establish Message, main <-chan T, liveness <-chan Message, inject func(Message) T) <-chan T {
	return chanx.Envelope(
		inject(establish),
		chanx.Join(main, chanx.Map(liveness, inject)),
		inject(NewClosedMessage()),
	)
}

// Receive creates a server-side session output for a message stream of type T, terminated with Closed.
func Receive[T any](main <-chan T, liveness <-chan Message, inject func(Message) T) <-chan T {
	return chanx.AppendLast(
		chanx.Join(main, chanx.Map(liveness, inject)),
		inject(NewClosedMessage()),
	)
}
