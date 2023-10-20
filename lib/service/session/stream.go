package session

import (
	"go.atoms.co/lib/chanx"
)

// Connect creates a client-side session envelope for a message stream of type T, with Establish and Closed
// delimiting the joined stream. The type T must be able to embed session messages. Terminates the session
// if the main chan is closed.
func Connect[T any](c *Client, establish Message, main <-chan T, liveness <-chan Message, inject func(Message) T) <-chan T {
	return chanx.Envelope(
		inject(establish),
		chanx.Join(chanx.Breaker(main, c, clientBufChanSize), chanx.Map(liveness, inject)),
		inject(NewClosedMessage()),
	)
}

// Receive creates a server-side session output for a message stream of type T, terminated with Closed.
// Terminates the session if the main chan is closed.
func Receive[T any](s *Server, main <-chan T, liveness <-chan Message, inject func(Message) T) <-chan T {
	return chanx.Append(
		chanx.Join(chanx.Breaker(main, s, serverBufChanSize), chanx.Map(liveness, inject)),
		inject(NewClosedMessage()),
	)
}
