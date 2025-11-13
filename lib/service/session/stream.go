package session

import (
	"fmt"
	"time"

	"atoms.co/lib-go/pkg/clock"
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

// ReadEstablish reads the establish session message from the input stream. This function can be used
// to initialize a session on the server side.
func ReadEstablish[T any](in <-chan T, extract func(T) (Message, bool)) (Establish, error) {
	return ReadEstablishWithTimeout(in, extract, defaultEstablishTimeout)
}

// ReadEstablishWithTimeout reads the establish session message from the input stream. This function can be used
// to initialize a session on the server side.
func ReadEstablishWithTimeout[T any](in <-chan T, extract func(T) (Message, bool), establishTimeout time.Duration) (Establish, error) {
	// Read first message
	first, ok := chanx.TryRead(in, clock.New(), establishTimeout)
	if !ok {
		return Establish{}, fmt.Errorf("no first session message")
	}
	sessionMsg, ok := extract(first)
	if !ok {
		return Establish{}, fmt.Errorf("expected session message, got %v", first)
	}
	if !sessionMsg.IsEstablish() {
		return Establish{}, fmt.Errorf("expected establish session message, got %v", first)
	}
	establish, _ := sessionMsg.Establish()
	return establish, nil
}
