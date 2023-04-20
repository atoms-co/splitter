package session

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session/pb"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// ClientID identifies a live session client instance. It is transient and bound in-memory
type ClientID string

// ClientDefinition represents a session client. Immutable.
type ClientDefinition struct {
	pb *session_v1.ClientDefinition
}

func NewClientDefinition(loc location.Location) ClientDefinition {
	return ClientDefinition{pb: &session_v1.ClientDefinition{
		ClientId: uuid.NewString(),
		Location: location.Unwrap(loc),
	}}
}

func WrapClientDefinition(pb *session_v1.ClientDefinition) ClientDefinition {
	return ClientDefinition{pb: pb}
}

func UnwrapClientDefinition(m ClientDefinition) *session_v1.ClientDefinition {
	return m.pb
}

func (c ClientDefinition) ID() ClientID {
	return ClientID(c.pb.GetClientId())
}

func (c ClientDefinition) Location() location.Location {
	return location.Wrap(c.pb.GetLocation())
}

func (c ClientDefinition) String() string {
	return fmt.Sprintf("%v[%v]", c.ID(), c.Location())
}

// ID identifies a unique session ID.
type ID string

func NewID() ID {
	return ID(uuid.NewString())
}

// Message is a session message. Immutable.
type Message struct {
	pb *session_v1.Message
}

func NewEstablishMessage(sid ID, client ClientDefinition) Message {
	return WrapMessage(&session_v1.Message{
		Request: &session_v1.Message_Establish_{
			Establish: &session_v1.Message_Establish{
				Client: UnwrapClientDefinition(client),
				Id:     string(sid),
			},
		},
	})
}

func NewHeartbeatMessage(now time.Time) Message {
	return WrapMessage(&session_v1.Message{
		Request: &session_v1.Message_Heartbeat_{
			Heartbeat: &session_v1.Message_Heartbeat{
				Now: timestamppb.New(now),
			},
		},
	})
}

func NewCloseMessage() Message {
	return WrapMessage(&session_v1.Message{
		Request: &session_v1.Message_Close_{
			Close: &session_v1.Message_Close{},
		},
	})
}

func NewEstablishedMessage(ttl time.Time) Message {
	return WrapMessage(&session_v1.Message{
		Request: &session_v1.Message_Established_{
			Established: &session_v1.Message_Established{
				Ttl: timestamppb.New(ttl),
			},
		},
	})
}

func NewClosedMessage() Message {
	return WrapMessage(&session_v1.Message{
		Request: &session_v1.Message_Closed_{
			Closed: &session_v1.Message_Closed{},
		},
	})
}

func WrapMessage(pb *session_v1.Message) Message {
	return Message{pb: pb}
}

func UnwrapMessage(m Message) *session_v1.Message {
	return m.pb
}

func (m Message) IsEstablish() bool {
	return m.pb.GetEstablish() != nil
}

func (m Message) IsEstablished() bool {
	return m.pb.GetEstablished() != nil
}

func (m Message) IsHeartbeat() bool {
	return m.pb.GetHeartbeat() != nil
}

func (m Message) IsClose() bool {
	return m.pb.GetClose() != nil
}

func (m Message) IsClosed() bool {
	return m.pb.GetClosed() != nil
}

func (m Message) Establish() (Establish, bool) {
	if !m.IsEstablish() {
		return Establish{}, false
	}
	establish := m.pb.GetEstablish()
	return Establish{
		Definition: WrapClientDefinition(establish.GetClient()),
		ID:         ID(establish.GetId()),
	}, true
}

type Establish struct {
	Definition ClientDefinition
	ID         ID
}

func (m Message) Heartbeat() (time.Time, bool) {
	if !m.IsHeartbeat() {
		return time.Time{}, false
	}
	return m.pb.GetHeartbeat().GetNow().AsTime(), true
}

func (m Message) Established() (time.Time, bool) {
	if !m.IsEstablished() {
		return time.Time{}, false
	}
	return m.pb.GetEstablished().GetTtl().AsTime(), true
}

func (m Message) MessageType() string {
	switch {
	case m.IsEstablish():
		return "session_establish"
	case m.IsEstablished():
		return "session_established"
	case m.IsClose():
		return "session_close"
	case m.IsClosed():
		return "session_closed"
	case m.IsHeartbeat():
		return "session_heartbeat"
	default:
		return "session_unknown"
	}
}

func (m Message) String() string {
	return proto.CompactTextString(m.pb)
}
