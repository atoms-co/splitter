package session

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.atoms.co/splitter/lib/service/location"
	sessionpb "go.atoms.co/splitter/lib/service/session/pb"
)

// ClientID identifies a live session client instance. It is transient and bound in-memory
type ClientID = location.InstanceID

// ID identifies a unique session ID.
type ID string

func NewID() ID {
	return ID(uuid.NewString())
}

// Message is a session message. Immutable.
type Message struct {
	pb *sessionpb.Message
}

func NewEstablishMessage(sid ID, client location.Instance) Message {
	return WrapMessage(&sessionpb.Message{
		Request: &sessionpb.Message_Establish_{
			Establish: &sessionpb.Message_Establish{
				Client: location.UnwrapInstance(client),
				Id:     string(sid),
			},
		},
	})
}

func NewEstablishedMessage(ttl time.Time, server location.Instance) Message {
	return WrapMessage(&sessionpb.Message{
		Request: &sessionpb.Message_Established_{
			Established: &sessionpb.Message_Established{
				Ttl:    timestamppb.New(ttl),
				Server: location.UnwrapInstance(server),
			},
		},
	})
}

func NewHeartbeatMessage(now time.Time) Message {
	return WrapMessage(&sessionpb.Message{
		Request: &sessionpb.Message_Heartbeat_{
			Heartbeat: &sessionpb.Message_Heartbeat{
				Now: timestamppb.New(now),
			},
		},
	})
}

func NewHeartbeakAckMessage(ttl time.Time) Message {
	return WrapMessage(&sessionpb.Message{
		Request: &sessionpb.Message_Ack{
			Ack: &sessionpb.Message_HeartbeatAck{
				Ttl: timestamppb.New(ttl),
			},
		},
	})
}

func NewClosedMessage() Message {
	return WrapMessage(&sessionpb.Message{
		Request: &sessionpb.Message_Closed_{
			Closed: &sessionpb.Message_Closed{},
		},
	})
}

func WrapMessage(pb *sessionpb.Message) Message {
	return Message{pb: pb}
}

func UnwrapMessage(m Message) *sessionpb.Message {
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

func (m Message) IsHeartbeatAck() bool {
	return m.pb.GetAck() != nil
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
		Client: location.WrapInstance(establish.GetClient()),
		ID:     ID(establish.GetId()),
	}, true
}

type Establish struct {
	Client location.Instance
	ID     ID
}

func (m Message) Heartbeat() (time.Time, bool) {
	if !m.IsHeartbeat() {
		return time.Time{}, false
	}
	return m.pb.GetHeartbeat().GetNow().AsTime(), true
}

func (m Message) HeartbeatAck() (time.Time, bool) {
	if !m.IsHeartbeatAck() {
		return time.Time{}, false
	}
	return m.pb.GetAck().GetTtl().AsTime(), true
}

func (m Message) Established() (Established, bool) {
	if !m.IsEstablished() {
		return Established{}, false
	}
	established := m.pb.GetEstablished()
	return Established{
		Ttl:    established.GetTtl().AsTime(),
		Server: location.WrapInstance(established.GetServer()),
	}, true
}

type Established struct {
	Ttl    time.Time
	Server location.Instance
}

func (m Message) MessageType() string {
	switch {
	case m.IsEstablish():
		return "session_establish"
	case m.IsEstablished():
		return "session_established"
	case m.IsClosed():
		return "session_closed"
	case m.IsHeartbeat():
		return "session_heartbeat"
	case m.IsHeartbeatAck():
		return "session_heartbeat_ack"
	default:
		return "session_unknown"
	}
}

func (m Message) String() string {
	return proto.CompactTextString(m.pb)
}
