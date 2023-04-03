package session

import (
	"atoms.co/lib-go/pkg/dist/region"
	"go.atoms.co/splitter/lib/service/session/pb"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
)

// Location is a location for component observability. System components move around. Can be used for debugging.
type Location struct {
	pb *session_v1.Location
}

func NewLocation(region region.Region, node string) Location {
	return Location{pb: &session_v1.Location{
		Region: string(region),
		Node:   node,
	}}
}

func WrapLocation(pb *session_v1.Location) Location {
	return Location{pb: pb}
}

func UnwrapLocation(l Location) *session_v1.Location {
	return l.pb
}

func (l Location) Region() region.Region {
	return region.Region(l.pb.GetRegion())
}

func (l Location) Node() string {
	return l.pb.GetNode()
}

func (l Location) String() string {
	return fmt.Sprintf("%v/%v", l.Region(), l.Node())
}

// ClientID identifies a live session client instance. It is transient and bound in-memory
type ClientID string

// Client represents a session client. Immutable.
type Client struct {
	pb *session_v1.Client
}

func NewClient(location Location) Client {
	return Client{pb: &session_v1.Client{
		ClientId: uuid.NewString(),
		Location: UnwrapLocation(location),
	}}
}

func WrapClient(pb *session_v1.Client) Client {
	return Client{pb: pb}
}

func UnwrapClient(m Client) *session_v1.Client {
	return m.pb
}

func (c Client) ID() ClientID {
	return ClientID(c.pb.GetClientId())
}

func (c Client) Location() Location {
	return WrapLocation(c.pb.GetLocation())
}

func (c Client) String() string {
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

func NewEstablishSessionMessage(sid ID, client Client) Message {
	return WrapMessage(&session_v1.Message{
		Request: &session_v1.Message_Establish_{
			Establish: &session_v1.Message_Establish{
				Client: UnwrapClient(client),
				Id:     string(sid),
			},
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
		Client: WrapClient(establish.GetClient()),
		ID:     ID(establish.GetId()),
	}, true
}

type Establish struct {
	Client Client
	ID     ID
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
