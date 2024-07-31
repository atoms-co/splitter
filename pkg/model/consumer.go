package model

import (
	"go.atoms.co/slicex"
	"go.atoms.co/lib/uuidx"
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pb"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type ConsumerID = InstanceID
type Consumer = Instance

type Shard struct {
	Domain QualifiedDomainName
	Type   DomainType
	Region Region
	To     Key
	From   Key
}

// NewShards returns N shards for a region and domain, uniformly split.
func NewShards(domain QualifiedDomainName, dtype DomainType, r Region, n int) []Shard {
	list, err := uuidx.Split(uuidx.Domain, n)
	if err != nil {
		panic(err)
	}
	return slicex.Map(list, func(s uuidx.Range) Shard {
		return Shard{
			Type:   dtype,
			Domain: domain,
			Region: r,
			From:   Key(s.From()),
			To:     Key(s.To()),
		}
	})
}

func ParseShard(pb *public_v1.Shard) (Shard, error) {
	if pb.GetDomain() == nil {
		return Shard{}, fmt.Errorf("missing domain: %v", proto.MarshalTextString(pb))
	}
	domain, err := ParseQualifiedDomainName(pb.GetDomain())
	if err != nil {
		return Shard{}, fmt.Errorf("invalid domain: %w", err)
	}
	switch pb.GetType() {
	case Unit:
		return Shard{
			Domain: domain,
			Type:   pb.GetType(),
		}, nil
	case Global:
		to, err := ParseKey(pb.GetTo())
		if err != nil {
			return Shard{}, fmt.Errorf("invalid to: %v", proto.MarshalTextString(pb))
		}
		from, err := ParseKey(pb.GetFrom())
		if err != nil {
			return Shard{}, fmt.Errorf("invalid from: %v", proto.MarshalTextString(pb))
		}
		return Shard{
			Domain: domain,
			Type:   pb.GetType(),
			To:     to,
			From:   from,
		}, nil
	case Regional:
		to, err := ParseKey(pb.GetTo())
		if err != nil {
			return Shard{}, fmt.Errorf("invalid to: %v", proto.MarshalTextString(pb))
		}
		from, err := ParseKey(pb.GetFrom())
		if err != nil {
			return Shard{}, fmt.Errorf("invalid from: %v", proto.MarshalTextString(pb))
		}
		return Shard{
			Domain: domain,
			Type:   pb.GetType(),
			Region: Region(pb.GetRegion()),
			To:     to,
			From:   from,
		}, nil
	default:
		return Shard{}, fmt.Errorf("invalid shard domain type: %v", pb.GetType())
	}
}

func (s Shard) Contains(key QualifiedDomainKey) bool {
	switch s.Type {
	case Unit:
		return s.Domain == key.Domain
	case Global:
		if s.Domain != key.Domain {
			return false
		}
		r, _ := uuidx.NewRange(uuid.UUID(s.From), uuid.UUID(s.To))
		return r.Contains(uuid.UUID(key.Key.Key))
	case Regional:
		if s.Domain != key.Domain || s.Region != key.Key.Region {
			return false
		}
		r, _ := uuidx.NewRange(uuid.UUID(s.From), uuid.UUID(s.To))
		return r.Contains(uuid.UUID(key.Key.Key))
	default:
		return false
	}
}

// IntersectsRange evaluates if two shards intersect over a key range while ignoring domain.
func (s Shard) IntersectsRange(t Shard) bool {
	switch {
	case s.Type == Unit || t.Type == Unit:
		return true

	case s.Type == Regional && t.Type == Regional:
		return s.Region == t.Region && s.hasRangeOverlap(t)

	default:
		return s.hasRangeOverlap(t)
	}
}

func (s Shard) hasRangeOverlap(t Shard) bool {
	r1, _ := uuidx.NewRange(uuid.UUID(s.From), uuid.UUID(s.To))
	r2, _ := uuidx.NewRange(uuid.UUID(t.From), uuid.UUID(t.To))
	_, res := r1.Intersects(r2)
	return res
}

func (s Shard) ToProto() *public_v1.Shard {
	return &public_v1.Shard{
		Region: string(s.Region),
		Type:   s.Type,
		Domain: s.Domain.ToProto(),
		To:     s.To.String(),
		From:   s.From.String(),
	}
}

func (s Shard) String() string {
	switch s.Type {
	case Unit:
		return fmt.Sprintf("%v", s.Domain)
	case Global:
		return fmt.Sprintf("%v[%v-%v)", s.Domain, s.From.String()[:4], s.To.String()[:4])
	case Regional:
		return fmt.Sprintf("%v@%v[%v-%v)", s.Domain, s.Region, s.From.String()[:4], s.To.String()[:4])
	default:
		return "invalid-shard"
	}
}

type GrantState = public_v1.GrantState

var (
	InvalidGrantState   = public_v1.GrantState_UNKNOWN
	ActiveGrantState    = public_v1.GrantState_ACTIVE
	AllocatedGrantState = public_v1.GrantState_ALLOCATED
	RevokedGrantState   = public_v1.GrantState_REVOKED
	LoadedGrantState    = public_v1.GrantState_ALLOCATED_LOADED
	UnloadedGrantState  = public_v1.GrantState_REVOKED_UNLOADED
)

func IsActiveGrant(state GrantState) bool {
	return state == ActiveGrantState
}

func IsAllocatedGrant(state GrantState) bool {
	return state == AllocatedGrantState
}

func IsRevokedGrant(state GrantState) bool {
	return state == RevokedGrantState
}

func IsLoadedGrant(state GrantState) bool {
	return state == LoadedGrantState
}

func IsUnloadedGrant(state GrantState) bool {
	return state == UnloadedGrantState
}

func IsAllocatedOrLoaded(state GrantState) bool {
	return state == AllocatedGrantState || state == LoadedGrantState
}

func IsRevokedOrUnloaded(state GrantState) bool {
	return state == RevokedGrantState || state == UnloadedGrantState
}

// GrantStateCanAdvanceTo returns true if the grant can advance to the given state.
func GrantStateCanAdvanceTo(state GrantState, next GrantState) bool {
	switch state {
	case AllocatedGrantState:
		return next != AllocatedGrantState
	case LoadedGrantState:
		return !IsAllocatedOrLoaded(next)
	case ActiveGrantState:
		return IsRevokedOrUnloaded(next)
	case RevokedGrantState:
		return IsUnloadedGrant(next)
	case UnloadedGrantState:
		return false
	default:
		return false
	}
}

// GrantID is a coordinator-determined grant id.
type GrantID = allocation.GrantID

type Grant struct {
	pb *public_v1.Grant
}

func NewGrant(id GrantID, shard Shard, state GrantState, lease, assigned time.Time) Grant {
	return WrapGrant(&public_v1.Grant{
		Id:       string(id),
		Shard:    shard.ToProto(),
		State:    state,
		Lease:    timestamppb.New(lease),
		Assigned: timestamppb.New(assigned),
	})
}

func WrapGrant(pb *public_v1.Grant) Grant {
	return Grant{pb: pb}
}

func UnwrapGrant(g Grant) *public_v1.Grant {
	return g.pb
}

func (g Grant) ID() GrantID {
	return GrantID(g.pb.GetId())
}

func (g Grant) Shard() Shard {
	ret, _ := ParseShard(g.pb.GetShard())
	return ret
}

func (g Grant) State() GrantState {
	return g.pb.GetState()
}

func (g Grant) Lease() time.Time {
	return g.pb.GetLease().AsTime()
}

func (g Grant) Assigned() time.Time {
	return g.pb.GetAssigned().AsTime()
}

func (g Grant) WithState(state GrantState) Grant {
	return NewGrant(g.ID(), g.Shard(), state, g.Lease(), g.Assigned())
}

func (g Grant) String() string {
	return fmt.Sprintf("%v[shard=%v, state=%v, lease=%v, assigned=%v]", g.ID(), g.Shard(), g.State(), g.Lease(), g.Assigned())
}

type GrantInfo struct {
	pb *public_v1.ClusterMessage_GrantInfo
}

func NewGrantInfo(id GrantID, shard Shard, state GrantState) GrantInfo {
	return GrantInfo{pb: &public_v1.ClusterMessage_GrantInfo{
		Id:    string(id),
		Shard: shard.ToProto(),
		State: state,
	}}
}

func WrapGrantInfo(pb *public_v1.ClusterMessage_GrantInfo) GrantInfo {
	return GrantInfo{pb: pb}
}

func UnwrapGrantInfo(g GrantInfo) *public_v1.ClusterMessage_GrantInfo {
	return g.pb
}

func (g GrantInfo) ID() GrantID {
	return GrantID(g.pb.GetId())
}

func (g GrantInfo) Shard() Shard {
	ret, _ := ParseShard(g.pb.GetShard())
	return ret
}

func (g GrantInfo) State() GrantState {
	return g.pb.GetState()
}

func (g GrantInfo) Equals(o GrantInfo) bool {
	return proto.Equal(g.pb, o.pb)
}

func (g GrantInfo) String() string {
	return fmt.Sprintf("%v[shard=%v, state=%v]", g.ID(), g.Shard(), g.State())
}
