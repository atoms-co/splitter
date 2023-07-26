package model

import (
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pb"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sort"
	"strings"
	"time"
)

// DistributionSplit represents a region change in a distribution from a given key.
type DistributionSplit struct {
	Key    Key
	Region Region
}

// Distribution is a coarse assignment of keys to region. It defines a segmentation of the full key range
// into segments, represented as sorted split points to minimize the work needed to ensure a valid,
// complete key distribution w/o gaps or overlaps. Immutable.
type Distribution struct {
	pb *public_v1.Distribution
}

func NewDistribution(initial Region, splits ...DistributionSplit) Distribution {
	sort.Slice(splits, func(i, j int) bool {
		return splits[i].Key.Less(splits[j].Key)
	})

	return Distribution{&public_v1.Distribution{
		Region: string(initial),
		Splits: slicex.Map(splits, func(s DistributionSplit) *public_v1.Distribution_Split {
			return &public_v1.Distribution_Split{
				Key:    s.Key.String(),
				Region: string(s.Region),
			}
		}),
	}}
}

func (d Distribution) Initial() Region {
	return Region(d.pb.GetRegion())
}

func (d Distribution) Splits() []DistributionSplit {
	return slicex.Map(d.pb.GetSplits(), func(pb *public_v1.Distribution_Split) DistributionSplit {
		return DistributionSplit{
			Key:    Key(uuid.MustParse(pb.GetKey())),
			Region: Region(pb.GetRegion()),
		}
	})
}

func (d Distribution) String() string {
	return proto.MarshalTextString(d.pb)
}

type PlacementName string

type QualifiedPlacementName struct {
	Tenant    TenantName
	Placement PlacementName
}

func ParseQualifiedPlacementNameStr(name string) (QualifiedPlacementName, bool) {
	parts := slicex.Map(strings.Split(name, "/"), strings.TrimSpace)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return QualifiedPlacementName{}, false
	}
	return QualifiedPlacementName{
		Tenant:    TenantName(parts[0]),
		Placement: PlacementName(parts[1]),
	}, true
}

func ParseQualifiedPlacementName(pb *public_v1.QualifiedPlacementName) (QualifiedPlacementName, error) {
	if pb.GetTenant() == "" || pb.GetName() == "" {
		return QualifiedPlacementName{}, fmt.Errorf("invalid placement name: %v", proto.MarshalTextString(pb))
	}
	return QualifiedPlacementName{
		Tenant:    TenantName(pb.GetTenant()),
		Placement: PlacementName(pb.GetName()),
	}, nil
}

func (n QualifiedPlacementName) ToProto() *public_v1.QualifiedPlacementName {
	return &public_v1.QualifiedPlacementName{
		Tenant: string(n.Tenant),
		Name:   string(n.Placement),
	}
}

func (n QualifiedPlacementName) String() string {
	return fmt.Sprintf("%v/%v", n.Tenant, n.Placement)
}

// Placement is a named distribution, such as "facility_id". Placement distributions may change over time.
// They are used by domains for dynamic region assignments.
type Placement struct {
	pb *public_v1.Placement
}

func NewPlacement(name QualifiedPlacementName, distribution Distribution) Placement {
	return Placement{pb: &public_v1.Placement{
		Name:    nil,
		Current: nil,
	}}
}

func WrapPlacement(pb *public_v1.Placement) Placement {
	return Placement{pb: pb}
}

func UnwrapPlacement(t Placement) *public_v1.Placement {
	return t.pb
}

func (t Placement) Name() QualifiedPlacementName {
	name, _ := ParseQualifiedPlacementName(t.pb.GetName())
	return name
}

func (t Placement) Current() Distribution {
	return Distribution{pb: t.pb.GetCurrent()}
}

func (t Placement) String() string {
	return fmt.Sprintf("%v(%v)", t.Name(), t.Current())
}

type PlacementInfo struct {
	pb *public_v1.PlacementInfo
}

func WrapPlacementInfo(pb *public_v1.PlacementInfo) PlacementInfo {
	return PlacementInfo{pb: pb}
}

func UnwrapPlacementInfo(t PlacementInfo) *public_v1.PlacementInfo {
	return t.pb
}

func NewPlacementInfo(Placement Placement, version Version, now time.Time) PlacementInfo {
	return WrapPlacementInfo(&public_v1.PlacementInfo{
		Placement: UnwrapPlacement(Placement),
		Version:   int64(version),
		Timestamp: timestamppb.New(now),
	})
}

func (t PlacementInfo) Placement() Placement {
	return WrapPlacement(t.pb.GetPlacement())
}

func (t PlacementInfo) Version() Version {
	return Version(t.pb.GetVersion())
}

func (t PlacementInfo) Timestamp() time.Time {
	return t.pb.GetTimestamp().AsTime()
}

func (t PlacementInfo) String() string {
	return proto.MarshalTextString(t.pb)
}
