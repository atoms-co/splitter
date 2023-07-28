package core

import (
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type BlockDistribution struct {
	pb *internal_v1.BlockDistribution
}

func WrapBlockDistribution(pb *internal_v1.BlockDistribution) BlockDistribution {
	return BlockDistribution{pb: pb}
}

func UnwrapBlockDistribution(t BlockDistribution) *internal_v1.BlockDistribution {
	return t.pb
}

func ParseBlockDistribution(pb *internal_v1.BlockDistribution) (BlockDistribution, error) {
	return BlockDistribution{pb: pb}, nil
}

func NewSingleRegionBlockDistribution(initial model.Region) BlockDistribution {
	return WrapBlockDistribution(&internal_v1.BlockDistribution{
		Region: string(initial),
	})
}

func (t BlockDistribution) String() string {
	return proto.MarshalTextString(t.pb)
}

type PlacementState = internal_v1.InternalPlacement_State

const (
	PlacementActive         = internal_v1.InternalPlacement_ACTIVE
	PlacementSuspended      = internal_v1.InternalPlacement_SUSPENDED
	PlacementDecommissioned = internal_v1.InternalPlacement_DECOMMISSIONED
)

type InternalPlacementConfig struct {
	pb *internal_v1.InternalPlacement_Config
}

func WrapInternalPlacementConfig(pb *internal_v1.InternalPlacement_Config) InternalPlacementConfig {
	return InternalPlacementConfig{pb: pb}
}

func UnwrapInternalPlacementConfig(t InternalPlacementConfig) *internal_v1.InternalPlacement_Config {
	return t.pb
}

func ParseInternalPlacementConfig(pb *internal_v1.InternalPlacement_Config) (InternalPlacementConfig, error) {
	return WrapInternalPlacementConfig(pb), nil
}

func NewInternalPlacementConfig(target, current BlockDistribution) InternalPlacementConfig {
	return WrapInternalPlacementConfig(&internal_v1.InternalPlacement_Config{
		Target:  UnwrapBlockDistribution(target),
		Current: UnwrapBlockDistribution(current),
	})
}

func (t InternalPlacementConfig) String() string {
	return proto.MarshalTextString(t.pb)
}

type InternalPlacement struct {
	pb *internal_v1.InternalPlacement
}

func NewInternalPlacement(name model.QualifiedPlacementName, config InternalPlacementConfig, now time.Time) InternalPlacement {
	return WrapInternalPlacement(&internal_v1.InternalPlacement{
		Name:    name.ToProto(),
		State:   PlacementActive,
		Config:  UnwrapInternalPlacementConfig(config),
		Created: timestamppb.New(now),
	})
}

func WrapInternalPlacement(pb *internal_v1.InternalPlacement) InternalPlacement {
	return InternalPlacement{pb: pb}
}

func UnwrapInternalPlacement(t InternalPlacement) *internal_v1.InternalPlacement {
	return t.pb
}

func (t InternalPlacement) Name() model.QualifiedPlacementName {
	ret, _ := model.ParseQualifiedPlacementName(t.pb.GetName())
	return ret
}

func (t InternalPlacement) State() PlacementState {
	return t.pb.GetState()
}

func (t InternalPlacement) String() string {
	return proto.MarshalTextString(t.pb)
}

type InternalPlacementInfo struct {
	pb *internal_v1.InternalPlacementInfo
}

func WrapInternalPlacementInfo(pb *internal_v1.InternalPlacementInfo) InternalPlacementInfo {
	return InternalPlacementInfo{pb: pb}
}

func UnwrapInternalPlacementInfo(t InternalPlacementInfo) *internal_v1.InternalPlacementInfo {
	return t.pb
}

func NewInternalPlacementInfo(placement InternalPlacement, version model.Version, now time.Time) InternalPlacementInfo {
	return WrapInternalPlacementInfo(&internal_v1.InternalPlacementInfo{
		Placement: UnwrapInternalPlacement(placement),
		Version:   int64(version),
		Timestamp: timestamppb.New(now),
	})
}

func (t InternalPlacementInfo) InternalPlacement() InternalPlacement {
	return WrapInternalPlacement(t.pb.GetPlacement())
}

func (t InternalPlacementInfo) Version() model.Version {
	return model.Version(t.pb.GetVersion())
}

func (t InternalPlacementInfo) Timestamp() time.Time {
	return t.pb.GetTimestamp().AsTime()
}

func (t InternalPlacementInfo) String() string {
	return proto.MarshalTextString(t.pb)
}
