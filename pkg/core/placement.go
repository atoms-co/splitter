package core

import (
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.atoms.co/lib/encoding/protox"
	"go.atoms.co/lib/container"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/uuidx"
	"go.atoms.co/splitter/pkg/model"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

// Block is a block number in the range [0;1023].
type Block int

// BlockDistributionSplit represents a region change in a distribution from a given block.
type BlockDistributionSplit struct {
	Block  Block
	Region model.Region
}

func ParseBlockDistributionSplitStr(str string) (BlockDistributionSplit, error) {
	parts := strings.Split(str, ":")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return BlockDistributionSplit{}, model.ErrInvalid
	}
	num, err := strconv.Atoi(parts[0])
	if err != nil {
		return BlockDistributionSplit{}, fmt.Errorf("invalid block: %v", err)
	}
	if num < 0 || num > 1023 {
		return BlockDistributionSplit{}, fmt.Errorf("split block out of range [0:1023]: %v", num)
	}
	return BlockDistributionSplit{
		Block:  Block(num),
		Region: model.Region(parts[1]),
	}, nil
}

func (s BlockDistributionSplit) Less(o BlockDistributionSplit) bool {
	return s.Block < o.Block
}

func (s BlockDistributionSplit) Greater(o BlockDistributionSplit) bool {
	return s.Block > o.Block
}

func (s BlockDistributionSplit) ToProto() *splitterprivatepb.BlockDistribution_Split {
	return &splitterprivatepb.BlockDistribution_Split{
		Block:  int64(s.Block),
		Region: string(s.Region),
	}
}

func (s BlockDistributionSplit) String() string {
	return fmt.Sprintf("%v:%v", s.Block, s.Region)
}

type BlockDistribution struct {
	pb *splitterprivatepb.BlockDistribution
}

func WrapBlockDistribution(pb *splitterprivatepb.BlockDistribution) BlockDistribution {
	return BlockDistribution{pb: pb}
}

func UnwrapBlockDistribution(t BlockDistribution) *splitterprivatepb.BlockDistribution {
	return t.pb
}

func ParseBlockDistribution(pb *splitterprivatepb.BlockDistribution) (BlockDistribution, error) {
	return BlockDistribution{pb: pb}, nil
}

func ParseBlockDistributionStr(str string) (BlockDistribution, error) {
	// format: region[/block:region]*

	parts := strings.Split(str, "/")
	if len(parts) == 0 || parts[0] == "" {
		return BlockDistribution{}, model.ErrInvalid
	}

	initial := model.Region(parts[0])
	var splits []BlockDistributionSplit

	var last Block
	for _, s := range parts[1:] {
		split, err := ParseBlockDistributionSplitStr(s)
		if err != nil {
			return BlockDistribution{}, fmt.Errorf("invalid split '%v': %v", s, err)
		}
		if split.Block <= last {
			return BlockDistribution{}, fmt.Errorf("conflicting split: %v, last=%v", split, last)
		}
		last = split.Block
		splits = append(splits, split)
	}

	return NewBlockDistribution(initial, splits...), nil
}

func NewBlockDistribution(initial model.Region, splits ...BlockDistributionSplit) BlockDistribution {
	return WrapBlockDistribution(&splitterprivatepb.BlockDistribution{
		Region: string(initial),
		Splits: slicex.Map(splits, BlockDistributionSplit.ToProto),
	})
}

// MoveBlockDistribution returns a distribution up to N blocks from current towards the target.
func MoveBlockDistribution(current, target BlockDistribution, n int) BlockDistribution {
	if n < 1 || current.Equals(target) {
		return current
	}

	ret := container.NewHeap(BlockDistributionSplit.Less)

	// (1) Process splits in reverse order so that we know the full ranges easily.

	have := newSplitHeap(current, BlockDistributionSplit.Greater)
	want := newSplitHeap(target, BlockDistributionSplit.Greater)

	cur := have.Pop()
	goal := want.Pop()
	end := Block(1024) // previous split point

	for end > 0 && n > 0 {
		if cur.Region == goal.Region {
			// Match. Emit the common tail segment.

			end = max(cur.Block, goal.Block)
		} else {
			// No match. Emit updated segment of size up to N.

			next := max(cur.Block, goal.Block, end-Block(n))
			n -= int(end - next)
			end = next
		}

		ret.Push(BlockDistributionSplit{Block: end, Region: goal.Region})
		if n == 0 {
			break
		}

		if end <= goal.Block && want.Len() > 0 {
			goal = want.Pop()
		}
		if end <= cur.Block && have.Len() > 0 {
			cur = have.Pop()
		}
	}

	if cur.Block < end {
		ret.Push(cur)
	}
	for have.Len() > 0 {
		ret.Push(have.Pop())
	}

	initial := ret.Pop()
	var splits []BlockDistributionSplit

	last := initial
	for ret.Len() > 0 {
		next := ret.Pop()
		if last.Region != next.Region {
			splits = append(splits, next)
		} // else no split needed: same region
		last = next
	}
	return NewBlockDistribution(initial.Region, splits...)
}

func (t BlockDistribution) Initial() model.Region {
	return model.Region(t.pb.GetRegion())
}

func (t BlockDistribution) Splits() []BlockDistributionSplit {
	return slicex.Map(t.pb.GetSplits(), func(t *splitterprivatepb.BlockDistribution_Split) BlockDistributionSplit {
		return BlockDistributionSplit{
			Block:  Block(t.GetBlock()),
			Region: model.Region(t.GetRegion()),
		}
	})
}

func (t BlockDistribution) Find(n Block) model.Region {
	splits := t.Splits()
	for i := len(splits); i > 0; i-- {
		if split := splits[i-1]; split.Block <= n {
			return split.Region
		}
	}
	return t.Initial()
}

func (t BlockDistribution) ToDistribution() model.Distribution {
	splits := slicex.Map(t.Splits(), func(s BlockDistributionSplit) model.DistributionSplit {
		key, _ := Divide(int64(s.Block), 1024)
		return model.DistributionSplit{Key: model.Key(key), Region: s.Region}
	})
	return model.NewDistribution(t.Initial(), splits...)
}

func (t BlockDistribution) Equals(o BlockDistribution) bool {
	if t.Initial() != o.Initial() {
		return false
	}
	return slices.Equal(t.Splits(), o.Splits())
}

func (t BlockDistribution) String() string {
	splits := slicex.Map(t.Splits(), BlockDistributionSplit.String)
	if len(splits) == 0 {
		return string(t.Initial())
	}
	return fmt.Sprintf("%v/%v", t.Initial(), strings.Join(splits, "/"))
}

func newSplitHeap(t BlockDistribution, cmp func(a, b BlockDistributionSplit) bool) *container.Heap[BlockDistributionSplit] {
	ret := container.NewHeap(cmp)
	ret.Push(BlockDistributionSplit{Region: t.Initial()})
	for _, s := range t.Splits() {
		ret.Push(s)
	}
	return ret
}

type PlacementState = splitterprivatepb.InternalPlacement_State

const (
	PlacementActive         = splitterprivatepb.InternalPlacement_ACTIVE
	PlacementSuspended      = splitterprivatepb.InternalPlacement_SUSPENDED
	PlacementDecommissioned = splitterprivatepb.InternalPlacement_DECOMMISSIONED
)

func ParsePlacementState(str string) (PlacementState, bool) {
	num, ok := splitterprivatepb.InternalPlacement_State_value[strings.ToUpper(str)]
	return PlacementState(num), ok
}

type InternalPlacementConfig struct {
	pb *splitterprivatepb.InternalPlacement_Config
}

func WrapInternalPlacementConfig(pb *splitterprivatepb.InternalPlacement_Config) InternalPlacementConfig {
	return InternalPlacementConfig{pb: pb}
}

func UnwrapInternalPlacementConfig(t InternalPlacementConfig) *splitterprivatepb.InternalPlacement_Config {
	return t.pb
}

func ParseInternalPlacementConfig(pb *splitterprivatepb.InternalPlacement_Config) (InternalPlacementConfig, error) {
	return WrapInternalPlacementConfig(pb), nil
}

func NewInternalPlacementConfig(target, current BlockDistribution, speed int) InternalPlacementConfig {
	return WrapInternalPlacementConfig(&splitterprivatepb.InternalPlacement_Config{
		Target:         UnwrapBlockDistribution(target),
		Current:        UnwrapBlockDistribution(current),
		BlocksPerCycle: int32(max(1, speed)),
	})
}

func (t InternalPlacementConfig) String() string {
	return protox.MarshalTextString(t.pb)
}

type UpdateInternalPlacementOption func(placement *splitterprivatepb.InternalPlacement)

func WithInternalPlacementState(state PlacementState) UpdateInternalPlacementOption {
	return func(placement *splitterprivatepb.InternalPlacement) {
		placement.State = state
	}
}

func WithInternalPlacementConfig(cfg InternalPlacementConfig) UpdateInternalPlacementOption {
	return func(placement *splitterprivatepb.InternalPlacement) {
		placement.Config = cfg.pb
	}
}

type InternalPlacement struct {
	pb *splitterprivatepb.InternalPlacement
}

func NewInternalPlacement(name model.QualifiedPlacementName, config InternalPlacementConfig, now time.Time) InternalPlacement {
	return WrapInternalPlacement(&splitterprivatepb.InternalPlacement{
		Name:    name.ToProto(),
		State:   PlacementActive,
		Config:  UnwrapInternalPlacementConfig(config),
		Created: timestamppb.New(now),
	})
}

func UpdateInternalPlacement(p InternalPlacement, opts ...UpdateInternalPlacementOption) InternalPlacement {
	pb := protox.Clone(p.pb)
	for _, fn := range opts {
		fn(pb)
	}
	return WrapInternalPlacement(pb)
}

func WrapInternalPlacement(pb *splitterprivatepb.InternalPlacement) InternalPlacement {
	return InternalPlacement{pb: pb}
}

func UnwrapInternalPlacement(t InternalPlacement) *splitterprivatepb.InternalPlacement {
	return t.pb
}

func (t InternalPlacement) Name() model.QualifiedPlacementName {
	ret, _ := model.ParseQualifiedPlacementName(t.pb.GetName())
	return ret
}

func (t InternalPlacement) State() PlacementState {
	return t.pb.GetState()
}

func (t InternalPlacement) IsActive() bool {
	return t.pb.GetState() == splitterprivatepb.InternalPlacement_ACTIVE
}

func (t InternalPlacement) IsSuspended() bool {
	return t.pb.GetState() == splitterprivatepb.InternalPlacement_SUSPENDED
}

func (t InternalPlacement) IsDecommissioned() bool {
	return t.pb.GetState() == splitterprivatepb.InternalPlacement_DECOMMISSIONED
}

func (t InternalPlacement) Target() BlockDistribution {
	return WrapBlockDistribution(t.pb.GetConfig().GetTarget())
}

func (t InternalPlacement) Current() BlockDistribution {
	return WrapBlockDistribution(t.pb.GetConfig().GetCurrent())
}

func (t InternalPlacement) BlocksPerCycle() int {
	return int(t.pb.GetConfig().GetBlocksPerCycle())
}

func (t InternalPlacement) Created() time.Time {
	return t.pb.GetCreated().AsTime()
}

func (t InternalPlacement) ToPlacement() model.Placement {
	return model.NewPlacement(t.Name(), t.Current().ToDistribution())
}

func (t InternalPlacement) String() string {
	return protox.MarshalTextString(t.pb)
}

func (t InternalPlacement) Equals(o InternalPlacement) bool {
	return protox.Equal(t.pb, o.pb)
}

type InternalPlacementInfo struct {
	pb *splitterprivatepb.InternalPlacementInfo
}

func WrapInternalPlacementInfo(pb *splitterprivatepb.InternalPlacementInfo) InternalPlacementInfo {
	return InternalPlacementInfo{pb: pb}
}

func UnwrapInternalPlacementInfo(t InternalPlacementInfo) *splitterprivatepb.InternalPlacementInfo {
	return t.pb
}

func NewInternalPlacementInfo(placement InternalPlacement, version model.Version, now time.Time) InternalPlacementInfo {
	return WrapInternalPlacementInfo(&splitterprivatepb.InternalPlacementInfo{
		Placement: UnwrapInternalPlacement(placement),
		Version:   int64(version),
		Timestamp: timestamppb.New(now),
	})
}

func (t InternalPlacementInfo) Name() model.QualifiedPlacementName {
	return t.InternalPlacement().Name()
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

func (t InternalPlacementInfo) ToPlacementInfo() model.PlacementInfo {
	return model.NewPlacementInfo(t.InternalPlacement().ToPlacement(), t.Version(), t.Timestamp())
}

func (t InternalPlacementInfo) String() string {
	return protox.MarshalTextString(t.pb)
}

func (t InternalPlacementInfo) Equals(o InternalPlacementInfo) bool {
	return protox.Equal(t.pb, o.pb)
}

// Divide returns a/b * (uuid.Max+1), i.e, the UUID of the a'th of b partitions.
func Divide(a, b int64) (uuid.UUID, error) {
	if b <= a || b < 1 {
		return uuid.UUID{}, fmt.Errorf("invalid")
	}

	maxValue := uuidx.Domain.To()
	end := big.NewInt(0).Add(big.NewInt(0).SetBytes(maxValue[:]), big.NewInt(1))
	ret := big.NewInt(0).Div(big.NewInt(0).Mul(end, big.NewInt(a)), big.NewInt(b))

	return uuid.FromBytes(ret.FillBytes(make([]byte, 16)))
}
