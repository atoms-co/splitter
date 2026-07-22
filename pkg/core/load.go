package core

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"go.atoms.co/lib/encoding/protox"
	"go.atoms.co/slicex"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/util/p2quantile"
)

type Shard struct {
	From   model.Key
	To     model.Key
	Region model.Region
}

func (s Shard) ToProto() *splitterprivatepb.Shard {
	return &splitterprivatepb.Shard{
		From:   s.From.String(),
		To:     s.To.String(),
		Region: string(s.Region),
	}
}

func NewShard(from, to model.Key, region model.Region) Shard {
	return Shard{
		From:   from,
		To:     to,
		Region: region,
	}
}

func parseShard(pb *splitterprivatepb.Shard) (Shard, error) {
	from, err := model.ParseKey(pb.GetFrom())
	if err != nil {
		return Shard{}, fmt.Errorf("invalid 'from' key: %w", err)
	}

	to, err := model.ParseKey(pb.GetTo())
	if err != nil {
		return Shard{}, fmt.Errorf("invalid 'to' key: %w", err)
	}

	return NewShard(from, to, model.Region(pb.GetRegion())), nil
}

type ShardQuantileInfo struct {
	pb *splitterprivatepb.DomainQuantileInfo_ShardQuantileInfo
}

func WrapShardQuantileInfo(pb *splitterprivatepb.DomainQuantileInfo_ShardQuantileInfo) ShardQuantileInfo {
	return ShardQuantileInfo{pb: pb}
}

func UnwrapShardQuantileInfo(q ShardQuantileInfo) *splitterprivatepb.DomainQuantileInfo_ShardQuantileInfo {
	return q.pb
}

func NewShardQuantileInfo(shard Shard, quantile float64) ShardQuantileInfo {
	return ShardQuantileInfo{pb: &splitterprivatepb.DomainQuantileInfo_ShardQuantileInfo{
		Shard:    shard.ToProto(),
		Quantile: quantile,
	}}
}

func (q ShardQuantileInfo) Shard() (Shard, error) {
	return parseShard(q.pb.GetShard())
}

func (q ShardQuantileInfo) Quantile() float64 {
	return q.pb.GetQuantile()
}

func (q ShardQuantileInfo) String() string {
	return protox.CompactTextString(q.pb)
}

type DomainQuantileInfo struct {
	pb *splitterprivatepb.DomainQuantileInfo
}

func WrapDomainQuantileInfo(pb *splitterprivatepb.DomainQuantileInfo) DomainQuantileInfo {
	return DomainQuantileInfo{pb: pb}
}

func UnwrapDomainQuantileInfo(q DomainQuantileInfo) *splitterprivatepb.DomainQuantileInfo {
	return q.pb
}

func NewDomainQuantileInfo(domainQuantile float64, shardQuantiles []ShardQuantileInfo) DomainQuantileInfo {
	return DomainQuantileInfo{pb: &splitterprivatepb.DomainQuantileInfo{
		DomainQuantile: domainQuantile,
		ShardQuantiles: slicex.Map(shardQuantiles, UnwrapShardQuantileInfo),
	}}
}

func (q DomainQuantileInfo) DomainQuantile() float64 {
	return q.pb.GetDomainQuantile()
}

func (q DomainQuantileInfo) ShardQuantiles() []ShardQuantileInfo {
	return slicex.Map(q.pb.GetShardQuantiles(), WrapShardQuantileInfo)
}

func (q DomainQuantileInfo) String() string {
	return protox.MarshalTextString(q.pb)
}

type P2QuantileSnapshot struct {
	pb *splitterprivatepb.DomainTrackerSnapshot_P2QuantileSnapshot
}

func WrapP2QuantileSnapshot(pb *splitterprivatepb.DomainTrackerSnapshot_P2QuantileSnapshot) P2QuantileSnapshot {
	return P2QuantileSnapshot{pb: pb}
}

func UnwrapP2QuantileSnapshot(q P2QuantileSnapshot) *splitterprivatepb.DomainTrackerSnapshot_P2QuantileSnapshot {
	return q.pb
}

func NewP2QuantileSnapshot(percentile float64, height []float64, markerPos []uint64, desiredMarkerPos []float64) P2QuantileSnapshot {
	return P2QuantileSnapshot{pb: &splitterprivatepb.DomainTrackerSnapshot_P2QuantileSnapshot{
		Percentile:       percentile,
		Height:           height,
		MarkerPos:        markerPos,
		DesiredMarkerPos: desiredMarkerPos,
	}}
}

func (s P2QuantileSnapshot) Percentile() float64 {
	return s.pb.GetPercentile()
}

func (s P2QuantileSnapshot) Height() []float64 {
	return s.pb.GetHeight()
}

func (s P2QuantileSnapshot) MarkerPos() []uint64 {
	return s.pb.GetMarkerPos()
}

func (s P2QuantileSnapshot) DesiredMarkerPos() []float64 {
	return s.pb.GetDesiredMarkerPos()
}

// Restore restores a p2quantile.P2Quantile from core.P2QuantileSnapshot.
func (s P2QuantileSnapshot) Restore() (*p2quantile.P2Quantile, error) {
	snapshot, err := p2quantile.NewSnapshot(s.Percentile(), s.Height(), s.MarkerPos(), s.DesiredMarkerPos())
	if err != nil {
		return nil, err
	}
	return p2quantile.Restore(snapshot), nil
}

func (s P2QuantileSnapshot) String() string {
	return protox.MarshalTextString(s.pb)
}

// SnapshotQuantile takes a snapshot for a p2quantile.P2Quantile.
func SnapshotQuantile(q *p2quantile.P2Quantile) P2QuantileSnapshot {
	if q == nil {
		return P2QuantileSnapshot{}
	}

	s := q.Snapshot()
	return NewP2QuantileSnapshot(s.Percentile, s.Height[:], s.MarkerPos[:], s.DesiredMarkerPos[:])
}

type ShardP2QuantileSnapshot struct {
	pb *splitterprivatepb.DomainTrackerSnapshot_ShardP2QuantileSnapshot
}

func WrapShardP2QuantileSnapshot(pb *splitterprivatepb.DomainTrackerSnapshot_ShardP2QuantileSnapshot) ShardP2QuantileSnapshot {
	return ShardP2QuantileSnapshot{pb: pb}
}

func UnwrapShardP2QuantileSnapshot(s ShardP2QuantileSnapshot) *splitterprivatepb.DomainTrackerSnapshot_ShardP2QuantileSnapshot {
	return s.pb
}

func NewShardP2QuantileSnapshot(shard Shard, snap P2QuantileSnapshot) ShardP2QuantileSnapshot {
	return ShardP2QuantileSnapshot{pb: &splitterprivatepb.DomainTrackerSnapshot_ShardP2QuantileSnapshot{
		Shard:    shard.ToProto(),
		Snapshot: UnwrapP2QuantileSnapshot(snap),
	}}
}

func (s ShardP2QuantileSnapshot) Shard() (Shard, error) {
	return parseShard(s.pb.GetShard())
}

func (s ShardP2QuantileSnapshot) Snapshot() P2QuantileSnapshot {
	return WrapP2QuantileSnapshot(s.pb.GetSnapshot())
}

func (s ShardP2QuantileSnapshot) String() string {
	return protox.MarshalTextString(s.pb)
}

type DomainTrackerSnapshot struct {
	pb *splitterprivatepb.DomainTrackerSnapshot
}

func WrapDomainTrackerSnapshot(pb *splitterprivatepb.DomainTrackerSnapshot) DomainTrackerSnapshot {
	return DomainTrackerSnapshot{pb: pb}
}

func UnwrapDomainTrackerSnapshot(s DomainTrackerSnapshot) *splitterprivatepb.DomainTrackerSnapshot {
	return s.pb
}

func NewDomainTrackerSnapshot(createdAt time.Time, domainSnap P2QuantileSnapshot, shardSnap []ShardP2QuantileSnapshot) DomainTrackerSnapshot {
	return DomainTrackerSnapshot{pb: &splitterprivatepb.DomainTrackerSnapshot{
		CreatedAt:      timestamppb.New(createdAt),
		DomainSnapshot: UnwrapP2QuantileSnapshot(domainSnap),
		ShardSnapshot:  slicex.Map(shardSnap, UnwrapShardP2QuantileSnapshot),
	}}
}

func (s DomainTrackerSnapshot) CreatedAt() time.Time {
	return s.pb.GetCreatedAt().AsTime()
}

func (s DomainTrackerSnapshot) DomainSnapshot() P2QuantileSnapshot {
	return WrapP2QuantileSnapshot(s.pb.GetDomainSnapshot())
}

func (s DomainTrackerSnapshot) ShardSnapshot() []ShardP2QuantileSnapshot {
	return slicex.Map(s.pb.GetShardSnapshot(), WrapShardP2QuantileSnapshot)
}

func (s DomainTrackerSnapshot) String() string {
	return protox.MarshalTextString(s.pb)
}

type DomainLoadInfo struct {
	pb *splitterprivatepb.DomainLoadInfo
}

func WrapDomainLoadInfo(pb *splitterprivatepb.DomainLoadInfo) DomainLoadInfo {
	return DomainLoadInfo{pb: pb}
}

func UnwrapDomainLoadInfo(m DomainLoadInfo) *splitterprivatepb.DomainLoadInfo {
	return m.pb
}

func NewDomainLoadInfo(domainName model.DomainName, trackerSnap DomainTrackerSnapshot, quantileInfo *DomainQuantileInfo) DomainLoadInfo {
	pb := &splitterprivatepb.DomainLoadInfo{
		DomainName:      string(domainName),
		TrackerSnapshot: UnwrapDomainTrackerSnapshot(trackerSnap),
	}

	if quantileInfo != nil {
		pb.QuantileInfo = UnwrapDomainQuantileInfo(*quantileInfo)
	}

	return DomainLoadInfo{pb: pb}
}

func (t DomainLoadInfo) DomainName() model.DomainName {
	return model.DomainName(t.pb.GetDomainName())
}

func (t DomainLoadInfo) TrackerSnapshot() DomainTrackerSnapshot {
	return WrapDomainTrackerSnapshot(t.pb.GetTrackerSnapshot())
}

func (t DomainLoadInfo) QuantileInfo() DomainQuantileInfo {
	return WrapDomainQuantileInfo(t.pb.GetQuantileInfo())
}

func (t DomainLoadInfo) HasQuantileInfo() bool {
	return t.pb.GetQuantileInfo() != nil
}

func (t DomainLoadInfo) String() string {
	return protox.MarshalTextString(t.pb)
}

type ServiceLoadInfo struct {
	pb *splitterprivatepb.ServiceLoadInfo
}

func WrapServiceLoadInfo(pb *splitterprivatepb.ServiceLoadInfo) ServiceLoadInfo {
	return ServiceLoadInfo{pb: pb}
}

func UnwrapServiceLoadInfo(s ServiceLoadInfo) *splitterprivatepb.ServiceLoadInfo {
	return s.pb
}

func NewServiceLoadInfo(service model.QualifiedServiceName, info []DomainLoadInfo) ServiceLoadInfo {
	return ServiceLoadInfo{pb: &splitterprivatepb.ServiceLoadInfo{
		Service: service.ToProto(),
		Info:    slicex.Map(info, UnwrapDomainLoadInfo),
	}}
}

func (t ServiceLoadInfo) Service() model.QualifiedServiceName {
	name, err := model.ParseQualifiedServiceName(t.pb.GetService())
	if err != nil {
		return model.QualifiedServiceName{}
	}
	return name
}

func (t ServiceLoadInfo) Domains() []DomainLoadInfo {
	return slicex.Map(t.pb.GetInfo(), WrapDomainLoadInfo)
}

func (t ServiceLoadInfo) String() string {
	return protox.MarshalTextString(t.pb)
}

type ServiceStatusMessage struct {
	pb *splitterprivatepb.WorkerMessage_ServiceStatus
}

func NewServiceStatusMessage(serviceLoad ServiceLoadInfo) ServiceStatusMessage {
	return ServiceStatusMessage{pb: &splitterprivatepb.WorkerMessage_ServiceStatus{
		Load: UnwrapServiceLoadInfo(serviceLoad),
	}}
}

func WrapServiceStatusMessage(pb *splitterprivatepb.WorkerMessage_ServiceStatus) ServiceStatusMessage {
	return ServiceStatusMessage{pb: pb}
}

func UnwrapServiceStatusMessage(m ServiceStatusMessage) *splitterprivatepb.WorkerMessage_ServiceStatus {
	return m.pb
}

func (m ServiceStatusMessage) Load() ServiceLoadInfo {
	return WrapServiceLoadInfo(m.pb.GetLoad())
}

func (m ServiceStatusMessage) String() string {
	return protox.MarshalTextString(m.pb)
}
