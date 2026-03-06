package core

import (
	"fmt"
	"time"

	"github.com/hashicorp/raft"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/splitter/pkg/model"
)

const (
	TenantKey       metrics.Key = "tenant"
	ServiceKey      metrics.Key = "service"
	DomainKey       metrics.Key = "domain"
	PlacementKey    metrics.Key = "placement"
	RaftServerIdKey metrics.Key = "raft_server_id"
	ResultKey       metrics.Key = "result"
	ActionKey       metrics.Key = "action"
	StatusKey       metrics.Key = "status"
	GrantStateKey   metrics.Key = "grant_state"
	LeaseStateKey   metrics.Key = "lease_state"
	MessageTypeKey  metrics.Key = "message_type"
	InstanceIDKey   metrics.Key = "instance_id"
	LocationKey     metrics.Key = "location"
	ShardRegionKey  metrics.Key = "shard_region"
)

var (
	QualifiedDomainKeys    = []metrics.Key{TenantKey, ServiceKey, DomainKey}
	QualifiedServiceKeys   = []metrics.Key{TenantKey, ServiceKey}
	QualifiedPlacementKeys = []metrics.Key{TenantKey, PlacementKey}

	GrantDurationBucketOptions = &metrics.BucketOptions{
		DistributionType: metrics.UserDefined,
		LatencyUnit:      time.Second,
		UserDefinedBuckets: []float64{
			1,
			5,
			60,
			600,     // 10m
			1800,    // 30m
			3600,    // 1h
			21_600,  // 6h
			57_600,  // 12h
			86_400,  // 24h
			345_600, // 3d
			604_800, // 1w
		},
	}
)

func QualifiedDomainTags(v model.QualifiedDomainName) []metrics.Tag {
	return []metrics.Tag{TenantTag(v.Service.Tenant), ServiceTag(v.Service.Service), DomainTag(v.Domain)}
}

func QualifiedServiceTags(v model.QualifiedServiceName) []metrics.Tag {
	return []metrics.Tag{TenantTag(v.Tenant), ServiceTag(v.Service)}
}

func QualifiedPlacementTags(v model.QualifiedPlacementName) []metrics.Tag {
	return []metrics.Tag{TenantTag(v.Tenant), PlacementTag(v.Placement)}
}

func TenantTag(v model.TenantName) metrics.Tag {
	return metrics.Tag{Key: TenantKey, Value: string(v)}
}

func ServiceTag(v model.ServiceName) metrics.Tag {
	return metrics.Tag{Key: ServiceKey, Value: string(v)}
}

func DomainTag(v model.DomainName) metrics.Tag {
	return metrics.Tag{Key: DomainKey, Value: string(v)}
}

func PlacementTag(v model.PlacementName) metrics.Tag {
	return metrics.Tag{Key: PlacementKey, Value: string(v)}
}

func RaftServerIdTag(v raft.ServerID) metrics.Tag {
	return metrics.Tag{Key: RaftServerIdKey, Value: string(v)}
}

func ResultTag(v any) metrics.Tag {
	return metrics.Tag{Key: ResultKey, Value: fmt.Sprintf("%v", v)}
}

// ResultErrorTag converts an error to a small set of result tags.
func ResultErrorTag(err error) metrics.Tag {
	if err != nil {
		if model.IsPermanentError(err) {
			return ResultTag(err.Error())
		}
		return ResultTag("failed")
	}
	return ResultTag("ok")
}

func ActionTag(v any) metrics.Tag {
	return metrics.Tag{Key: ActionKey, Value: fmt.Sprintf("%v", v)}
}

func StatusTag(v any) metrics.Tag {
	return metrics.Tag{Key: StatusKey, Value: fmt.Sprintf("%v", v)}
}

func GrantStateTag(v model.GrantState) metrics.Tag {
	return metrics.Tag{Key: GrantStateKey, Value: fmt.Sprintf("%v", v)}
}

func LeaseStateTag(v string) metrics.Tag {
	return metrics.Tag{Key: LeaseStateKey, Value: fmt.Sprintf("%v", v)}
}

func MessageTypeTag(v any) metrics.Tag {
	return metrics.Tag{Key: MessageTypeKey, Value: fmt.Sprintf("%v", v)}
}

func InstanceIDTag(v location.InstanceID) metrics.Tag {
	return metrics.Tag{Key: InstanceIDKey, Value: fmt.Sprintf("%v", v)}
}

func LocationTag(v location.Location) metrics.Tag {
	return metrics.Tag{Key: LocationKey, Value: fmt.Sprintf("%v", v)}
}

func ShardRegionTag(v location.Region) metrics.Tag {
	return metrics.Tag{Key: ShardRegionKey, Value: fmt.Sprintf("%v", v)}
}
