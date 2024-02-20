package core

import (
	"go.atoms.co/lib/metrics"
	"go.atoms.co/splitter/pkg/model"
	"fmt"
	"github.com/hashicorp/raft"
)

const (
	TenantKey            metrics.Key = "tenant"
	ServiceKey           metrics.Key = "service"
	DomainKey            metrics.Key = "domain"
	PlacementKey         metrics.Key = "placement"
	RaftServerIdKey      metrics.Key = "raft_server_id"
	ResultKey            metrics.Key = "result"
	ActionKey            metrics.Key = "action"
	StatusKey            metrics.Key = "status"
	GrantStateKey        metrics.Key = "grant_state"
	GrantModificationKey metrics.Key = "grant_modification"
	LeaseStateKey        metrics.Key = "lease_state"
	MessageTypeKey       metrics.Key = "message_type"
)

var (
	QualifiedDomainKeys    = []metrics.Key{TenantKey, ServiceKey, DomainKey}
	QualifiedServiceKeys   = []metrics.Key{TenantKey, ServiceKey}
	QualifiedPlacementKeys = []metrics.Key{TenantKey, PlacementKey}
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

func GrantModificationTag(v model.GrantState) metrics.Tag {
	return metrics.Tag{Key: GrantModificationKey, Value: fmt.Sprintf("%v", v)}
}

func LeaseStateTag(v string) metrics.Tag {
	return metrics.Tag{Key: LeaseStateKey, Value: fmt.Sprintf("%v", v)}
}

func MessageTypeTag(v any) metrics.Tag {
	return metrics.Tag{Key: MessageTypeKey, Value: fmt.Sprintf("%v", v)}
}
