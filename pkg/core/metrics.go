package core

import (
	"go.atoms.co/lib/metrics"
	"go.atoms.co/splitter/pkg/model"
	"github.com/hashicorp/raft"
)

const (
	TenantKey       metrics.Key = "tenant"
	DomainKey       metrics.Key = "domain"
	RaftServerIdKey metrics.Key = "raft_server_id"
)

var (
	QualifiedDomainKeys = []metrics.Key{TenantKey, DomainKey}
)

func TenantTag(v model.TenantName) metrics.Tag {
	return metrics.Tag{Key: TenantKey, Value: string(v)}
}

func DomainTag(v model.DomainName) metrics.Tag {
	return metrics.Tag{Key: DomainKey, Value: string(v)}
}

func RaftServerIdTag(v raft.ServerID) metrics.Tag {
	return metrics.Tag{Key: RaftServerIdKey, Value: string(v)}
}
