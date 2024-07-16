package model

import (
	"go.atoms.co/lib/metrics"
	"fmt"
)

const (
	tenantKey  metrics.Key = "tenant"
	serviceKey metrics.Key = "service"

	domainKey     metrics.Key = "domain"
	operationKey  metrics.Key = "operation"
	resultKey     metrics.Key = "result"
	statusKey     metrics.Key = "status"
	leaseStateKey metrics.Key = "lease_state"

	sourceKey        metrics.Key = "source"
	sourceVersionKey metrics.Key = "source_version"
)

var (
	qualifiedDomainKeys = []metrics.Key{tenantKey, serviceKey, domainKey}
)

func qualifiedDomainTags(v QualifiedDomainName) []metrics.Tag {
	return []metrics.Tag{tenantTag(v.Service.Tenant), serviceTag(v.Service.Service), domainTag(v.Domain)}
}

func tenantTag(t TenantName) metrics.Tag {
	return metrics.Tag{Key: tenantKey, Value: string(t)}
}

func serviceTag(v ServiceName) metrics.Tag {
	return metrics.Tag{Key: serviceKey, Value: string(v)}
}

func domainTag(v DomainName) metrics.Tag {
	return metrics.Tag{Key: domainKey, Value: string(v)}
}

func operationTag(op string) metrics.Tag {
	return metrics.Tag{Key: operationKey, Value: op}
}

func statusTag(err error) metrics.Tag {
	if err != nil {
		return metrics.Tag{Key: statusKey, Value: "fail"}
	}
	return metrics.Tag{Key: statusKey, Value: "ok"}
}

func resultTag(result string) metrics.Tag {
	return metrics.Tag{Key: resultKey, Value: result}
}

func leaseStateTag(v LeaseState) metrics.Tag {
	return metrics.Tag{Key: leaseStateKey, Value: fmt.Sprintf("%v", v)}
}

func sourceTag() metrics.Tag {
	return metrics.Tag{Key: sourceKey, Value: "splitter-go-client"}
}

func sourceVersionTag(v string) metrics.Tag {
	return metrics.Tag{Key: sourceVersionKey, Value: v}
}
