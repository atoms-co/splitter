package model

import (
	"context"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/slicex"
	"fmt"
	"google.golang.org/grpc/status"
)

const (
	tenantKey  metrics.Key = "tenant"
	serviceKey metrics.Key = "service"

	domainKey     metrics.Key = "domain"
	operationKey  metrics.Key = "operation"
	resultKey     metrics.Key = "result"
	handlerKey    metrics.Key = "handler"
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

func handlerTag(handler string) metrics.Tag {
	return metrics.Tag{Key: handlerKey, Value: handler}
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

var (
	numForwarded = metrics.NewSingleViewCounter("go.atoms.co/splitter/client/forwarded_requests", "Number of forwarded requests", slicex.CopyAppend(qualifiedDomainKeys, resultKey)...)
	numHandled   = metrics.NewSingleViewCounter("go.atoms.co/splitter/client/handled_requests", "Number of requests handled locally", slicex.CopyAppend(qualifiedDomainKeys, resultKey, handlerKey)...)
)

func recordForwardedRequest(ctx context.Context, domain QualifiedDomainName, result string) {
	numForwarded.Increment(ctx, 1, slicex.CopyAppend(qualifiedDomainTags(domain), resultTag(result))...)
}

func recordHandledRequest(ctx context.Context, domain QualifiedDomainName, handler, result string) {
	numHandled.Increment(ctx, 1, slicex.CopyAppend(qualifiedDomainTags(domain), resultTag(result), handlerTag(handler))...)
}

func recordHandledRequestError(ctx context.Context, domain QualifiedDomainName, handler string, err error) {
	var result string
	if st, ok := status.FromError(err); ok {
		result = st.Code().String()
	} else {
		result = "error"
	}
	numHandled.Increment(ctx, 1, slicex.CopyAppend(qualifiedDomainTags(domain), resultTag(result), handlerTag(handler))...)
}
