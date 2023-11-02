package model

import (
	"go.atoms.co/lib/metrics"
)

const (
	tenantKey    metrics.Key = "tenant"
	operationKey metrics.Key = "operation"
	resultKey    metrics.Key = "result"
	statusKey    metrics.Key = "status"
)

func tenantTag(t TenantName) metrics.Tag {
	return metrics.Tag{Key: tenantKey, Value: string(t)}
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
