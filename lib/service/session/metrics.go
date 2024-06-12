package session

import (
	"fmt"

	"go.atoms.co/lib/metrics"
)

const (
	messageTypeKey metrics.Key = "message_type"
)

func messageTypeTag(t string) metrics.Tag {
	return metrics.Tag{Key: messageTypeKey, Value: fmt.Sprintf("%v", t)}
}
