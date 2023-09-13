package session

import (
	"go.atoms.co/lib/metrics"
	"fmt"
)

const (
	messageTypeKey metrics.Key = "message_type"
)

func messageTypeTag(t string) metrics.Tag {
	return metrics.Tag{Key: messageTypeKey, Value: fmt.Sprintf("%v", t)}
}
