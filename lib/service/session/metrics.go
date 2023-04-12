package session

import (
	"go.atoms.co/lib/metrics"
	"fmt"
)

const (
	sessionIDKey   metrics.Key = "session_id"
	messageTypeKey metrics.Key = "message_type"
)

func sessionIDTag(sid ID) metrics.Tag {
	return metrics.Tag{Key: sessionIDKey, Value: fmt.Sprintf("%v", sid)}
}

func messageTypeTag(t string) metrics.Tag {
	return metrics.Tag{Key: messageTypeKey, Value: fmt.Sprintf("%v", t)}
}
