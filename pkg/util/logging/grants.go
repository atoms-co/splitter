package logging

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"

	"go.atoms.co/lib/log"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/lib/service/location"
	splitterpb "go.atoms.co/splitter/pb"
	"go.atoms.co/splitter/pkg/allocation"
)

type Source string

const (
	SourceCoordinator Source = "coordinator"
	SourceConsumer    Source = "consumer"
)

type EventType string

const (
	EventAssign  EventType = "assign"
	EventPromote EventType = "promote"
	EventRevoke  EventType = "revoke"
	EventUpdate  EventType = "update"
	EventRelease EventType = "release"
	EventExpire  EventType = "expire"
	EventRemove  EventType = "remove"
	// EventCheckpoint records current grants grouped by shard
	EventCheckpoint EventType = "checkpoint"

	maxGrantsPerLog = 64
)

type GrantEvent struct {
	Source Source
	Type   EventType

	Shard  *splitterpb.Shard
	Grant  allocation.GrantID
	Worker location.InstanceID

	FromState *splitterpb.GrantState
	ToState   *splitterpb.GrantState
}

type GrantSnapshot struct {
	Grant          string
	Worker         string
	ConsumerRegion string
	ConsumerNode   string
	State          splitterpb.GrantState
	AssignedAt     *time.Time
}

type ShardSnapshot struct {
	Domain      string
	ShardRegion string
	ShardFrom   string
	ShardTo     string
	Grants      []GrantSnapshot
}

type grantLogSnapshot struct {
	Grant          string     `json:"grant"`
	Worker         string     `json:"worker"`
	ConsumerRegion string     `json:"consumer_region,omitempty"`
	ConsumerNode   string     `json:"consumer_node,omitempty"`
	State          string     `json:"state"`
	AssignedAt     *time.Time `json:"assigned_at,omitempty"`
}

type shardLogSnapshot struct {
	Domain      string             `json:"domain"`
	ShardRegion string             `json:"shard_region"`
	ShardFrom   string             `json:"shard_from"`
	ShardTo     string             `json:"shard_to"`
	Grants      []grantLogSnapshot `json:"grants"`
}

func grantEventFields(event GrantEvent) []log.Field {
	fields := []log.Field{log.String("event_source", string(event.Source))}
	if event.Type != "" {
		fields = append(fields, log.String("event_type", string(event.Type)))
	}
	if event.Type == EventCheckpoint {
		return fields
	}
	fields = append(fields,
		log.String("domain", event.Shard.GetDomain().GetName()),
		log.String("shard_region", event.Shard.GetRegion()),
		log.String("shard_from", event.Shard.GetFrom()),
		log.String("shard_to", event.Shard.GetTo()),
		log.String("grant", string(event.Grant)),
		log.String("worker", string(event.Worker)),
	)
	if event.FromState != nil {
		fields = append(fields, log.String("from_state", strings.ToLower(event.FromState.String())))
	}
	if event.ToState != nil {
		fields = append(fields, log.String("to_state", strings.ToLower(event.ToState.String())))
	}
	return fields
}

func OutputGrantEvent(ctx context.Context, severity log.Severity, message string, event GrantEvent, fields ...log.Field) {
	fields = append(grantEventFields(event), fields...)
	log.Output(log.NewContext(ctx, fields...), severity, 2, message)
}

func OutputGrants(ctx context.Context, severity log.Severity, message string, event GrantEvent, at time.Time, shards []ShardSnapshot, fields ...log.Field) {
	id := uuid.NewString()
	logShards := slicex.Map(shards, func(shard ShardSnapshot) shardLogSnapshot {
		return shardLogSnapshot{
			Domain:      shard.Domain,
			ShardRegion: shard.ShardRegion,
			ShardFrom:   shard.ShardFrom,
			ShardTo:     shard.ShardTo,
			Grants: slicex.Map(shard.Grants, func(grant GrantSnapshot) grantLogSnapshot {
				return grantLogSnapshot{Grant: grant.Grant, Worker: grant.Worker, ConsumerRegion: grant.ConsumerRegion, ConsumerNode: grant.ConsumerNode, State: strings.ToLower(grant.State.String()), AssignedAt: grant.AssignedAt}
			}),
		}
	})

	event.Type = EventCheckpoint
	parts := splitShards(logShards)
	for partIndex, part := range parts {
		partFields := []log.Field{log.String("checkpoint_id", id), log.Time("checkpoint_at", at), log.Int("checkpoint_part_index", partIndex), log.Int("checkpoint_part_count", len(parts)), log.Reflect("shards", part)}
		logFields := grantEventFields(event)
		logFields = append(logFields, partFields...)
		logFields = append(logFields, fields...)
		log.Output(log.NewContext(ctx, logFields...), severity, 2, message)
	}
}

func splitShards(shards []shardLogSnapshot) [][]shardLogSnapshot {
	// Split shards into logs of at most maxGrantsPerLog grants.
	chunks := [][]shardLogSnapshot{make([]shardLogSnapshot, 0, maxGrantsPerLog)}
	grantCount := 0
	for _, shard := range shards {
		shardGrantCount := len(shard.Grants)
		last := len(chunks) - 1
		if len(chunks[last]) > 0 && grantCount+shardGrantCount > maxGrantsPerLog {
			chunks = append(chunks, make([]shardLogSnapshot, 0, maxGrantsPerLog))
			grantCount = 0
			last++
		}
		chunks[last] = append(chunks[last], shard)
		grantCount += shardGrantCount
	}
	return chunks
}
