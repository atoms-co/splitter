package model

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"

	"go.atoms.co/lib/log"
	"go.atoms.co/lib/mapx"
)

// GrantEventType identifies an event in structured grant logging.
type GrantEventType string

type grantLogSource string

const (
	// GrantAssigned records a grant assignment.
	GrantAssigned GrantEventType = "assign"
	// GrantPromoted records a grant promotion to active.
	GrantPromoted GrantEventType = "promote"
	// GrantRevoked records a revoked grant.
	GrantRevoked GrantEventType = "revoke"
	// GrantUpdated records a grant becoming loaded or unloaded.
	GrantUpdated GrantEventType = "update"
	// GrantReleased records a released grant.
	GrantReleased GrantEventType = "release"
	// GrantExpired records an expired grant.
	GrantExpired GrantEventType = "expire"
	// GrantRemoved records a grant removed from a consumer.
	GrantRemoved GrantEventType = "remove"
	// GrantAttached records a grant accepted from a reconnecting consumer.
	GrantAttached GrantEventType = "attach"
	// GrantCheckpoint records current grants grouped by shard.
	GrantCheckpoint GrantEventType = "checkpoint"
	// CoordinatorStarted records the beginning of a coordinator allocation epoch.
	CoordinatorStarted GrantEventType = "coordinator_start"

	grantLogSourceCoordinator grantLogSource = "coordinator"
	grantLogSourceConsumer    grantLogSource = "consumer"

	maxGrantsPerLog = 64
)

type grantEvent struct {
	source    grantLogSource
	eventType GrantEventType

	shard  Shard
	grant  GrantID
	worker InstanceID

	fromState *GrantState
	toState   *GrantState
}

// grantLogSnapshot is serialized by log.Reflect into each shard's grants array in checkpoint logs.
type grantLogSnapshot struct {
	Grant          string     `json:"grant"`
	Worker         string     `json:"worker"`
	ConsumerRegion string     `json:"consumer_region,omitempty"`
	ConsumerNode   string     `json:"consumer_node,omitempty"`
	State          string     `json:"state"`
	AssignedAt     *time.Time `json:"assigned_at,omitempty"`
}

// shardLogSnapshot is serialized by log.Reflect into the shards array in checkpoint logs.
type shardLogSnapshot struct {
	Domain      string             `json:"domain"`
	ShardRegion string             `json:"shard_region"`
	ShardFrom   string             `json:"shard_from"`
	ShardTo     string             `json:"shard_to"`
	Grants      []grantLogSnapshot `json:"grants"`
}

func grantEventFields(event grantEvent) []log.Field {
	fields := []log.Field{log.String("event_source", string(event.source))}
	if event.eventType != "" {
		fields = append(fields, log.String("event_type", string(event.eventType)))
	}
	if event.eventType == GrantCheckpoint || event.eventType == CoordinatorStarted {
		return fields
	}
	fields = append(fields, log.String("domain", string(event.shard.Domain.Domain)), log.String("shard_region", string(event.shard.Region)))
	fields = append(fields, log.String("shard_from", event.shard.From.String()), log.String("shard_to", event.shard.To.String()))
	fields = append(fields, log.String("grant", string(event.grant)), log.String("worker", string(event.worker)))
	if event.fromState != nil {
		fields = append(fields, log.String("from_state", strings.ToLower(event.fromState.String())))
	}
	if event.toState != nil {
		fields = append(fields, log.String("to_state", strings.ToLower(event.toState.String())))
	}
	return fields
}

func logGrantEvent(ctx context.Context, severity log.Severity, message string, event grantEvent, calldepth int, fields ...log.Field) {
	fields = append(grantEventFields(event), fields...)
	log.Output(log.NewContext(ctx, fields...), severity, calldepth, message)
}

// LogCoordinatorStarted records the beginning of a coordinator allocation epoch.
func LogCoordinatorStarted(ctx context.Context, severity log.Severity, message string) {
	logGrantEvent(ctx, severity, message, grantEvent{source: grantLogSourceCoordinator, eventType: CoordinatorStarted}, 3)
}

// LogCoordinatorGrantEvent emits structured fields for a coordinator grant event.
func LogCoordinatorGrantEvent(ctx context.Context, severity log.Severity, message string, eventType GrantEventType, shard Shard, grant GrantID, worker InstanceID, fromState, toState *GrantState, fields ...log.Field) {
	event := grantEvent{
		source:    grantLogSourceCoordinator,
		eventType: eventType,
		shard:     shard,
		grant:     grant,
		worker:    worker,
		fromState: fromState,
		toState:   toState,
	}
	logGrantEvent(ctx, severity, message, event, 3, fields...)
}

func coordinatorGrantShards(cluster *ClusterMap) []shardLogSnapshot {
	grouped := make(map[Shard][]grantLogSnapshot)
	for _, assignment := range cluster.Assignments() {
		consumer := assignment.Consumer()
		location := consumer.Location()
		for _, grant := range assignment.Grants() {
			shard := grant.Shard()
			grouped[shard] = append(grouped[shard], grantLogSnapshot{
				Grant:          string(grant.ID()),
				Worker:         string(consumer.ID()),
				ConsumerRegion: string(location.Region),
				ConsumerNode:   string(location.Node),
				State:          strings.ToLower(grant.State().String()),
			})
		}
	}

	return mapx.MapToSlice(grouped, func(shard Shard, grants []grantLogSnapshot) shardLogSnapshot {
		return shardLogSnapshot{
			Domain:      string(shard.Domain.Domain),
			ShardRegion: string(shard.Region),
			ShardFrom:   shard.From.String(),
			ShardTo:     shard.To.String(),
			Grants:      grants,
		}
	})
}

func logGrants(ctx context.Context, severity log.Severity, message string, source grantLogSource, at time.Time, shards []shardLogSnapshot, calldepth int, fields ...log.Field) {
	id := uuid.NewString()
	parts := splitShards(shards)
	for partIndex, part := range parts {
		partFields := []log.Field{log.String("checkpoint_id", id), log.Time("checkpoint_at", at), log.Int("checkpoint_part_index", partIndex), log.Int("checkpoint_part_count", len(parts)), log.Reflect("shards", part)}
		logFields := grantEventFields(grantEvent{source: source, eventType: GrantCheckpoint})
		logFields = append(logFields, partFields...)
		logFields = append(logFields, fields...)
		log.Output(log.NewContext(ctx, logFields...), severity, calldepth, message)
	}
}

// LogCoordinatorGrants logs the coordinator's current grants grouped by shard,
// splitting large grant sets across multiple log entries.
func LogCoordinatorGrants(ctx context.Context, severity log.Severity, message string, at time.Time, cluster *ClusterMap, fields ...log.Field) {
	logGrants(ctx, severity, message, grantLogSourceCoordinator, at, coordinatorGrantShards(cluster), 3, fields...)
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
