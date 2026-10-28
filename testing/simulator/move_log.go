package simulator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.atoms.co/lib/log"

	"go.atoms.co/splitter/pkg/model"
)

// The real coordinator starts with a fresh background context. Scenario runs
// therefore serialize, while a separate mutex protects the active log receiver.
// The logger itself is installed once by TestMain, never replaced during a run.
var simulationMu sync.Mutex

var moveCapture struct {
	sync.RWMutex
	simulation *simulation
}

type moveLogger struct{ log.Logger }

func (l moveLogger) Log(ctx context.Context, severity log.Severity, calldepth int, message string) {
	if message == "Initiated grant move" {
		fields := log.FromContext(ctx)
		var tenant, service string
		for _, field := range fields {
			if field.Key == "tenant" {
				tenant = field.String
			}
			if field.Key == "service" {
				service = field.String
			}
		}
		moveCapture.RLock()
		s := moveCapture.simulation
		moveCapture.RUnlock()
		if s != nil && tenant == string(s.service.Tenant) && service == string(s.service.Service) {
			s.post(func() { s.recordMoveLog(fields) })
		}
	}
	l.Logger.Log(ctx, severity, calldepth+1, message)
}

func (s *simulation) captureMoveLogs() func() {
	moveCapture.Lock()
	moveCapture.simulation = s
	moveCapture.Unlock()
	return func() {
		moveCapture.Lock()
		moveCapture.simulation = nil
		moveCapture.Unlock()
	}
}

type moveLog struct {
	source, destination string
	from, to            model.GrantID
	shard               model.Shard
	diff                BalanceDiff
}

// parseMoveLog consumes the existing structured fields, not formatted log lines.
// Only load is textual: AdjustedLoad is emitted with log.Stringer in production.
func parseMoveLog(service model.QualifiedServiceName, fields []log.Field) (moveLog, error) {
	values := make(map[string]string)
	for _, field := range fields {
		if field.Type == log.StringType {
			values[field.Key] = field.String
		} else if field.Key == "load" && field.Type == log.StringerType {
			values[field.Key] = field.Interface.(fmt.Stringer).String()
		}
	}
	for _, key := range []string{"event_source", "domain", "shard_region", "shard_from", "shard_to", "from_worker", "to_worker", "from_grant", "to_grant", "load"} {
		value, present := values[key]
		if !present || (value == "" && key != "shard_region") {
			return moveLog{}, fmt.Errorf("move log missing %s", key)
		}
	}
	if values["event_source"] != "coordinator" {
		return moveLog{}, fmt.Errorf("unexpected move log source %q", values["event_source"])
	}
	from, err := model.ParseKey(values["shard_from"])
	if err != nil {
		return moveLog{}, fmt.Errorf("move log shard_from: %w", err)
	}
	to, err := model.ParseKey(values["shard_to"])
	if err != nil {
		return moveLog{}, fmt.Errorf("move log shard_to: %w", err)
	}
	var diff BalanceDiff
	var total int64
	if n, err := fmt.Sscanf(values["load"], "%d[load=%d, place=%d, colo=%d]", &total, &diff.Load, &diff.Place, &diff.Colo); err != nil || n != 4 {
		return moveLog{}, fmt.Errorf("invalid move log load %q", values["load"])
	}
	if total != diff.Load+diff.Place+diff.Colo {
		return moveLog{}, fmt.Errorf("inconsistent move log load %q", values["load"])
	}
	shardType := model.Global
	if values["shard_region"] != "" {
		shardType = model.Regional
	}
	return moveLog{
		source: values["from_worker"], destination: values["to_worker"],
		from: model.GrantID(values["from_grant"]), to: model.GrantID(values["to_grant"]), diff: diff,
		shard: model.Shard{Type: shardType, Domain: model.QualifiedDomainName{Service: service, Domain: model.DomainName(values["domain"])}, Region: model.Region(values["shard_region"]), From: from, To: to},
	}, nil
}

func (s *simulation) recordMoveLog(fields []log.Field) {
	event, err := parseMoveLog(s.service, fields)
	if err != nil {
		s.t.Fatalf("capture coordinator move: %v", err)
	}
	// Transfer accounting needs only grant IDs and the shard, not lease metadata.
	from := model.NewGrant(event.from, event.shard, model.RevokedGrantState, time.Time{}, time.Time{})
	to := model.NewGrant(event.to, event.shard, model.AllocatedGrantState, time.Time{}, time.Time{})
	s.balance(from, to, event.source, event.destination)
	transfer := s.transfers[event.to]
	// The summary log follows the handoff sends. Preserve an outcome already
	// recorded from Assign, including when the same log is delivered twice.
	transfer.Reason = "balance"
	transfer.BalanceDiff = &event.diff
}
