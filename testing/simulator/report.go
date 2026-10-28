package simulator

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"slices"
	"sort"
	"strconv"
	"time"

	"go.atoms.co/splitter/pkg/model"
)

// BalanceDiff is the allocator decision delta, matching the production move log.
// Load-only Path A has negative Load; Path B has zero Load. Both have zero
// placement and colocation deltas. A zero Load does not mean no work moved.
type BalanceDiff struct {
	Load  int64 `json:"load"`
	Place int64 `json:"place"`
	Colo  int64 `json:"colo"`
}

// Transfer correlates a new destination grant with its source. Same-consumer
// reacquisitions are retained in the trace but excluded from movement totals.
type Transfer struct {
	BalanceDiff      *BalanceDiff `json:"balance_diff,omitempty"`
	FromRegion       string       `json:"from_region"`
	ToRegion         string       `json:"to_region"`
	At               Duration     `json:"at"`
	Phase            string       `json:"phase"`
	Reason           string       `json:"reason"`
	Shard            string       `json:"shard"`
	From             string       `json:"from"`
	To               string       `json:"to"`
	SourceGrant      string       `json:"source_grant"`
	DestinationGrant string       `json:"destination_grant"`
	Status           string       `json:"status"`
	CompletedAt      *Duration    `json:"completed_at,omitempty"`
}

type LifecycleEvent struct {
	Region   string   `json:"region"`
	Shard    string   `json:"shard,omitempty"`
	Grant    string   `json:"grant,omitempty"`
	At       Duration `json:"at"`
	Kind     string   `json:"kind"`
	Slot     int      `json:"slot"`
	Consumer string   `json:"consumer,omitempty"`
	Outcome  string   `json:"outcome"`
}

type ShardLoad struct {
	// Index identifies a synthetic shard independently of its native domain/range.
	// Preference groups are an adapter to the coordinator's native region rules.
	Index            int      `json:"index"`
	PreferredRegions []string `json:"preferred_regions"`
	Shard            string   `json:"shard"`
	Load             int      `json:"load"`
}

type ConsumerSample struct {
	Region      string `json:"region"`
	OutOfRegion int    `json:"out_of_region"`
	ActiveLoad  int    `json:"active_load"`
	ServingLoad int    `json:"serving_load"`
	Consumer    string `json:"consumer"`
	Connected   bool   `json:"connected"`
	Active      int    `json:"active"`
	Allocated   int    `json:"allocated"`
	Revoked     int    `json:"revoked"`
	Serving     int    `json:"serving"`
}

type Sample struct {
	OutOfRegion int              `json:"out_of_region"`
	At          Duration         `json:"at"`
	Phase       string           `json:"phase"`
	Unserved    int              `json:"unserved"`
	Consumers   []ConsumerSample `json:"consumers"`
}

type MovementTotals struct {
	CrossRegionMoves int `json:"cross_region_moves"`
	Moves            int `json:"moves"`
	Completed        int `json:"completed"`
	Interrupted      int `json:"interrupted"`
	Pending          int `json:"pending"`
}

type Summary struct {
	FinalOutOfRegion int `json:"final_out_of_region"`
	// Keys are phase/reason, e.g. churn/balance. Startup is not mixed into churn.
	Movements                  map[string]MovementTotals `json:"movements"`
	SameConsumerReacquisitions int                       `json:"same_consumer_reacquisitions"`
	EventsExecuted             map[string]int            `json:"events_executed"`
	EventsSkipped              map[string]int            `json:"events_skipped"`
	FinalUnserved              int                       `json:"final_unserved"`
}

type Report struct {
	ShardLoads []ShardLoad      `json:"shard_loads"`
	Config     Config           `json:"config"`
	Summary    Summary          `json:"summary"`
	Events     []LifecycleEvent `json:"events"`
	Transfers  []*Transfer      `json:"transfers"`
	Samples    []Sample         `json:"samples"`
}

func (s *simulation) balance(from, to model.Grant, source, destination string) {
	s.newTransfer(from, to, source, destination, "balance")
}

func (s *simulation) newTransfer(from, to model.Grant, source, destination, reason string) {
	if _, exists := s.transfers[to.ID()]; exists {
		return
	}
	t := &Transfer{FromRegion: s.consumerRegions[source], ToRegion: s.consumerRegions[destination], At: s.elapsed(), Phase: s.phase(), Reason: reason, Shard: shardName(to.Shard()), From: source, To: destination,
		SourceGrant: string(from.ID()), DestinationGrant: string(to.ID()), Status: "pending"}
	if s.retired[destination] {
		t.Status = "interrupted"
		s.causes[to.ID()] = "quit"
	}
	s.transfers[to.ID()] = t
	s.report.Transfers = append(s.report.Transfers, t)
}

func (s *simulation) assigned(c *consumer, g model.Grant) {
	if transfer := s.transfers[g.ID()]; transfer != nil && transfer.Status == "expired" && transfer.To == string(c.instance.ID()) && !s.retired[transfer.To] {
		// Reattachment may restore the same grant before coordinator cleanup.
		// Keep one initiated move, but let successful promotion complete it.
		transfer.Status = "pending"
	}
	if s.seen[g.ID()] {
		return
	}
	s.seen[g.ID()] = true
	if _, exists := s.transfers[g.ID()]; exists {
		return
	}
	previous, exists := s.owners[g.Shard()]
	if !exists {
		return
	}
	reason := s.causes[previous.grant]
	if reason == "" {
		reason = "reassignment"
	}
	s.newTransfer(model.NewGrant(previous.grant, g.Shard(), model.ActiveGrantState, g.Lease(), g.Assigned()), g, previous.consumer, string(c.instance.ID()), reason)
}

func (s *simulation) sample() {
	sample := Sample{At: s.elapsed(), Phase: s.phase(), Consumers: make([]ConsumerSample, 0, len(s.consumers))}
	served := map[model.Shard]bool{}
	for _, c := range s.consumers {
		row := ConsumerSample{Region: c.region, Consumer: string(c.instance.ID()), Connected: c.connected}
		for _, g := range c.grants {
			if !time.Now().Before(g.lease) {
				continue
			}
			switch {
			case g.state == model.ActiveGrantState:
				row.Active++
				row.ActiveLoad += s.shardLoads[g.assignment.Shard()]
			case model.IsAllocatedOrLoaded(g.state):
				row.Allocated++
			case model.IsRevokedOrUnloaded(g.state):
				row.Revoked++
			}
			if g.serving {
				shard := g.assignment.Shard()
				if served[shard] {
					s.t.Fatalf("shard %s is serving on multiple consumers", shard)
				}
				served[shard] = true
				row.Serving++
				prefs := s.shardPreferences[shard]
				if len(prefs) > 0 && !slices.Contains(prefs, c.region) {
					row.OutOfRegion++
					sample.OutOfRegion++
				}
				row.ServingLoad += s.shardLoads[shard]
			}
		}
		sample.Consumers = append(sample.Consumers, row)
	}
	sample.Unserved = s.cfg.Shards - len(served)
	s.report.Samples = append(s.report.Samples, sample)
}

func (s *simulation) finish() {
	summary := Summary{Movements: map[string]MovementTotals{}, EventsExecuted: map[string]int{}, EventsSkipped: map[string]int{}}
	for _, t := range s.report.Transfers {
		if t.From == t.To {
			summary.SameConsumerReacquisitions++
			continue
		}
		key := t.Phase + "/" + t.Reason
		total := summary.Movements[key]
		total.Moves++
		if t.FromRegion != t.ToRegion {
			total.CrossRegionMoves++
		}
		switch t.Status {
		case "completed":
			total.Completed++
		case "pending":
			total.Pending++
		default:
			total.Interrupted++
		}
		summary.Movements[key] = total
	}
	for _, e := range s.report.Events {
		if e.Outcome == "executed" {
			summary.EventsExecuted[e.Kind]++
		} else {
			summary.EventsSkipped[e.Kind]++
		}
	}
	summary.FinalOutOfRegion = s.report.Samples[len(s.report.Samples)-1].OutOfRegion
	summary.FinalUnserved = s.report.Samples[len(s.report.Samples)-1].Unserved
	s.report.Summary = summary
}

func (r Report) WriteJSON(w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(r)
}

// WriteCSV emits one row per consumer per sample. Serving includes a revoked
// source until handoff or lease expiry; an allocated destination is not active.
func (r Report) WriteCSV(w io.Writer) error {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"seconds", "phase", "consumer", "connected", "active", "allocated", "revoked", "serving", "unserved", "active_load", "serving_load", "region", "out_of_region"}); err != nil {
		return err
	}
	for _, sample := range r.Samples {
		for _, c := range sample.Consumers {
			if err := cw.Write([]string{strconv.FormatFloat(time.Duration(sample.At).Seconds(), 'f', 3, 64), sample.Phase, c.Consumer, strconv.FormatBool(c.Connected), strconv.Itoa(c.Active), strconv.Itoa(c.Allocated), strconv.Itoa(c.Revoked), strconv.Itoa(c.Serving), strconv.Itoa(sample.Unserved), strconv.Itoa(c.ActiveLoad), strconv.Itoa(c.ServingLoad), c.Region, strconv.Itoa(c.OutOfRegion)}); err != nil {
				return err
			}
		}
	}
	cw.Flush()
	return cw.Error()
}

func (r Report) WriteSummary(w io.Writer) error {
	if _, err := fmt.Fprintln(w, "phase/reason                 moves completed interrupted pending cross-region"); err != nil {
		return err
	}
	keys := make([]string, 0, len(r.Summary.Movements))
	for key := range r.Summary.Movements {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		v := r.Summary.Movements[key]
		if _, err := fmt.Fprintf(w, "%-23s %9d %9d %11d %7d %12d\n", key, v.Moves, v.Completed, v.Interrupted, v.Pending, v.CrossRegionMoves); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, "Same-consumer reacquisitions: %d; final unserved shards: %d; final outside preferred regions: %d\n", r.Summary.SameConsumerReacquisitions, r.Summary.FinalUnserved, r.Summary.FinalOutOfRegion)
	return err
}

// WriteHTML generates a standalone report with no remote assets or server.
func (r Report) WriteHTML(w io.Writer) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return reportTemplate.Execute(w, template.JS(data)) // json.Marshal escapes HTML-sensitive characters.
}

var reportTemplate = template.Must(template.New("report").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><title>Splitter balancing simulation</title>
<style>body{font:15px system-ui;margin:32px;color:#172238}canvas{width:100%;height:360px;border:1px solid #ddd}table{border-collapse:collapse}td,th{padding:8px;text-align:right;border-bottom:1px solid #ddd}td:first-child,th:first-child{text-align:left}pre{white-space:pre-wrap}</style>
<h1>Splitter balancing simulation</h1><p>Actual coordinator, virtual time, synthetic consumers with regional placement preferences. Select serving shard counts or total load per consumer.</p>
<label>Chart: <select id="metric"><option value="serving">Serving shards</option><option value="serving_load">Serving load</option><option value="out_of_region">Shards outside preferred regions</option></select></label>
<canvas id="chart"></canvas><p id="position"></p><input id="time" type="range" min="0" style="width:100%"><pre id="snapshot"></pre>
<h2>Regional placement</h2><p>Regions are soft preferences: cross-region placement is allowed. Counts below track serving shards outside every preferred region; shards without a preference always match.</p><table><thead><tr><th>Region</th><th>Connected consumers</th><th>Serving shards</th><th>Serving load</th><th>Outside preferred regions</th></tr></thead><tbody id="regions"></tbody></table>
<h2>Movement counts</h2><p>Moves counts initiated transfers between consumers. Moving the same shard twice counts as two moves.</p><table><thead><tr><th>Phase / reason</th><th>Moves</th><th>Completed</th><th>Interrupted</th><th>Pending</th><th>Cross-region moves</th></tr></thead><tbody id="totals"></tbody></table>
<h2>Lifecycle events</h2><table><thead><tr><th>Event</th><th>Executed</th><th>Skipped</th></tr></thead><tbody id="events"></tbody></table>
<p id="final"></p><details><summary>Shard loads and region preferences</summary><pre id="shards"></pre></details><details><summary>Configuration</summary><pre id="config"></pre></details>
<script>const report={{.}};const samples=report.samples;const slider=document.getElementById('time');slider.max=samples.length-1;
const canvas=document.getElementById('chart');const metric=document.getElementById('metric');
function draw(){const series=new Map();let maximum=1;for(const [i,s]of samples.entries())for(const c of s.consumers){if(!series.has(c.consumer))series.set(c.consumer,Array(samples.length).fill(0));const n=c[metric.value];series.get(c.consumer)[i]=n;maximum=Math.max(maximum,n)}canvas.width=canvas.clientWidth*devicePixelRatio;canvas.height=360*devicePixelRatio;const ctx=canvas.getContext('2d');ctx.scale(devicePixelRatio,devicePixelRatio);const w=canvas.clientWidth-64,h=300;const max=maximum;ctx.font='12px system-ui';ctx.fillText(max+(metric.value==='serving_load'?' load':' shards'),8,20);ctx.fillText('0',30,330);ctx.fillText('simulated time →',w/2,350);let i=0;for(const values of series.values()){ctx.strokeStyle='hsl('+i++*137.5+' 65% 45%)';ctx.beginPath();values.forEach((n,j)=>{const x=48+j*w/Math.max(1,samples.length-1),y=24+h-n*h/max;j?ctx.lineTo(x,y):ctx.moveTo(x,y)});ctx.stroke()}}
function inspect(){const s=samples[slider.value];document.getElementById('position').textContent=s.at+' · '+s.phase+' · '+s.unserved+' unserved · '+s.out_of_region+' outside preferred regions';document.getElementById('snapshot').textContent=s.consumers.map(c=>c.consumer+' ['+c.region+']: '+c.serving+' serving (load '+c.serving_load+'), '+c.allocated+' allocated, '+c.revoked+' revoked, '+c.out_of_region+' outside preferred regions'+(c.connected?'':' (disconnected)')).join('\n');const regions=new Map();for(const c of s.consumers){if(!regions.has(c.region))regions.set(c.region,[0,0,0,0]);const v=regions.get(c.region);v[0]+=Number(c.connected);v[1]+=c.serving;v[2]+=c.serving_load;v[3]+=c.out_of_region}const body=document.getElementById('regions');body.textContent='';for(const [region,v]of [...regions].sort()){const tr=document.createElement('tr');for(const value of [region,...v]){const td=document.createElement('td');td.textContent=value;tr.append(td)}body.append(tr)}}slider.oninput=inspect;
for(const [key,v]of Object.entries(report.summary.movements).sort()){const tr=document.createElement('tr');for(const value of[key,v.moves,v.completed,v.interrupted,v.pending,v.cross_region_moves]){const td=document.createElement('td');td.textContent=value;tr.append(td)}document.getElementById('totals').append(tr)}
for(const kind of [...new Set([...Object.keys(report.summary.events_executed),...Object.keys(report.summary.events_skipped)])].sort()){const tr=document.createElement('tr');for(const value of[kind,report.summary.events_executed[kind]||0,report.summary.events_skipped[kind]||0]){const td=document.createElement('td');td.textContent=value;tr.append(td)}document.getElementById('events').append(tr)}
document.getElementById('shards').textContent=report.shard_loads.map(s=>'shard-'+s.index+' · '+s.shard+': load '+s.load+'; preferred regions: '+(s.preferred_regions.length?s.preferred_regions.join(', '):'any')).join('\n');document.getElementById('config').textContent=JSON.stringify(report.config,null,2);document.getElementById('final').textContent='Final unserved shards: '+report.summary.final_unserved+'; same-consumer reacquisitions: '+report.summary.same_consumer_reacquisitions+'; final outside preferred regions: '+report.summary.final_out_of_region;metric.onchange=draw;window.onresize=draw;draw();inspect();</script></html>`))
