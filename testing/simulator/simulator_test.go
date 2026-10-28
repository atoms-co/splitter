package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	stdlog "log"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"go.atoms.co/lib/log"

	"go.atoms.co/splitter/pkg/model"
)

var scenarioJSON = flag.String("scenario", "", "JSON object overriding DefaultConfig for TestScenario")

var coordinatorLogs = flag.Bool("coordinator_logs", false, "include verbose coordinator protocol logs")

func TestMain(m *testing.M) {
	log.SetLogger(moveLogger{Logger: &log.Standard{}})
	flag.Parse()
	if !*coordinatorLogs {
		stdlog.SetOutput(io.Discard)
	}
	os.Exit(m.Run())
}

// TestScenario is also the user-facing simulator runner. All IO happens outside
// virtual time; Bazel retains reports in its undeclared test outputs directory.
func TestScenario(t *testing.T) {
	cfg := DefaultConfig()
	if *scenarioJSON != "" {
		var err error
		cfg, err = decodeScenario(*scenarioJSON)
		if err != nil {
			t.Fatalf("decode scenario: %v", err)
		}
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("invalid scenario: %v", err)
	}
	start := time.Now()
	report := Run(t, cfg)
	var summary bytes.Buffer
	if err := report.WriteSummary(&summary); err != nil {
		t.Fatal(err)
	}
	t.Logf("Simulated %s in %s\n%s", time.Duration(cfg.Warmup+cfg.Duration), time.Since(start), summary.String())
	dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	for _, output := range []struct {
		name  string
		write func(*os.File) error
	}{
		{"report.json", func(f *os.File) error { return report.WriteJSON(f) }},
		{"samples.csv", func(f *os.File) error { return report.WriteCSV(f) }},
		{"report.html", func(f *os.File) error { return report.WriteHTML(f) }},
	} {
		path := filepath.Join(dir, output.name)
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		writeErr := output.write(f)
		closeErr := f.Close()
		if writeErr != nil {
			t.Fatal(writeErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	t.Logf("Reports: %s", dir)
}

func decodeScenario(input string) (Config, error) {
	cfg := DefaultConfig()
	decoder := json.NewDecoder(strings.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return cfg, fmt.Errorf("scenario must contain exactly one JSON object")
	}
	return cfg, nil
}

func TestDurationJSON(t *testing.T) {
	for _, value := range []time.Duration{0, 5 * time.Second, 90 * time.Minute, -time.Second, 1500 * time.Microsecond} {
		t.Run(value.String(), func(t *testing.T) {
			encoded, err := json.Marshal(Duration(value))
			if err != nil {
				t.Fatal(err)
			}
			if string(encoded) != fmt.Sprintf("%q", value.String()) {
				t.Fatalf("duration encoded as %s, want a duration string", encoded)
			}
			var decoded Duration
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			if time.Duration(decoded) != value {
				t.Fatalf("duration round trip got %v, want %v", time.Duration(decoded), value)
			}
		})
	}
	for _, input := range []string{`"invalid"`, `"5"`, `""`, `5`, `null`, `true`, `{}`} {
		t.Run(input, func(t *testing.T) {
			value := Duration(17 * time.Second)
			if err := json.Unmarshal([]byte(input), &value); err == nil {
				t.Fatalf("invalid duration %s accepted", input)
			}
			if value != Duration(17*time.Second) {
				t.Fatal("failed decode changed the duration")
			}
		})
	}
}

func TestScenarioPresetsDurationJSON(t *testing.T) {
	presets, err := filepath.Glob("scenarios/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(presets) == 0 {
		t.Fatal("checked-in scenarios are missing from test data")
	}
	for _, path := range presets {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := decodeScenario(string(data))
			if err != nil {
				t.Fatal(err)
			}
			if err := cfg.validate(); err != nil {
				t.Fatal(err)
			}
			if cfg.Warmup != Duration(5*time.Minute) || cfg.Duration != Duration(12*time.Hour) || cfg.ReconnectDelay != Duration(5*time.Second) {
				t.Fatalf("preset duration values were not decoded correctly: %+v", cfg)
			}
			completedAt := Duration(4 * time.Second)
			report := Report{Config: cfg, Samples: []Sample{{At: Duration(1500 * time.Millisecond)}},
				Events:    []LifecycleEvent{{At: Duration(2 * time.Second)}},
				Transfers: []*Transfer{{At: Duration(3 * time.Second), CompletedAt: &completedAt}}}
			var output bytes.Buffer
			if err := report.WriteJSON(&output); err != nil {
				t.Fatal(err)
			}
			var decoded Report
			if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(report, decoded) {
				t.Fatalf("report changed during JSON round trip: %s", output.Bytes())
			}
		})
	}
}

// testConfig keeps deterministic fault injection out of the scenario API.
type testConfig struct {
	Config
	actions []testAction
}

type testAction struct {
	At              Duration
	Kind            string
	Consumer, Count int
	Region          string
}

func runTest(t *testing.T, cfg testConfig) Report {
	return run(t, cfg.Config, func(s *simulation) {
		for i := 0; i < len(cfg.actions); {
			j := i + 1
			for j < len(cfg.actions) && cfg.actions[j].At == cfg.actions[i].At {
				j++
			}
			group := cfg.actions[i:j]
			s.after(time.Duration(cfg.Warmup+group[0].At), func() {
				for _, a := range group {
					switch a.Kind {
					case "add":
						for range a.Count {
							s.addConsumer(defaultRegion(a.Region))
						}
					case "sample":
					default:
						s.execute(a.Kind, a.Consumer)
					}
				}
				s.settle()
				s.sample()
			})
			i = j
		}
	})
}

func smallConfig() testConfig {
	c := testConfig{Config: DefaultConfig()}
	c.Consumers = 2
	c.Shards = 8
	c.Warmup = Duration(2 * time.Minute)
	c.Duration = Duration(3 * time.Minute)
	return c
}

func TestLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*testConfig)
		check     func(*testing.T, Report)
	}{
		{"steady", func(c *testConfig) {}, func(t *testing.T, r Report) {
			if v := r.Summary.Movements["churn/balance"]; v.Moves != 0 {
				t.Fatalf("settled consumers moved: %+v", v)
			}
		}},
		{"scale_out", func(c *testConfig) { c.Consumers = 1; c.actions = []testAction{{Kind: "add", Count: 1}} }, func(t *testing.T, r Report) {
			if v := r.Summary.Movements["churn/balance"]; v.Completed == 0 {
				t.Fatalf("scale-out did not complete balancing: %+v", r.Summary)
			}
			last := r.Samples[len(r.Samples)-1]
			if len(last.Consumers) != 2 || last.Consumers[0].Serving != 4 || last.Consumers[1].Serving != 4 {
				t.Fatalf("unexpected final distribution: %+v", last)
			}
		}},
		{"drain", func(c *testConfig) { c.actions = []testAction{{Kind: "drain", Consumer: 0}} }, func(t *testing.T, r Report) {
			v := r.Summary.Movements["churn/drain"]
			if v.Moves != 1 || v.Completed != 1 {
				t.Fatalf("drain must hand off exactly one shard: %+v", r.Summary)
			}
		}},
		{"quick_reconnect", func(c *testConfig) { c.Consumers = 1; c.actions = []testAction{{Kind: "reconnect"}} }, func(t *testing.T, r Report) {
			if len(r.Transfers) != 0 {
				t.Fatalf("quick reconnect moved/recreated grants: %+v", r.Transfers)
			}
		}},
		{"expired_reconnect", func(c *testConfig) {
			c.Consumers = 1
			c.ReconnectDelay = Duration(time.Minute)
			c.actions = []testAction{{Kind: "reconnect"}, {At: Duration(45 * time.Second), Kind: "sample"}}
		}, func(t *testing.T, r Report) {
			if r.Summary.SameConsumerReacquisitions == 0 {
				t.Fatal("long disconnect did not expire and recreate grants")
			}
			unserved := false
			for _, sample := range r.Samples {
				if sample.Phase != "warmup" && sample.Unserved > 0 {
					unserved = true
				}
			}
			if !unserved {
				t.Fatal("missing lease-expiry serving gap")
			}
		}},
		{"quit", func(c *testConfig) { c.actions = []testAction{{Kind: "quit"}} }, func(t *testing.T, r Report) {
			var quitAt, replacementAt Duration
			for _, event := range r.Events {
				if event.Kind == "quit" {
					quitAt = event.At
				}
				if event.Kind == "quit_complete" {
					replacementAt = event.At
				}
			}
			if replacementAt-quitAt != Duration(5*time.Second) {
				t.Fatalf("replacement must connect five seconds after quit: %s", time.Duration(replacementAt-quitAt))
			}
			if r.Summary.Movements["churn/quit"].Completed == 0 {
				t.Fatalf("quit did not cause recovery: %+v", r.Summary)
			}
		}},
		{"simultaneous_add_and_quit", func(c *testConfig) {
			c.actions = []testAction{{Kind: "add", Count: 1}, {Kind: "quit", Consumer: 2}}
		}, func(t *testing.T, r Report) {
			if r.Summary.EventsExecuted["quit"] != 1 {
				t.Fatal("simultaneous events did not run in input order")
			}
		}},
		{"all_consumers_quit", func(c *testConfig) {
			c.actions = []testAction{{Kind: "quit", Consumer: 0}, {Kind: "quit", Consumer: 1}}
		}, func(t *testing.T, r Report) {
			if r.Summary.EventsExecuted["quit"] != 2 {
				t.Fatal("expected both consumers to quit")
			}
		}},
		{"random_churn", func(c *testConfig) {
			c.DrainRate = 3
			c.ReconnectRate = 2
			c.QuitRate = 1
			c.RelinquishRate = 1
			c.Duration = Duration(3 * time.Minute)
		}, func(t *testing.T, r Report) {
			for _, kind := range []string{"drain", "reconnect", "quit", "relinquish"} {
				if r.Summary.EventsExecuted[kind] == 0 {
					t.Fatalf("no %s generated", kind)
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := smallConfig()
			tc.configure(&cfg)
			r := runTest(t, cfg)
			if tc.name != "random_churn" && r.Summary.FinalUnserved != 0 {
				t.Fatalf("simulation left %d unserved shards: %+v", r.Summary.FinalUnserved, r.Summary)
			}
			tc.check(t, r)
			// Every initiated cross-consumer transfer has exactly one outcome.
			for reason, v := range r.Summary.Movements {
				if v.Moves != v.Completed+v.Interrupted+v.Pending {
					t.Fatalf("unaccounted transfers %s: %+v", reason, v)
				}
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testConfig)
	}{
		{"load_type", func(c *testConfig) { c.ShardLoadType = "unknown" }},
		{"consumers", func(c *testConfig) { c.Consumers = 0 }},
		{"region_count", func(c *testConfig) { c.ConsumerRegions = []ConsumerRegion{{Region: "westus", Consumers: 1}} }},
		{"duplicate_consumer_region", func(c *testConfig) {
			c.ConsumerRegions = []ConsumerRegion{{Region: "westus", Consumers: 8}, {Region: "westus", Consumers: 8}}
		}},
		{"blank_consumer_region", func(c *testConfig) { c.ConsumerRegions = []ConsumerRegion{{Region: " ", Consumers: c.Consumers}} }},
		{"negative_region_count", func(c *testConfig) { c.ConsumerRegions = []ConsumerRegion{{Region: "westus", Consumers: -1}} }},
		{"shard_group_count", func(c *testConfig) { c.ShardRegionPreferences = []ShardRegionPreference{{Shards: 1, Regions: nil}} }},
		{"negative_shard_group_count", func(c *testConfig) { c.ShardRegionPreferences = []ShardRegionPreference{{Shards: -1, Regions: nil}} }},
		{"blank_shard_region", func(c *testConfig) {
			c.ShardRegionPreferences = []ShardRegionPreference{{Shards: c.Shards, Regions: []string{""}}}
		}},
		{"duplicate_shard_region", func(c *testConfig) {
			c.ShardRegionPreferences = []ShardRegionPreference{{Shards: c.Shards, Regions: []string{"eastus", "eastus"}}}
		}},
		{"negative_rate", func(c *testConfig) { c.QuitRate = -1 }},
		{"negative_relinquish_rate", func(c *testConfig) { c.RelinquishRate = -1 }},
		{"negative_delay", func(c *testConfig) { c.ReconnectDelay = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig{Config: DefaultConfig()}
			tc.change(&c)
			if err := c.validate(); err == nil {
				t.Fatal("invalid scenario accepted")
			}
		})
	}
}

func TestShardLoadTypes(t *testing.T) {
	for _, tc := range []struct {
		strategy ShardLoadType
		shards   int
		want     []int
	}{
		{Fixed, 5, []int{50, 50, 50, 50, 50}},
		{EvenDistribution, 1, []int{50}},
		{EvenDistribution, 2, []int{1, 99}},
		{EvenDistribution, 5, []int{1, 26, 50, 75, 99}},
		{EvenDistribution, 99, nil},
		{EvenDistribution, 128, nil},
	} {
		t.Run(fmt.Sprintf("%s/%d", tc.strategy, tc.shards), func(t *testing.T) {
			cfg := smallConfig()
			cfg.Consumers = 1
			cfg.Shards = tc.shards
			cfg.ShardLoadType = tc.strategy
			r := runTest(t, cfg)
			if len(r.ShardLoads) != tc.shards {
				t.Fatalf("got %d shard loads, want %d", len(r.ShardLoads), tc.shards)
			}
			total := 0
			scores := make([]int, 0, len(r.ShardLoads))
			for _, shard := range r.ShardLoads {
				if shard.Load < 1 || shard.Load > 99 {
					t.Fatalf("out-of-range load: %+v", shard)
				}
				scores = append(scores, shard.Load)
				total += shard.Load
			}
			slices.Sort(scores)
			if tc.want != nil && !slices.Equal(scores, tc.want) {
				t.Fatalf("incorrect score multiset: %v, want %v", scores, tc.want)
			}
			if tc.shards == 99 {
				for i, score := range scores {
					if score != i+1 {
						t.Fatalf("missing score %d", i+1)
					}
				}
			}
			last := r.Samples[len(r.Samples)-1]
			if last.Unserved != 0 || last.Consumers[0].ServingLoad != total || last.Consumers[0].ActiveLoad != total {
				t.Fatalf("incorrect final load totals: %+v, want %d", last, total)
			}
		})
	}
}

func TestEvenDistributionBalancesByLoad(t *testing.T) {
	cfg := smallConfig()
	cfg.Consumers = 1
	cfg.Shards = 3
	cfg.ShardLoadType = EvenDistribution
	cfg.actions = []testAction{{Kind: "add", Count: 1}}
	r := runTest(t, cfg)
	// With loads 1, 50, and 99 initially on one consumer, the coordinator must
	// move the 99-load shard to the new consumer and leave 51 on the source.
	var first *Transfer
	for _, transfer := range r.Transfers {
		if transfer.Reason == "balance" {
			first = transfer
			break
		}
	}
	heaviest := ""
	for _, shard := range r.ShardLoads {
		if shard.Load == 99 {
			heaviest = shard.Shard
		}
	}
	if first == nil || first.Shard != heaviest || first.Status != "completed" {
		t.Fatalf("expected a completed move of the heaviest shard, got %+v", first)
	}
	last := r.Samples[len(r.Samples)-1]
	if last.Unserved != 0 || last.Consumers[0].ServingLoad != 51 || last.Consumers[1].ServingLoad != 99 {
		t.Fatalf("coordinator did not balance the configured weights: %+v", last)
	}
}

func TestRelinquish(t *testing.T) {
	cfg := smallConfig()
	cfg.Consumers = 1
	cfg.Shards = 4
	// Simultaneous attempts must release different grants; the consumer's
	// remaining two shards continue serving until the released shards return.
	cfg.actions = []testAction{{Kind: "relinquish"}, {Kind: "relinquish"}}
	r := runTest(t, cfg)
	var grants []string
	for _, event := range r.Events {
		if event.Kind == "relinquish" && event.Outcome == "executed" {
			if event.Shard == "" || event.Grant == "" {
				t.Fatal("relinquish event missing selected shard/grant")
			}
			grants = append(grants, event.Grant)
		}
	}
	if len(grants) != 2 || grants[0] == grants[1] {
		t.Fatalf("expected two distinct relinquished grants, got %v", grants)
	}
	gap := false
	for _, sample := range r.Samples {
		if !sample.Consumers[0].Connected {
			t.Fatal("relinquish disconnected the consumer")
		}
		if sample.Phase == "churn" && sample.Unserved == 2 && sample.Consumers[0].Serving == 2 {
			gap = true
		}
	}
	if !gap {
		t.Fatal("relinquish did not stop the selected shards while keeping the others")
	}
	if r.Summary.FinalUnserved != 0 || r.Summary.SameConsumerReacquisitions != 2 {
		t.Fatalf("released shards did not recover through the real coordinator: %+v", r.Summary)
	}
	if r.Summary.EventsExecuted["quit"] != 0 || r.Summary.EventsExecuted["reconnect"] != 0 {
		t.Fatalf("relinquish triggered a consumer lifecycle change: %+v", r.Summary)
	}
}

func TestRelinquishWithoutServingShards(t *testing.T) {
	cfg := smallConfig()
	cfg.Consumers, cfg.Shards = 1, 1
	cfg.actions = []testAction{{Kind: "relinquish"}, {Kind: "relinquish"}}
	r := runTest(t, cfg)
	if r.Summary.EventsSkipped["relinquish"] != 1 || r.Summary.EventsExecuted["relinquish"] != 1 {
		t.Fatalf("expected second relinquish to skip without a serving shard: %+v", r.Summary)
	}
	if r.Summary.FinalUnserved != 0 {
		t.Fatal("startup did not recover")
	}
}

func TestRelinquishTransferAttribution(t *testing.T) {
	cfg := smallConfig()
	// Relinquish opens a hole on consumer 0. Draining a shard from consumer 1
	// fills it, so the relinquished shard must eventually move to consumer 1.
	cfg.actions = []testAction{{Kind: "relinquish", Consumer: 0}, {Kind: "drain", Consumer: 1}}
	r := runTest(t, cfg)
	moves := r.Summary.Movements["churn/relinquish"]
	if moves.Moves != 1 || moves.Completed != 1 {
		t.Fatalf("missing relinquish transfer attribution: %+v", r.Summary)
	}
	if r.Summary.FinalUnserved != 0 {
		t.Fatal("relinquished shard did not recover")
	}
}

func TestRegionalPlacement(t *testing.T) {
	for _, tc := range []struct {
		name      string
		consumers []ConsumerRegion
		shards    []ShardRegionPreference
		strategy  ShardLoadType
		outside   int
	}{
		{"local", []ConsumerRegion{{Region: "centralus", Consumers: 2}, {Region: "eastus", Consumers: 2}}, []ShardRegionPreference{{Shards: 4, Regions: []string{"centralus"}}, {Shards: 4, Regions: []string{"eastus"}}}, Fixed, 0},
		{"weighted", []ConsumerRegion{{Region: "centralus", Consumers: 2}, {Region: "eastus", Consumers: 2}}, []ShardRegionPreference{{Shards: 4, Regions: []string{"centralus", "eastus"}}, {Shards: 4, Regions: []string{"eastus", "centralus"}}}, EvenDistribution, 0},
		{"fallback", []ConsumerRegion{{Region: "westus", Consumers: 4}}, []ShardRegionPreference{{Shards: 8, Regions: []string{"eastus"}}}, Fixed, 8},
		{"multiple_preferences", []ConsumerRegion{{Region: "westus", Consumers: 4}}, []ShardRegionPreference{{Shards: 8, Regions: []string{"centralus", "westus"}}}, Fixed, 0},
		{"any_region", []ConsumerRegion{{Region: "westus", Consumers: 4}}, []ShardRegionPreference{{Shards: 8, Regions: nil}}, EvenDistribution, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := smallConfig()
			cfg.Consumers = 4
			cfg.ConsumerRegions, cfg.ShardRegionPreferences, cfg.ShardLoadType = tc.consumers, tc.shards, tc.strategy
			r := runTest(t, cfg)
			if len(r.ShardLoads) != cfg.Shards || r.Summary.FinalUnserved != 0 || r.Summary.FinalOutOfRegion != tc.outside {
				t.Fatalf("incorrect shard count or region placement: loads=%d summary=%+v", len(r.ShardLoads), r.Summary)
			}
			totalLoad := 0
			gotScores, wantScores := map[int]int{}, map[int]int{}
			for i, shard := range r.ShardLoads {
				gotScores[shard.Load]++
				wantScores[cfg.ShardLoadType.loadForShard(i, cfg.Shards)]++
				totalLoad += shard.Load
			}
			if !reflect.DeepEqual(gotScores, wantScores) {
				t.Fatalf("score multiset changed across region groups: got=%v want=%v", gotScores, wantScores)
			}
			last := r.Samples[len(r.Samples)-1]
			servingLoad := 0
			for i, c := range last.Consumers {
				if c.Region != cfg.initialConsumerRegion(i) {
					t.Fatalf("slot %d has unexpected region %q", i, c.Region)
				}
				servingLoad += c.ServingLoad
			}
			if servingLoad != totalLoad {
				t.Fatalf("lost load across regional groups: got=%d want=%d", servingLoad, totalLoad)
			}
		})
	}
}

func TestRegionalCapacityJoins(t *testing.T) {
	cfg := smallConfig()
	cfg.Consumers, cfg.Shards = 1, 2
	cfg.ConsumerRegions = []ConsumerRegion{{Region: "westus", Consumers: 1}}
	cfg.ShardRegionPreferences = []ShardRegionPreference{{Shards: 2, Regions: []string{"eastus"}}}
	cfg.actions = []testAction{{Kind: "add", Count: 1, Region: "eastus"}}
	r := runTest(t, cfg)
	// Both shards initially run remotely. Native region affinity moves them to
	// the newly available preferred worker, even though this leaves west idle.
	fallback := false
	for _, sample := range r.Samples {
		if sample.Phase == "warmup" && sample.OutOfRegion == cfg.Shards {
			fallback = true
		}
	}
	moves := r.Summary.Movements["churn/balance"]
	if !fallback || r.Summary.FinalUnserved != 0 || r.Summary.FinalOutOfRegion != 0 || moves.CrossRegionMoves != 2 || moves.Completed != 2 {
		t.Fatalf("preferred-region capacity did not restore placement: fallback=%v summary=%+v", fallback, r.Summary)
	}
	for _, tr := range r.Transfers {
		if tr.FromRegion != "westus" || tr.ToRegion != "eastus" {
			t.Fatalf("incorrect transfer regions: %+v", tr)
		}
	}
	// Exercise report rendering and verify region fields survive every export.
	var html, csv, json bytes.Buffer
	if err := r.WriteHTML(&html); err != nil {
		t.Fatal(err)
	}
	if err := r.WriteCSV(&csv); err != nil {
		t.Fatal(err)
	}
	if err := r.WriteJSON(&json); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct{ output, want string }{
		{html.String(), "Cross-region moves"}, {html.String(), "Regional placement"},
		{csv.String(), "serving_load,region,out_of_region"}, {csv.String(), ",eastus,0"},
		{json.String(), `"preferred_regions"`}, {json.String(), `"cross_region_moves": 2`},
	} {
		if !strings.Contains(check.output, check.want) {
			t.Fatalf("report missing %q", check.want)
		}
	}
}

func TestRegionSurvivesConsumerLifecycle(t *testing.T) {
	cfg := smallConfig()
	cfg.Consumers = 1
	cfg.ConsumerRegions = []ConsumerRegion{{Region: "westus", Consumers: 1}}
	cfg.ShardRegionPreferences = []ShardRegionPreference{{Shards: cfg.Shards, Regions: nil}}
	cfg.actions = []testAction{{Kind: "reconnect"}, {At: Duration(15 * time.Second), Kind: "quit"}, {At: Duration(50 * time.Second), Kind: "add", Count: 1, Region: "eastus"}}
	r := runTest(t, cfg)
	original := r.Samples[0].Consumers[0].Consumer
	replaced := false
	for _, sample := range r.Samples {
		if sample.Consumers[0].Region != "westus" {
			t.Fatalf("reconnect/replacement changed slot region: %+v", sample)
		}
		if sample.Consumers[0].Consumer != original {
			replaced = true
		}
	}
	if !replaced || r.Summary.FinalUnserved != 0 || r.Summary.EventsExecuted["reconnect_complete"] != 1 || r.Summary.EventsExecuted["quit_complete"] != 1 {
		t.Fatalf("missing lifecycle recovery: %+v", r.Summary)
	}
	for _, event := range r.Events {
		want := "westus"
		if event.Kind == "add" {
			want = "eastus"
		}
		if event.Region != want {
			t.Fatalf("incorrect lifecycle region: %+v", event)
		}
	}
}

func TestRegionalGroupsBalanceByLoad(t *testing.T) {
	cfg := smallConfig()
	cfg.Consumers, cfg.Shards = 1, 3
	cfg.ShardLoadType = EvenDistribution
	cfg.ConsumerRegions = []ConsumerRegion{{Region: "westus", Consumers: 1}}
	cfg.ShardRegionPreferences = []ShardRegionPreference{{Shards: 1, Regions: []string{"westus"}}, {Shards: 2, Regions: []string{"westus"}}}
	cfg.actions = []testAction{{Kind: "add", Count: 1, Region: "westus"}}
	r := runTest(t, cfg)
	moves := r.Summary.Movements["churn/balance"]
	last := r.Samples[len(r.Samples)-1]
	if moves.Completed != 1 || moves.CrossRegionMoves != 0 || last.Consumers[0].ServingLoad != 51 || last.Consumers[1].ServingLoad != 99 {
		t.Fatalf("native allocator did not use per-domain seeded loads: last=%+v summary=%+v", last, r.Summary)
	}
}

func TestRandomShardAssignments(t *testing.T) {
	cfg := smallConfig()
	cfg.Consumers, cfg.Shards = 3, 99
	cfg.ShardLoadType = EvenDistribution
	cfg.ConsumerRegions = []ConsumerRegion{{Region: "centralus", Consumers: 1}, {Region: "northcentralus", Consumers: 1}, {Region: "eastus2", Consumers: 1}}
	cfg.ShardRegionPreferences = []ShardRegionPreference{{Shards: 33, Regions: []string{"centralus"}}, {Shards: 33, Regions: []string{"northcentralus"}}, {Shards: 33, Regions: []string{"eastus2"}}}
	first, repeated := runTest(t, cfg), runTest(t, cfg)
	if !reflect.DeepEqual(first.ShardLoads, repeated.ShardLoads) {
		t.Fatal("same seed changed static load/region assignments")
	}
	cfg.ShardLoadType = Fixed
	fixed := runTest(t, cfg)
	for i, a := range first.ShardLoads {
		if a.Index != i || fixed.ShardLoads[i].Index != i {
			t.Fatal("synthetic shard indices are not stable")
		}
		if a.Shard != fixed.ShardLoads[i].Shard || !slices.Equal(a.PreferredRegions, fixed.ShardLoads[i].PreferredRegions) || fixed.ShardLoads[i].Load != 50 {
			t.Fatal("changing load type changed region assignment or Fixed load")
		}
	}
	cfg.ShardLoadType = EvenDistribution
	cfg.Seed++
	changed := runTest(t, cfg)
	changedLoad, changedRegion := false, false
	counts, mins, maxs := map[string]int{}, map[string]int{}, map[string]int{}
	scores := make([]int, 0, cfg.Shards)
	transitions := 0
	previous := ""
	for i, shard := range first.ShardLoads {
		changedLoad = changedLoad || shard.Load != changed.ShardLoads[i].Load
		changedRegion = changedRegion || !slices.Equal(shard.PreferredRegions, changed.ShardLoads[i].PreferredRegions)
		region := shard.PreferredRegions[0]
		if i > 0 && region != previous {
			transitions++
		}
		previous = region
		if counts[region] == 0 {
			mins[region] = shard.Load
		}
		counts[region]++
		mins[region], maxs[region] = min(mins[region], shard.Load), max(maxs[region], shard.Load)
		scores = append(scores, shard.Load)
	}
	if !changedLoad || !changedRegion {
		t.Fatal("different seed did not change both static assignments")
	}
	slices.Sort(scores)
	for i, score := range scores {
		if score != i+1 {
			t.Fatalf("randomization lost score %d", i+1)
		}
	}
	// With seed 42 each region gets a mix of low and high scores, and region
	// assignments are interleaved rather than three consecutive blocks.
	if transitions < 10 {
		t.Fatalf("region assignments remain in sequential blocks: %d transitions", transitions)
	}
	for _, group := range cfg.ShardRegionPreferences {
		region := group.Regions[0]
		if counts[region] != group.Shards || mins[region] >= 33 || maxs[region] <= 66 {
			t.Fatalf("region count or score mixing incorrect: region=%s count=%d range=[%d,%d]", region, counts[region], mins[region], maxs[region])
		}
	}
}

func TestCustomizedDistributionValidation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		percentages []float64
		valid       bool
	}{
		{"missing", nil, false},
		{"empty", []float64{}, false},
		{"short", []float64{100}, false},
		{"long", make([]float64, 11), false},
		{"zero_total", make([]float64, 10), false},
		{"wrong_total", []float64{99, 0, 0, 0, 0, 0, 0, 0, 0, 0}, false},
		{"negative", []float64{101, -1, 0, 0, 0, 0, 0, 0, 0, 0}, false},
		{"over_100", []float64{101, 0, 0, 0, 0, 0, 0, 0, 0, 0}, false},
		{"nan", []float64{math.NaN(), 0, 0, 0, 0, 0, 0, 0, 0, 0}, false},
		{"infinity", []float64{math.Inf(1), 0, 0, 0, 0, 0, 0, 0, 0, 0}, false},
		{"first_only", []float64{100, 0, 0, 0, 0, 0, 0, 0, 0, 0}, true},
		{"last_only", []float64{0, 0, 0, 0, 0, 0, 0, 0, 0, 100}, true},
		{"fractional", []float64{60.1, 8.8, 4.4, 3.7, 2.8, 2.8, 2.2, 2.3, 5, 7.9}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.ShardLoadType = CustomizedDistribution
			cfg.ShardLoadDistribution = tc.percentages
			if err := cfg.validate(); (err == nil) != tc.valid {
				t.Fatalf("validation error = %v, valid = %v", err, tc.valid)
			}
			cfg.ShardLoadDistribution = []float64{100, 0, 0, 0, 0, 0, 0, 0, 0, 0}
			cfg.ShardRegionPreferences = []ShardRegionPreference{{Shards: cfg.Shards, Regions: []string{"centralus"}, ShardLoadDistribution: tc.percentages}}
			// Nil means the override was omitted, so the default remains valid.
			if err := cfg.validate(); (err == nil) != (tc.valid || tc.percentages == nil) {
				t.Fatalf("regional validation error = %v, valid = %v", err, tc.valid)
			}
		})
	}
	cfg := DefaultConfig()
	cfg.ShardLoadDistribution = []float64{100, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	for _, kind := range []ShardLoadType{Fixed, EvenDistribution} {
		cfg.ShardLoadType = kind
		if err := cfg.validate(); err == nil {
			t.Fatal("unused distribution accepted")
		}
		cfg.ShardRegionPreferences = []ShardRegionPreference{{Shards: cfg.Shards, ShardLoadDistribution: cfg.ShardLoadDistribution}}
		cfg.ShardLoadDistribution = nil
		if err := cfg.validate(); err == nil {
			t.Fatal("unused regional distribution accepted")
		}
		cfg.ShardLoadDistribution = cfg.ShardRegionPreferences[0].ShardLoadDistribution
		cfg.ShardRegionPreferences = nil
	}
}

func TestCustomizedDistributionSampling(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ShardLoadType = CustomizedDistribution
	cfg.Shards = 100000
	cfg.ShardLoadDistribution = []float64{60, 0, 0, 0, 0, 0, 0, 0, 0, 40}
	scores := cfg.shardLoadScores()
	if !slices.Equal(scores, cfg.shardLoadScores()) {
		t.Fatal("same seed changed sampled scores")
	}
	cfg.Seed++
	if slices.Equal(scores, cfg.shardLoadScores()) {
		t.Fatal("different seed did not change scores")
	}
	counts := map[int]int{}
	low := 0
	for _, score := range scores {
		if score < 1 || (score >= 10 && score < 90) || score >= 100 {
			t.Fatalf("sampled disabled bucket: %d", score)
		}
		if score < 10 {
			low++
		}
		counts[score]++
	}
	if math.Abs(float64(low)/float64(cfg.Shards)-0.6) > 0.01 {
		t.Fatalf("incorrect bucket weighting: %d/%d", low, cfg.Shards)
	}
	for score := 1; score < 100; score++ {
		expected := 0.0
		switch {
		case score < 10:
			expected = 0.6 / 9
		case score >= 90:
			expected = 0.4 / 10
		default:
			continue
		}
		if math.Abs(float64(counts[score])/float64(cfg.Shards)-expected) > 0.005 {
			t.Fatalf("nonuniform score sampling: score=%d count=%d", score, counts[score])
		}
	}
	// Single-bucket configurations exercise every lower/upper bound and zero buckets.
	cfg.Shards = 1000
	for bucket := range 10 {
		cfg.ShardLoadDistribution = make([]float64, 10)
		cfg.ShardLoadDistribution[bucket] = 100
		seen := map[int]bool{}
		for _, score := range cfg.shardLoadScores() {
			if score < max(1, bucket*10) || score >= (bucket+1)*10 {
				t.Fatalf("bucket %d produced %d", bucket, score)
			}
			seen[score] = true
		}
		if len(seen) != (bucket+1)*10-max(1, bucket*10) {
			t.Fatalf("bucket %d missed score endpoints: %v", bucket, seen)
		}
	}
}

func TestCustomizedDistributionSimulation(t *testing.T) {
	cfg := smallConfig()
	cfg.Shards = 64
	cfg.ShardRegionPreferences = []ShardRegionPreference{{Shards: 32, Regions: []string{"centralus"}}, {Shards: 32, Regions: []string{"eastus"}}}
	cfg.ShardLoadType = CustomizedDistribution
	cfg.ShardLoadDistribution = []float64{60, 0, 0, 0, 0, 0, 0, 0, 0, 40}
	cfg.actions = []testAction{{Kind: "add", Count: 1}}
	report := runTest(t, cfg)
	total := 0
	for i, score := range cfg.shardLoadScores() {
		if report.ShardLoads[i].Load != score {
			t.Fatal("simulation changed sampled score")
		}
		total += score
	}
	last := report.Samples[len(report.Samples)-1]
	serving := 0
	for _, c := range last.Consumers {
		serving += c.ServingLoad
	}
	if serving != total || last.Unserved != 0 {
		t.Fatalf("lost sampled load: serving=%d want=%d unserved=%d", serving, total, last.Unserved)
	}
	cfg.ShardLoadType = Fixed
	cfg.ShardLoadDistribution = nil
	fixed := runTest(t, cfg)
	for i, shard := range report.ShardLoads {
		if shard.Shard != fixed.ShardLoads[i].Shard || !slices.Equal(shard.PreferredRegions, fixed.ShardLoads[i].PreferredRegions) {
			t.Fatal("custom sampling changed region assignment")
		}
	}
	// A one-shard custom distribution must sample normally rather than use 50.
	cfg.Shards = 1
	cfg.ShardRegionPreferences = nil
	cfg.actions = nil
	cfg.ShardLoadType = CustomizedDistribution
	cfg.ShardLoadDistribution = []float64{0, 0, 0, 0, 0, 0, 0, 0, 0, 100}
	single := runTest(t, cfg)
	if score := single.ShardLoads[0].Load; score < 90 || score >= 100 {
		t.Fatalf("singleton score = %d", score)
	}
}

func TestBalanceDecisionDiff(t *testing.T) {
	cfg := smallConfig()
	cfg.Consumers = 1
	cfg.actions = []testAction{{Kind: "add", Count: 1}}
	r := runTest(t, cfg)
	pathA, pathB := 0, 0
	for _, transfer := range r.Transfers {
		if transfer.Reason != "balance" {
			continue
		}
		diff := transfer.BalanceDiff
		if diff == nil || diff.Place != 0 || diff.Colo != 0 || diff.Load > 0 {
			t.Fatalf("missing or invalid load-only allocator delta: %+v", diff)
		}
		if diff.Load < 0 {
			pathA++
		} else {
			pathB++
		}
	}
	if pathA == 0 || pathB == 0 {
		t.Fatalf("expected both balancing paths, got A=%d B=%d", pathA, pathB)
	}
	var output bytes.Buffer
	if err := r.WriteJSON(&output); err != nil {
		t.Fatal(err)
	}
	var decoded Report
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	for i, transfer := range decoded.Transfers {
		if !reflect.DeepEqual(transfer.BalanceDiff, r.Transfers[i].BalanceDiff) {
			t.Fatal("JSON lost allocator decision delta")
		}
	}
}

func TestRemovedConfigFields(t *testing.T) {
	for _, field := range []string{"recovery", "sample_interval", "load_delay", "drain_delay", "events", "replacement_delay"} {
		t.Run(field, func(t *testing.T) {
			cfg := DefaultConfig()
			decoder := json.NewDecoder(strings.NewReader(fmt.Sprintf("{%q:null}", field)))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&cfg); err == nil {
				t.Fatal("removed scenario field accepted")
			}
		})
	}
}

func TestCutoffAndFixedSampling(t *testing.T) {
	for _, kind := range []string{"reconnect", "quit"} {
		t.Run(kind, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Consumers, cfg.Shards = 2, 8
			cfg.Warmup = Duration(time.Minute)
			cfg.Duration = Duration(75 * time.Second)
			cfg.ReconnectDelay = Duration(time.Minute)
			end := cfg.Warmup + cfg.Duration
			r := run(t, cfg, func(s *simulation) {
				s.after(time.Duration(end)-time.Second, func() { s.execute(kind, 0) })
			})
			last := r.Samples[len(r.Samples)-1]
			if last.At != end || last.Phase != "churn" {
				t.Fatalf("simulation ran beyond churn: %+v", last)
			}
			if r.Summary.EventsExecuted[kind] != 1 || r.Summary.EventsExecuted[kind+"_complete"] != 0 {
				t.Fatalf("pending lifecycle completed beyond cutoff: %+v", r.Summary)
			}
			if last.Consumers[0].Connected {
				t.Fatal("consumer reconnected beyond cutoff")
			}
			want := []Duration{0, Duration(time.Minute), Duration(2 * time.Minute), end}
			if len(r.Samples) != len(want) {
				t.Fatalf("unexpected sample count: %d", len(r.Samples))
			}
			for i, at := range want {
				if r.Samples[i].At != at {
					t.Fatalf("sample %d: got %s want %s", i, time.Duration(r.Samples[i].At), time.Duration(at))
				}
			}
			if kind == "quit" && last.Unserved == 0 {
				t.Fatal("cutoff hid shards still awaiting replacement")
			}
		})
	}
}

func TestRegionalLoadDistributions(t *testing.T) {
	cfg := smallConfig()
	cfg.Shards = 32
	cfg.ShardLoadType = CustomizedDistribution
	cfg.ShardLoadDistribution = []float64{0, 0, 0, 0, 0, 100, 0, 0, 0, 0}
	cfg.ShardRegionPreferences = []ShardRegionPreference{
		{Shards: 8, Regions: []string{"centralus"}, ShardLoadDistribution: []float64{100, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{Shards: 8, Regions: []string{"eastus"}, ShardLoadDistribution: []float64{0, 0, 0, 0, 0, 0, 0, 0, 0, 100}},
		{Shards: 8, Regions: []string{"centralus", "eastus"}},
		{Shards: 8},
	}
	// Exercise the public JSON schema, including the report's config serialization.
	data, err := json.Marshal(cfg.Config)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Config
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.Config, decoded) {
		t.Fatal("regional distributions changed during JSON round trip")
	}
	scores := cfg.shardLoadScores()
	if !slices.Equal(scores, cfg.shardLoadScores()) {
		t.Fatal("regional scores are not seeded")
	}
	report := runTest(t, cfg)
	counts := map[string]int{}
	for _, shard := range report.ShardLoads {
		lower, upper, key := 50, 60, "default"
		if len(shard.PreferredRegions) == 1 {
			key = shard.PreferredRegions[0]
			if key == "centralus" {
				lower, upper = 1, 10
			} else {
				lower, upper = 90, 100
			}
		}
		counts[key]++
		if shard.Load < lower || shard.Load >= upper || shard.Load != scores[shard.Index] {
			t.Fatalf("wrong score for preferred regions: %+v", shard)
		}
	}
	if counts["centralus"] != 8 || counts["eastus"] != 8 || counts["default"] != 16 {
		t.Fatalf("wrong regional populations: %v", counts)
	}
	// Turning off overrides must preserve identities and all fallback scores.
	cfg.ShardRegionPreferences[0].ShardLoadDistribution = nil
	cfg.ShardRegionPreferences[1].ShardLoadDistribution = nil
	fallback := runTest(t, cfg)
	for i, shard := range report.ShardLoads {
		other := fallback.ShardLoads[i]
		if shard.Shard != other.Shard || !slices.Equal(shard.PreferredRegions, other.PreferredRegions) {
			t.Fatal("regional distributions changed shard identities")
		}
		if len(shard.PreferredRegions) != 1 && shard.Load != other.Load {
			t.Fatal("regional overrides changed fallback scores")
		}
	}
}

func TestRegionalDrainRates(t *testing.T) {
	zero, override := 0.0, 4.0
	cfg := smallConfig()
	cfg.Consumers = 3
	cfg.DrainRate = 2
	cfg.ConsumerRegions = []ConsumerRegion{
		{Region: "centralus", Consumers: 1, DrainRate: &override},
		{Region: "eastus", Consumers: 1, DrainRate: &zero},
		{Region: "westus", Consumers: 1},
	}
	cfg.ShardRegionPreferences = []ShardRegionPreference{{Shards: cfg.Shards}}
	cfg.actions = []testAction{{Kind: "quit", Consumer: 1}, {At: Duration(time.Minute), Kind: "reconnect", Consumer: 0}}
	// The JSON representation must distinguish omitted rates from explicit zero.
	data, err := json.Marshal(cfg.Config)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Config
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, cfg.Config) {
		t.Fatal("regional rates changed on JSON round trip")
	}
	for region, want := range map[string]float64{"centralus": 4, "eastus": 0, "westus": 2, "unlisted": 2} {
		if got := decoded.drainRateForRegion(region); got != want {
			t.Fatalf("%s drain rate = %v, want %v", region, got, want)
		}
	}
	report := runTest(t, cfg)
	counts := map[string]int{}
	for _, event := range report.Events {
		if event.Kind == "drain" {
			counts[event.Region]++
		}
	}
	if report.Summary.EventsExecuted["quit_complete"] != 1 || report.Summary.EventsExecuted["reconnect_complete"] != 1 {
		t.Fatal("replacement/reconnect was not exercised")
	}
	if counts["centralus"] == 0 || counts["westus"] == 0 || counts["eastus"] != 0 {
		t.Fatalf("overrides/fallback were not applied to scheduling: %v", counts)
	}
}

func TestRegionalDrainRateValidation(t *testing.T) {
	for _, rate := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1), 0, 0.1} {
		cfg := DefaultConfig()
		cfg.ConsumerRegions = []ConsumerRegion{{Region: "centralus", Consumers: cfg.Consumers, DrainRate: &rate}}
		valid := rate >= 0 && !math.IsInf(rate, 0)
		if err := cfg.validate(); (err == nil) != valid {
			t.Fatalf("regional drain_rate %v validation = %v, valid = %v", rate, err, valid)
		}
	}
}

func TestIndependentLifecycleSchedules(t *testing.T) {
	cfg := smallConfig()
	cfg.Consumers, cfg.Shards = 3, 24
	cfg.Duration = Duration(15 * time.Minute)
	cfg.DrainRate, cfg.RelinquishRate, cfg.ReconnectRate, cfg.QuitRate = 0.3, 0.5, 0.3, 0.2
	zero, high := 0.0, 3.0
	cfg.ConsumerRegions = []ConsumerRegion{
		{Region: "centralus", Consumers: 2, DrainRate: &zero},
		{Region: "northcentralus", Consumers: 1},
	}
	cfg.ShardRegionPreferences = []ShardRegionPreference{{Shards: cfg.Shards}}
	base := runTest(t, cfg)
	type attempt struct {
		At   Duration
		Slot int
		Kind string
	}
	// Compare attempts, including skipped attempts. Outcomes and selected shard
	// identities can change with allocation, while these event clocks must not.
	schedule := func(r Report, includeCUDrain bool) []attempt {
		var attempts []attempt
		for _, e := range r.Events {
			if e.Slot >= 3 || (!includeCUDrain && e.Kind == "drain" && e.Region == "centralus") {
				continue
			}
			switch e.Kind {
			case "drain", "relinquish", "reconnect", "quit":
				attempts = append(attempts, attempt{e.At, e.Slot, e.Kind})
			}
		}
		return attempts
	}
	want := schedule(base, false)
	counts := map[string]int{}
	for _, e := range want {
		counts[e.Kind]++
	}
	for _, kind := range []string{"drain", "relinquish", "reconnect", "quit"} {
		if counts[kind] == 0 {
			t.Fatalf("did not exercise %s scheduling", kind)
		}
	}
	cfg.ConsumerRegions[0].DrainRate = &high
	changed := runTest(t, cfg)
	if !reflect.DeepEqual(want, schedule(changed, false)) {
		t.Fatal("CU drain rate changed another lifecycle clock")
	}
	if len(schedule(changed, true)) <= len(want) {
		t.Fatal("increased CU drain rate did not add attempts")
	}
	// Different shard populations change relinquish selection and availability,
	// but must not affect any lifecycle clock.
	cfg.Shards = 48
	cfg.ShardRegionPreferences[0].Shards = cfg.Shards
	moreShards := runTest(t, cfg)
	if !reflect.DeepEqual(schedule(changed, true), schedule(moreShards, true)) {
		t.Fatal("shard selection or availability changed lifecycle clocks")
	}
	// Creating another slot cannot consume random draws for existing slots.
	cfg.Consumers = 4
	cfg.ConsumerRegions[1].Consumers = 2
	moreConsumers := runTest(t, cfg)
	if !reflect.DeepEqual(schedule(changed, true), schedule(moreConsumers, true)) {
		t.Fatal("another consumer changed existing slot clocks")
	}
	cfg.Seed++
	differentSeed := runTest(t, cfg)
	if reflect.DeepEqual(schedule(moreConsumers, true), schedule(differentSeed, true)) {
		t.Fatal("seed did not change lifecycle clocks")
	}
}

// withProtocolSimulation drives deliberately queued protocol messages without a
// coordinator goroutine, so lease/assignment races have deterministic ordering.
func withProtocolSimulation(t *testing.T, check func(*testing.T, *simulation, []model.Shard)) {
	t.Helper()
	simulationMu.Lock()
	defer simulationMu.Unlock()
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Consumers, cfg.Shards, cfg.Warmup = 2, 2, 0
		s := &simulation{t: t, cfg: cfg, start: time.Now(),
			service: model.QualifiedServiceName{Tenant: "simulation", Service: "simulation"},
			events:  make(chan func(), 4096), done: make(chan struct{}),
			consumerRegions: map[string]string{}, transfers: map[model.GrantID]*Transfer{},
			owners: map[model.Shard]owner{}, causes: map[model.GrantID]string{}, seen: map[model.GrantID]bool{}, retired: map[string]bool{},
			shardLoads: map[model.Shard]int{}, shardPreferences: map[model.Shard][]string{},
			report: Report{Config: cfg}}
		defer s.captureMoveLogs()()
		defer func() {
			close(s.done)
			for _, timer := range s.timers {
				timer.Stop()
			}
			synctest.Wait()
		}()
		for range 2 {
			c := &consumer{region: "centralus", slot: len(s.consumers), generation: 1,
				grants: map[model.GrantID]*grant{}, in: make(chan model.ConsumerMessage, 4096)}
			s.newIdentity(c)
			s.consumers = append(s.consumers, c)
		}
		shards := model.NewShards(model.QualifiedDomainName{Service: s.service, Domain: "shards"}, model.Global, "", 2)
		for _, shard := range shards {
			s.shardLoads[shard] = 50
		}
		check(t, s, shards)
	})
}

func TestReconnectExtendsOnlyConfirmedGrants(t *testing.T) {
	withProtocolSimulation(t, func(t *testing.T, s *simulation, shards []model.Shard) {
		c := s.consumers[0]
		oldLease := time.Now().Add(5 * time.Second)
		from := model.NewGrant("source", shards[0], model.ActiveGrantState, oldLease, time.Now())
		retained := model.NewGrant("retained", shards[1], model.ActiveGrantState, oldLease, time.Now())
		s.handle(c, model.NewAssign(from, retained))
		s.settle()
		// Revoke for source was lost on the old connection. Extend arrives on the
		// new connection before Assign confirms only the surviving retained grant.
		c.generation++
		lease := time.Now().Add(time.Minute)
		s.handle(c, model.NewExtend(lease))
		if !c.grants[from.ID()].lease.Equal(oldLease) || !c.grants[retained.ID()].lease.Equal(oldLease) {
			t.Fatal("unconfirmed retained grants received a new lease")
		}
		s.handle(c, model.NewAssign(model.NewGrant(retained.ID(), retained.Shard(), retained.State(), lease, retained.Assigned())))
		lease = lease.Add(time.Minute)
		s.handle(c, model.NewExtend(lease))
		if !c.grants[retained.ID()].lease.Equal(lease) || !c.grants[from.ID()].lease.Equal(oldLease) {
			t.Fatal("extension did not distinguish confirmed and unconfirmed grants")
		}
		time.Sleep(6 * time.Second)
		s.settle()
		if c.grants[from.ID()] != nil || c.grants[retained.ID()] == nil || !c.grants[retained.ID()].serving {
			t.Fatal("lost-revoke source did not expire while retained grant continued")
		}
	})
}

func TestReassignedExpiredTransferCompletes(t *testing.T) {
	withProtocolSimulation(t, func(t *testing.T, s *simulation, shards []model.Shard) {
		from := model.NewGrant("source", shards[0], model.ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		to := model.NewGrant("destination", shards[0], model.AllocatedGrantState, time.Now().Add(time.Second), time.Now())
		s.handle(s.consumers[0], model.NewAssign(from))
		s.settle()
		s.balance(from, to, string(s.consumers[0].instance.ID()), string(s.consumers[1].instance.ID()))
		s.handle(s.consumers[1], model.NewAssign(to))
		time.Sleep(2 * time.Second)
		s.settle()
		if s.transfers[to.ID()].Status != "expired" {
			t.Fatal("destination did not expire locally")
		}
		// The coordinator can restore the same grant ID on reattachment before
		// its cleanup tick. Source is no longer serving when promotion succeeds.
		s.stopGrant(s.consumers[0], from.ID(), "interrupted")
		s.handle(s.consumers[1], model.NewAssign(model.NewGrant(to.ID(), to.Shard(), model.ActiveGrantState, time.Now().Add(time.Minute), to.Assigned())))
		s.settle()
		transfer := s.transfers[to.ID()]
		if transfer.Status != "completed" || transfer.CompletedAt == nil || len(s.report.Transfers) != 1 {
			t.Fatalf("restored handoff not completed exactly once: %+v", transfer)
		}
	})
}

func TestQuitBeforeInitialServingRetainsProvenance(t *testing.T) {
	withProtocolSimulation(t, func(t *testing.T, s *simulation, shards []model.Shard) {
		from := model.NewGrant("source", shards[0], model.ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		source, destination := s.consumers[0], s.consumers[1]
		source.connected = true
		s.handle(source, model.NewAssign(from))
		if source.grants[from.ID()].serving {
			t.Fatal("initial load callback unexpectedly completed")
		}
		s.execute("quit", 0)
		to := model.NewGrant("replacement", shards[0], model.ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		s.handle(destination, model.NewAssign(to))
		s.settle()
		transfer := s.transfers[to.ID()]
		if transfer == nil || transfer.SourceGrant != string(from.ID()) || transfer.Reason != "quit" || transfer.Status != "completed" {
			t.Fatalf("initialization quit lost transfer provenance: %+v", transfer)
		}
	})
}

func TestQuitInterruptsUnreceivedAndLateTransfers(t *testing.T) {
	withProtocolSimulation(t, func(t *testing.T, s *simulation, shards []model.Shard) {
		from := model.NewGrant("source", shards[0], model.ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		source, destination := s.consumers[0], s.consumers[1]
		s.handle(source, model.NewAssign(from))
		s.settle()
		to := model.NewGrant("unreceived", shards[0], model.AllocatedGrantState, time.Now().Add(time.Minute), time.Now())
		s.balance(from, to, string(source.instance.ID()), string(destination.instance.ID()))
		destination.connected = true
		s.execute("quit", 1)
		if s.transfers[to.ID()].Status != "interrupted" {
			t.Fatal("transfer stayed pending when destination quit before Assign")
		}
		late := model.NewGrant("late", shards[0], model.AllocatedGrantState, to.Lease(), to.Assigned())
		s.balance(from, late, string(source.instance.ID()), string(destination.instance.ID()))
		if s.transfers[late.ID()].Status != "interrupted" {
			t.Fatal("late move log resurrected transfer to retired identity")
		}
		s.sample()
		s.finish()
		if got := s.report.Summary.Movements["churn/balance"]; got.Moves != 2 || got.Interrupted != 2 || got.Pending != 0 {
			t.Fatalf("quit transfer accounting is wrong: %+v", got)
		}
	})
}

func TestImmediateDrainReconnect(t *testing.T) {
	cfg := smallConfig()
	cfg.ReconnectDelay = 0
	cfg.actions = []testAction{{Kind: "drain", Consumer: 0}, {Kind: "reconnect", Consumer: 0}}
	r := runTest(t, cfg)
	if r.Summary.FinalUnserved != 0 {
		t.Fatalf("immediate drain/reconnect left unserved shards: %+v", r.Summary)
	}
}

func TestMoveLogAfterAssignmentPreservesOutcome(t *testing.T) {
	withProtocolSimulation(t, func(t *testing.T, s *simulation, shards []model.Shard) {
		from := model.NewGrant("source", shards[0], model.ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		to := model.NewGrant("destination", shards[0], model.ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		source, destination := s.consumers[0], s.consumers[1]
		s.handle(source, model.NewAssign(from))
		s.settle()
		s.handle(destination, model.NewAssign(to))
		s.settle()
		transfer := s.transfers[to.ID()]
		if transfer == nil || transfer.Reason != "reassignment" || transfer.Status != "completed" {
			t.Fatalf("expected completed reassignment before move log, got %+v", transfer)
		}

		ctx := log.NewContext(context.Background(), log.String("tenant", string(s.service.Tenant)), log.String("service", string(s.service.Service)))
		fields := []log.Field{
			log.String("from_worker", string(source.instance.ID())), log.String("to_worker", string(destination.instance.ID())),
			log.String("from_grant", string(from.ID())), log.String("to_grant", string(to.ID())),
			log.String("load", "-3[load=-3, place=0, colo=0]"),
		}
		for range 2 {
			model.LogCoordinatorGrantEvent(ctx, log.SevInfo, "Initiated grant move", "", from.Shard(), from.ID(), source.instance.ID(), nil, nil, fields...)
		}
		s.settle()
		if len(s.report.Transfers) != 1 || s.transfers[to.ID()] != transfer || transfer.Status != "completed" {
			t.Fatalf("late move log duplicated or reset the handoff: %+v", s.report.Transfers)
		}
		if transfer.Reason != "balance" || transfer.BalanceDiff == nil || transfer.BalanceDiff.Load != -3 {
			t.Fatalf("late move log lost balancing classification: %+v", transfer)
		}
	})
}

func TestMoveLogRejectsMalformedRecords(t *testing.T) {
	service := model.QualifiedServiceName{Tenant: "simulation", Service: "simulation"}
	valid := []log.Field{
		log.String("event_source", "coordinator"), log.String("domain", "shards"), log.String("shard_region", ""),
		log.String("shard_from", model.ZeroKey.String()), log.String("shard_to", model.MaxKey.String()),
		log.String("from_worker", "source"), log.String("to_worker", "destination"),
		log.String("from_grant", "old"), log.String("to_grant", "new"), log.String("load", "0[load=0, place=0, colo=0]"),
	}
	if _, err := parseMoveLog(service, valid); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, key, value string }{
		{"missing_grant", "to_grant", ""}, {"invalid_shard", "shard_from", "invalid"},
		{"invalid_load", "load", "changed format"}, {"inconsistent_load", "load", "1[load=0, place=0, colo=0]"},
		{"other_source", "event_source", "consumer"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fields := append([]log.Field(nil), valid...)
			fields = append(fields, log.String(test.key, test.value))
			if _, err := parseMoveLog(service, fields); err == nil {
				t.Fatal("malformed move log was accepted")
			}
		})
	}
}

func TestConcurrentSimulationLogCapture(t *testing.T) {
	for i := range 2 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			cfg := smallConfig()
			cfg.Shards += i * 5
			cfg.Consumers = 1
			cfg.actions = []testAction{{Kind: "add", Count: 1}}
			report := runTest(t, cfg)
			balances := 0
			for _, transfer := range report.Transfers {
				if transfer.Reason == "balance" {
					balances++
					if transfer.BalanceDiff == nil {
						t.Fatal("balancing move lost its log delta")
					}
				}
			}
			if balances == 0 {
				t.Fatal("no balancing logs captured")
			}
			if report.Summary.FinalUnserved != 0 {
				t.Fatal("unserved shards after concurrent run")
			}
		})
	}
}
