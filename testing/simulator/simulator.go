package simulator

import (
	"context"
	"fmt"
	rand "math/rand/v2"
	"sort"
	"testing"
	"testing/synctest"
	"time"

	"go.atoms.co/lib/net/location/go/location"
	"go.atoms.co/lib/net/session/go/session"

	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
)

// sampleInterval fixes report snapshots at one virtual minute.
const sampleInterval = time.Minute

// replacementDelay is downtime before a fresh identity replaces a quit consumer.
const replacementDelay = 5 * time.Second

type simulation struct {
	shardLoads       map[model.Shard]int
	shardPreferences map[model.Shard][]string
	consumerRegions  map[string]string
	t                *testing.T
	cfg              Config
	start            time.Time
	coord            coordinator.Coordinator
	service          model.QualifiedServiceName
	events           chan func()
	done             chan struct{}
	timers           []*time.Timer
	consumers        []*consumer
	randomStreams    map[randomStream]*rand.Rand
	report           Report
	transfers        map[model.GrantID]*Transfer
	owners           map[model.Shard]owner
	causes           map[model.GrantID]string
	seen             map[model.GrantID]bool
	retired          map[string]bool
	serial           int
}

// randomStream identifies a stable consumer slot and one random purpose. Slots
// survive reconnects and identity replacements; timing and shard choices never
// consume draws from one another.
type randomStream struct {
	slot int
	kind string
}

func (s *simulation) randomFor(slot int, kind string) *rand.Rand {
	key := randomStream{slot: slot, kind: kind}
	if rng, ok := s.randomStreams[key]; ok {
		return rng
	}
	// Explicit IDs keep existing streams stable when another purpose is added.
	// Reserve eight streams per slot, outside setup's PCG sequences 1, 2 and 3.
	var purpose uint64
	switch kind {
	case "drain":
		purpose = 0
	case "relinquish":
		purpose = 1
	case "reconnect":
		purpose = 2
	case "quit":
		purpose = 3
	case "relinquish_shard":
		purpose = 4
	default:
		panic(fmt.Sprintf("unknown simulation random stream %q", kind))
	}
	rng := rand.New(rand.NewPCG(uint64(s.cfg.Seed), 256+uint64(slot)*8+purpose))
	s.randomStreams[key] = rng
	return rng
}

type owner struct {
	consumer string
	grant    model.GrantID
}

// Run executes the scenario in virtual time. Call outside any existing synctest
// bubble. The returned report can be written to disk after the bubble exits.
func Run(t *testing.T, cfg Config) Report {
	t.Helper()
	return run(t, cfg, nil)
}

// run accepts setup for deterministic protocol tests; scenarios use random rates.
func run(t *testing.T, cfg Config, setup func(*simulation)) Report {
	t.Helper()
	simulationMu.Lock()
	defer simulationMu.Unlock()
	if err := cfg.validate(); err != nil {
		t.Fatalf("invalid simulation: %v", err)
	}
	var result Report
	synctest.Test(t, func(t *testing.T) {
		s := &simulation{t: t, cfg: cfg, start: time.Now(), service: model.QualifiedServiceName{Tenant: "simulation", Service: "simulation"},
			events: make(chan func(), 4096), done: make(chan struct{}), randomStreams: map[randomStream]*rand.Rand{},
			consumerRegions: map[string]string{}, transfers: map[model.GrantID]*Transfer{}, owners: map[model.Shard]owner{}, causes: map[model.GrantID]string{}, seen: map[model.GrantID]bool{}, retired: map[string]bool{},
			report: Report{Config: cfg, Events: []LifecycleEvent{}, Transfers: []*Transfer{}, Samples: []Sample{}}}
		defer s.captureMoveLogs()()
		s.run(setup)
		result = s.report
	})
	return result
}

func (s *simulation) post(fn func()) {
	select {
	case s.events <- fn:
	case <-s.done:
	}
}

func (s *simulation) after(delay time.Duration, fn func()) {
	s.timers = append(s.timers, time.AfterFunc(max(delay, 0), func() { s.post(fn) }))
}

func (s *simulation) elapsed() Duration { return Duration(time.Since(s.start)) }

func (s *simulation) phase() string {
	elapsed := s.elapsed()
	if elapsed < s.cfg.Warmup {
		return "warmup"
	}
	return "churn"
}

func (s *simulation) run(setup func(*simulation)) {
	tenant, err := model.NewTenant(s.service.Tenant, s.start)
	if err != nil {
		s.t.Fatal(err)
	}
	service, err := model.NewService(s.service, s.start, model.WithServiceConfig(
		model.NewServiceConfig(model.WithTrackLoad(s.cfg.ShardLoadType != Fixed))))
	if err != nil {
		s.t.Fatal(err)
	}
	domains := s.initializeDomains()
	loads := s.initializeShardLoads(domains)
	state := core.NewState(model.NewTenantInfo(tenant, 1, s.start), []model.ServiceInfoEx{model.NewServiceInfoEx(model.NewServiceInfo(service, 1, s.start), domains)}, nil, loads)
	ctx, cancel := context.WithCancel(context.Background())
	coord, status := coordinator.New(ctx, location.New("centralus", "simulator"), s.service, state, make(chan core.Update))
	s.coord = coord
	defer func() {
		close(s.done)
		for _, timer := range s.timers {
			timer.Stop()
		}
		for _, c := range s.consumers {
			if c.connected {
				close(c.in)
				c.connected = false
			}
		}
		coord.Close()
		cancel()
		synctest.Wait()
	}()
	go func() {
		for {
			select {
			case _, ok := <-status:
				if !ok {
					status = nil
				}
			case <-s.done:
				return
			}
		}
	}()
	<-coord.Initialized().Closed()
	for range s.cfg.Consumers {
		s.addConsumer(s.cfg.initialConsumerRegion(len(s.consumers)))
	}
	if setup != nil {
		setup(s)
	}

	s.settle()
	s.sample()
	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()
	end := time.NewTimer(time.Duration(s.cfg.Warmup + s.cfg.Duration))
	defer end.Stop()
	for {
		select {
		case fn := <-s.events:
			fn()
		case <-ticker.C:
			s.settle()
			s.sample()
		case <-end.C:
			s.settle()
			s.sample()
			s.finish()
			return
		}
	}
}

// settle processes messages and zero-delay actions at the current virtual time.
// No disk IO is performed in the bubble, so waiting goroutines remain durable.
func (s *simulation) settle() {
	for {
		synctest.Wait()
		select {
		case fn := <-s.events:
			fn()
		default:
			return
		}
	}
}

func (s *simulation) addConsumer(region string) {
	c := &consumer{region: region, slot: len(s.consumers), grants: map[model.GrantID]*grant{}}
	s.consumers = append(s.consumers, c)
	s.newIdentity(c)
	s.connect(c)
	for _, rate := range []struct {
		kind string
		rate float64
	}{{"drain", s.cfg.drainRateForRegion(region)}, {"relinquish", s.cfg.RelinquishRate}, {"reconnect", s.cfg.ReconnectRate}, {"quit", s.cfg.QuitRate}} {
		s.scheduleRandom(c.slot, rate.kind, rate.rate)
	}
}

func (s *simulation) newIdentity(c *consumer) {
	s.serial++
	id := fmt.Sprintf("consumer-%04d", s.serial)
	c.instance = model.NewInstance(location.NewInstance(location.New(location.Region(c.region), location.Node(id)), location.WithInstanceID(model.InstanceID(id)), location.WithInstanceCreatedAt(time.Now())), id)
	s.consumerRegions[id] = c.region
}

func (s *simulation) connect(c *consumer) {
	var retained []model.Grant
	for _, g := range c.grants {
		if time.Now().Before(g.lease) {
			retained = append(retained, g.value())
		}
	}
	sort.Slice(retained, func(i, j int) bool { return retained[i].ID() < retained[j].ID() })
	in := make(chan model.ConsumerMessage, 4096)
	metadata := model.NewConsumerMetadata()
	model.UnwrapConsumerMetadata(metadata).Version = "simulator"
	in <- model.NewRegister(c.instance, s.service, nil, retained, model.NewOptions(), metadata)
	out, err := s.coord.Connect(context.Background(), session.NewID(), c.instance.Instance(), in)
	if err != nil {
		s.t.Fatalf("connect %s: %v", c.instance.ID(), err)
	}
	c.in = in
	c.connected = true
	c.generation++
	generation := c.generation
	go func() {
		for msg := range out {
			if !msg.IsClientMessage() {
				continue // Consumer discovery broadcasts do not affect ownership.
			}
			s.post(func() {
				if c.connected && c.generation == generation {
					s.handle(c, msg)
				}
			})
		}
	}()
}

func (s *simulation) scheduleRandom(slot int, kind string, rate float64) {
	if rate == 0 {
		return
	}
	now := time.Since(s.start)
	base := max(now, time.Duration(s.cfg.Warmup))
	delayFloat := s.randomFor(slot, kind).ExpFloat64() * float64(time.Minute) / rate
	remaining := time.Duration(s.cfg.Warmup+s.cfg.Duration) - base
	if delayFloat >= float64(remaining) {
		return
	}
	delay := max(time.Duration(delayFloat), time.Nanosecond)
	s.after(base-now+delay, func() { s.execute(kind, slot); s.scheduleRandom(slot, kind, rate) })
}

func (s *simulation) execute(kind string, slot int) {
	e := LifecycleEvent{At: s.elapsed(), Kind: kind, Slot: slot, Outcome: "executed"}
	c := s.consumers[slot]
	e.Consumer = string(c.instance.ID())
	e.Region = c.region
	if !c.connected {
		e.Outcome = "skipped_disconnected"
		s.report.Events = append(s.report.Events, e)
		return
	}
	switch kind {
	case "drain", "relinquish":
		var active []model.Grant
		for _, g := range c.grants {
			if g.state == model.ActiveGrantState && time.Now().Before(g.lease) && (kind == "drain" || g.serving) {
				active = append(active, g.value())
			}
		}
		sort.Slice(active, func(i, j int) bool { return active[i].ID() < active[j].ID() })
		if len(active) == 0 {
			e.Outcome = "skipped_no_active_shards"
		} else if kind == "relinquish" {
			// Release one randomly selected serving shard immediately. The real
			// coordinator delays reassignment until its prior lease expires.
			g := active[s.randomFor(slot, "relinquish_shard").IntN(len(active))]
			e.Shard, e.Grant = shardName(g.Shard()), string(g.ID())
			s.causes[g.ID()] = "relinquish"
			s.send(c, model.NewReleased(g))
			s.stopGrant(c, g.ID(), "relinquished")
		} else {
			g := active[0]
			s.causes[g.ID()] = "drain"
			s.send(c, model.NewRevoke(g))
		}
	case "reconnect", "quit":
		for id := range c.grants {
			s.causes[id] = kind
		}
		close(c.in)
		c.connected = false
		delay := time.Duration(s.cfg.ReconnectDelay)
		if kind == "quit" {
			identity := string(c.instance.ID())
			s.retired[identity] = true
			// Move logs can precede or follow Assign delivery. Account for
			// every incoming move, including grants the consumer never received.
			for id, transfer := range s.transfers {
				if transfer.To == identity && transfer.Status == "pending" {
					transfer.Status = "interrupted"
					s.causes[id] = "quit"
				}
			}
			delay = replacementDelay
			for id := range c.grants {
				s.stopGrant(c, id, "interrupted")
			}
		}
		s.after(delay, func() {
			if kind == "quit" {
				s.newIdentity(c)
			}
			s.connect(c)
			s.report.Events = append(s.report.Events, LifecycleEvent{At: s.elapsed(), Kind: kind + "_complete", Slot: slot, Consumer: string(c.instance.ID()), Region: c.region, Outcome: "executed"})
		})
	}
	s.report.Events = append(s.report.Events, e)
}

func (s *simulation) send(c *consumer, msg model.ConsumerMessage) {
	if c.connected {
		c.in <- msg
	}
}
