package cluster

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pb/private"
	"fmt"
	"github.com/hashicorp/raft"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	statsInterval    = 5 * time.Second
	notifyInterval   = 30 * time.Second
	notifyTimeout    = 10 * time.Second
	bootstrapTimeout = 1 * time.Minute
)

var (
	numAppliedIndex = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/cluster/raft_applied_index", "RAFT last index applied to the FSM", core.RaftServerIdKey))
	numLastIndex    = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/cluster/raft_last_index", "RAFT last index in stable storage", core.RaftServerIdKey))
	numLastContact  = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/cluster/raft_last_contact", "RAFT time since last leader contact", core.RaftServerIdKey))
	numState        = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/cluster/raft_state", "RAFT state iota", core.RaftServerIdKey))
)

type Option func(*Cluster)

func WithFastBootstrap(cluster *Cluster) {
	cluster.fastBootstrap = true
}

// Cluster represents a RAFT cluster where initial bootstrapping is determined by a static set of peers
type Cluster struct {
	iox.AsyncCloser

	cl    clock.Clock
	id    raft.ServerID
	addr  raft.ServerAddress
	raft  *raft.Raft
	peers []string

	observer *raft.Observer
	leaderCh <-chan leader.Directive

	fastBootstrap bool
	drain         iox.AsyncCloser

	inject       chan func()
	bootstrapped bool
	notified     map[raft.ServerID]raft.ServerAddress
}

func New(cl clock.Clock, id raft.ServerID, addr raft.ServerAddress, r *raft.Raft, peers []string, port int, opts ...Option) (*Cluster, <-chan leader.Directive) {
	c := &Cluster{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		id:          id,
		addr:        addr,
		raft:        r,
		peers:       peers,
		drain:       iox.NewAsyncCloser(),
		inject:      make(chan func()),
		notified:    make(map[raft.ServerID]raft.ServerAddress),
	}
	for _, fn := range opts {
		fn(c)
	}

	observeCh := make(chan raft.Observation)
	observer := raft.NewObserver(observeCh, true, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	r.RegisterObserver(observer)
	c.observer = observer

	leaderCh := chanx.Map(observeCh, func(o raft.Observation) leader.Directive {
		obs, _ := o.Data.(raft.LeaderObservation)

		log.Debugf(context.Background(), "New leader observation: %v", obs)

		switch obs.LeaderID {
		case "":
			return leader.Directive{
				Type: leader.Disconnect,
			}

		case id:
			return leader.Directive{
				Type: leader.Lead,
				ID:   string(id),
			}

		default:
			host, _, err := net.SplitHostPort(string(obs.LeaderAddr))
			if err != nil {
				log.Errorf(context.Background(), "Internal: unexpected leader %v endpoint: %v", obs.LeaderID, obs.LeaderAddr)

				return leader.Directive{
					Type: leader.Disconnect,
				}
			}

			return leader.Directive{
				Type:     leader.Follow,
				ID:       string(obs.LeaderID),
				Endpoint: fmt.Sprintf("%v:%v", host, port),
			}
		}
	})

	broadcast := chanx.NewBroadcaster[leader.Directive]()
	out1, _ := broadcast.Connect()
	out2, _ := broadcast.Connect()
	c.leaderCh = out2
	broadcast.Forward(context.Background(), leaderCh)

	go c.init(context.Background(), observeCh)

	return c, out1
}

func (c *Cluster) Notify(ctx context.Context, id string, address string) error {
	raftID := raft.ServerID(id)
	raftAddress := raft.ServerAddress(address)

	log.Infof(ctx, "Received notify request: %v@%v", raftID, raftAddress)
	servers, err := syncx.Txn1(ctx, c.txn, func() ([]raft.Server, error) {
		if c.bootstrapped || c.hasLeader() {
			return nil, nil
		}

		if _, ok := c.notified[raftID]; ok {
			return nil, nil
		}

		c.notified[raftID] = raftAddress
		if len(c.notified) < len(c.peers) {
			return nil, nil
		}

		var servers []raft.Server
		for id, addr := range c.notified {
			servers = append(servers, raft.Server{
				ID:      id,
				Address: addr,
			})
		}

		c.bootstrapped = true

		return servers, nil
	})
	if err != nil {
		return err
	}

	// nothing to do
	if servers == nil {
		return nil
	}

	log.Infof(ctx, "Reached expected bootstrap count %d, servers: %v", len(c.peers), servers)
	err = c.raft.BootstrapCluster(raft.Configuration{
		Servers: servers,
	}).Error()
	if err != nil {
		log.Warnf(ctx, "Cluster bootstrap failed: %s", err)
	} else {
		log.Infof(ctx, "Cluster bootstrap successful, servers: %s", servers)
	}
	return nil
}

func (c *Cluster) Info(ctx context.Context) map[string]string {
	return c.raft.Stats()
}

func (c *Cluster) Drain(timeout time.Duration) {
	c.drain.Close()
	c.cl.AfterFunc(timeout, c.Close)
}

func (c *Cluster) init(ctx context.Context, observeCh chan raft.Observation) {
	defer c.Close()
	defer close(observeCh) // closes c.leaderCh

	notifyTicker := c.cl.NewTicker(notifyInterval)
	defer notifyTicker.Stop()

	notifyCutoff := c.cl.Now()
	if !c.fastBootstrap {
		notifyCutoff = notifyCutoff.Add(jitter(notifyInterval))
	}

	// Initial notify
	c.notifyPeers(ctx, notifyCutoff)

notifyLoop:
	for !c.hasLeader() {
		select {
		case <-notifyTicker.C:
			// notify peers of raft address
			c.notifyPeers(ctx, notifyCutoff)
		case <-c.leaderCh:
			break notifyLoop // stop notifying after leader is elected
		case <-c.cl.After(bootstrapTimeout):
			c.shutdownRaft(ctx)
			return
		case fn := <-c.inject:
			fn()
		case <-c.drain.Closed():
			c.shutdownRaft(ctx)
			return
		case <-c.Closed():
			return
		}
	}

	c.process(ctx)
}

func (c *Cluster) notifyPeers(ctx context.Context, notifyCutoff time.Time) {
	if c.cl.Now().Before(notifyCutoff) {
		return
	}
	for _, endpoint := range c.peers {
		go c.notifyPeer(ctx, endpoint)
	}
}

func (c *Cluster) notifyPeer(ctx context.Context, endpoint string) {
	ctx, cancel := context.WithTimeout(ctx, notifyTimeout)
	defer cancel()

	cc, err := grpcx.DialNonBlocking(ctx, endpoint, grpcx.WithInsecure())
	if err != nil {
		log.Warnf(ctx, "Failed to dial peer: %v", err)
		return
	}
	defer cc.Close()

	client := internal_v1.NewClusterServiceClient(cc)

	if _, err := client.Notify(ctx, &internal_v1.ClusterNotifyRequest{
		Id:      string(c.id),
		Address: string(c.addr),
	}); err != nil {
		log.Errorf(ctx, "Notify failed, %v", err)
	}
}

func (c *Cluster) hasLeader() bool {
	addr, id := c.raft.LeaderWithID()
	return addr != "" || id != ""
}

func (c *Cluster) process(ctx context.Context) {
	stats := c.cl.NewTicker(statsInterval)
	defer stats.Stop()

	for {
		select {
		case <-stats.C:
			c.recordMetrics(ctx)
		case fn := <-c.inject:
			fn()
		case <-c.drain.Closed():
			c.shutdownRaft(ctx)
			return
		case <-c.Closed():
			c.shutdownRaft(ctx)
		}
	}
}

func (c *Cluster) recordMetrics(ctx context.Context) {
	numAppliedIndex.Set(ctx, float64(c.raft.AppliedIndex()), core.RaftServerIdTag(c.id))
	numLastIndex.Set(ctx, float64(c.raft.LastIndex()), core.RaftServerIdTag(c.id))
	if c.raft.State() == raft.Follower {
		numLastContact.Set(ctx, c.cl.Since(c.raft.LastContact()).Seconds(), core.RaftServerIdTag(c.id))
	} else {
		numLastContact.Reset(ctx, core.RaftServerIdTag(c.id))
	}
	numState.Set(ctx, float64(c.raft.State()), core.RaftServerIdTag(c.id))
}

func (c *Cluster) shutdownRaft(ctx context.Context) {
	c.raft.DeregisterObserver(c.observer)
	err := c.raft.Shutdown().Error()
	if err != nil {
		log.Errorf(ctx, "Failed to shutdown raft node cleanly: %v", err)
	}
}

// txn runs the given function in the main thread sync. Any signal that triggers a complex action must
// perform I/O or expensive parts outside txn and potentially use multiple txn calls.
func (c *Cluster) txn(ctx context.Context, fn func() error) error {
	var wg sync.WaitGroup
	var err error

	wg.Add(1)
	select {
	case c.inject <- func() {
		defer wg.Done()
		err = fn()
	}:
		wg.Wait()
		return err
	case <-c.Closed():
		return model.ErrDraining
	case <-ctx.Done():
		return model.ErrDraining
	}
}

func jitter(duration time.Duration) time.Duration {
	return duration + time.Duration(rand.Float64()*float64(duration))
}
