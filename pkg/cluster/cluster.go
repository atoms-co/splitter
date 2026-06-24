package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"

	"go.atoms.co/iox"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/syncx"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
)

const (
	statsInterval    = 5 * time.Second
	notifyInterval   = 30 * time.Second
	notifyTimeout    = 10 * time.Second
	bootstrapTimeout = 1 * time.Minute
	forwardTimeout   = 35 * time.Second
)

var (
	numAppliedIndex = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/cluster/raft_applied_index", "RAFT last index applied to the FSM", core.RaftServerIdKey))
	numLastIndex    = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/cluster/raft_last_index", "RAFT last index in stable storage", core.RaftServerIdKey))
	numLastContact  = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/cluster/raft_last_contact", "RAFT time since last leader contact", core.RaftServerIdKey))
	numState        = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/cluster/raft_state", "RAFT state iota", core.RaftServerIdKey))
)

type Option func(*cluster)

func WithFastBootstrap() Option {
	return func(cluster *cluster) {
		cluster.fastBootstrap = true
	}
}

// Cluster represents a RAFT cluster where initial bootstrapping is determined by a static set of peers
type Cluster interface {
	iox.RAsyncCloser

	Notify(ctx context.Context, id string, address string) error
	AddNode(ctx context.Context, id string, address string) error
	RemoveNode(ctx context.Context, id string) error
	Info(ctx context.Context) map[string]string
	Drain(timeout time.Duration)
}

type cluster struct {
	iox.AsyncCloser

	id       raft.ServerID
	addr     raft.ServerAddress
	raft     *raft.Raft
	ldb, sdb *boltdb.BoltStore
	trans    *raft.NetworkTransport
	peers    []string
	port     int

	observer *raft.Observer
	leaderCh <-chan leader.Directive

	fastBootstrap bool
	joiningNode   bool
	drain         iox.AsyncCloser

	inject       chan func()
	bootstrapped bool
	notified     map[raft.ServerID]raft.ServerAddress
}

func New(id raft.ServerID, addr raft.ServerAddress, r *raft.Raft, ldb, sdb *boltdb.BoltStore, trans *raft.NetworkTransport, peers []string, port int, opts ...Option) (Cluster, <-chan leader.Directive) {
	c := &cluster{
		AsyncCloser: iox.NewAsyncCloser(),
		id:          id,
		addr:        addr,
		raft:        r,
		ldb:         ldb,
		sdb:         sdb,
		trans:       trans,
		peers:       peers,
		port:        port,
		drain:       iox.NewAsyncCloser(),
		inject:      make(chan func()),
		notified:    make(map[raft.ServerID]raft.ServerAddress),
	}
	for _, fn := range opts {
		fn(c)
	}
	c.joiningNode = !c.isBootstrappedPeer(string(c.id))

	observeCh := make(chan raft.Observation)
	observer := raft.NewObserver(observeCh, true, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	r.RegisterObserver(observer)
	c.observer = observer

	leaderCh := chanx.Map(observeCh, func(o raft.Observation) leader.Directive {
		obs, _ := o.Data.(raft.LeaderObservation)

		log.Infof(context.Background(), "New leader observation: %v", obs)

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

func (c *cluster) Notify(ctx context.Context, id string, address string) error {
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

// AddNode handles an incoming add request from a new node. If the current node is the RAFT leader, it adds the
// voter. Otherwise, it forwards the request to the current leader.
func (c *cluster) AddNode(ctx context.Context, id string, address string) error {
	log.Infof(ctx, "Received add request: %v@%v", id, address)

	leaderAddr, leaderID := c.raft.LeaderWithID()
	if leaderID == "" {
		return fmt.Errorf("no leader available")
	}

	if leaderID != c.id {
		return c.forwardToLeader(ctx, leaderAddr, func(ctx context.Context, client splitterprivatepb.ClusterServiceClient) error {
			_, err := client.AddNode(ctx, &splitterprivatepb.ClusterAddNodeRequest{Id: id, Address: address})
			return err
		})
	}

	future := c.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, 30*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter %v: %w", id, err)
	}

	log.Infof(ctx, "Successfully added voter %v@%v", id, address)
	return nil
}

// RemoveNode handles an incoming remove request. If this node is the RAFT leader, it removes the
// node. Otherwise, it forwards the request to the current leader.
func (c *cluster) RemoveNode(ctx context.Context, id string) error {
	log.Infof(ctx, "Received remove request: %v", id)

	if c.isBootstrappedPeer(id) {
		return fmt.Errorf("cannot remove bootstrap peer %v", id)
	}

	leaderAddr, leaderID := c.raft.LeaderWithID()
	if leaderID == "" {
		return fmt.Errorf("no leader available")
	}

	if leaderID != c.id {
		return c.forwardToLeader(ctx, leaderAddr, func(ctx context.Context, client splitterprivatepb.ClusterServiceClient) error {
			_, err := client.RemoveNode(ctx, &splitterprivatepb.ClusterRemoveNodeRequest{Id: id})
			return err
		})
	}

	if id == string(c.id) {
		log.Infof(ctx, "Leader is leaving, transferring leadership")
		if err := c.raft.LeadershipTransfer().Error(); err != nil {
			return fmt.Errorf("failed to transfer leadership: %w", err)
		}

		return raft.ErrNotLeader
	}

	future := c.raft.RemoveServer(raft.ServerID(id), 0, 30*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to remove server %v: %w", id, err)
	}

	log.Infof(ctx, "Successfully removed server %v", id)
	return nil
}

// forwardToLeader forwards a request to the current RAFT leader.
func (c *cluster) forwardToLeader(ctx context.Context, leaderAddr raft.ServerAddress, fn func(context.Context, splitterprivatepb.ClusterServiceClient) error) error {
	host, _, err := net.SplitHostPort(string(leaderAddr))
	if err != nil {
		return fmt.Errorf("unexpected leader address %v: %w", leaderAddr, err)
	}
	endpoint := fmt.Sprintf("%v:%v", host, c.port)

	ctx, cancel := context.WithTimeout(ctx, forwardTimeout)
	defer cancel()

	cc, err := grpcx.DialNonBlocking(ctx, endpoint, grpcx.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to dial leader %v: %w", endpoint, err)
	}
	defer func() { _ = cc.Close() }()

	log.Infof(ctx, "Not leader, forwarding to leader %v", endpoint)
	return fn(ctx, splitterprivatepb.NewClusterServiceClient(cc))
}

func (c *cluster) Info(ctx context.Context) map[string]string {
	return c.raft.Stats()
}

func (c *cluster) Drain(timeout time.Duration) {
	c.drain.Close()
	time.AfterFunc(timeout, c.Close)
}

func (c *cluster) init(ctx context.Context, observeCh chan raft.Observation) {
	defer c.Close()
	defer close(observeCh) // closes c.leaderCh

	if c.joiningNode {
		// Joining node: RAFT is running but this node may not be part of the cluster yet.
		// An operator must call the Join RPC on an existing peer to add it.
		// On restart, RAFT reconnects automatically via TCP transport.
		log.Infof(ctx, "Node %v is not a bootstrap peer, waiting for manual join or reconnect", c.id)
		c.process(ctx)
	} else {
		c.bootstrapCluster(ctx)
	}
}

func (c *cluster) bootstrapCluster(ctx context.Context) {
	notifyTicker := time.NewTicker(notifyInterval)
	defer notifyTicker.Stop()

	notifyCutoff := time.Now()
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
		case <-time.After(bootstrapTimeout):
			if err := c.shutdownRaft(ctx); err != nil {
				log.Errorf(ctx, "Failed to shutdown raft: %v", err)
			}
			return
		case fn := <-c.inject:
			fn()
		case <-c.drain.Closed():
			if err := c.shutdownRaft(ctx); err != nil {
				log.Errorf(ctx, "Failed to shutdown raft: %v", err)
			}
			return
		case <-c.Closed():
			return
		}
	}

	c.process(ctx)
}

func (c *cluster) notifyPeers(ctx context.Context, notifyCutoff time.Time) {
	if time.Now().Before(notifyCutoff) {
		return
	}
	for _, endpoint := range c.peers {
		go c.notifyPeer(ctx, endpoint)
	}
}

func (c *cluster) notifyPeer(ctx context.Context, endpoint string) {
	ctx, cancel := context.WithTimeout(ctx, notifyTimeout)
	defer cancel()

	cc, err := grpcx.DialNonBlocking(ctx, endpoint, grpcx.WithInsecure())
	if err != nil {
		log.Warnf(ctx, "Failed to dial peer: %v", err)
		return
	}
	defer cc.Close()

	client := splitterprivatepb.NewClusterServiceClient(cc)

	if _, err := client.Notify(ctx, &splitterprivatepb.ClusterNotifyRequest{
		Id:      string(c.id),
		Address: string(c.addr),
	}); err != nil {
		log.Errorf(ctx, "Notify failed, %v", err)
	}
}

func (c *cluster) hasLeader() bool {
	addr, id := c.raft.LeaderWithID()
	return addr != "" || id != ""
}

func (c *cluster) process(ctx context.Context) {
	stats := time.NewTicker(statsInterval)
	defer stats.Stop()

	for {
		select {
		case <-stats.C:
			c.recordMetrics(ctx)
		case fn := <-c.inject:
			fn()
		case <-c.drain.Closed():
			if err := c.shutdownRaft(ctx); err != nil {
				log.Errorf(ctx, "Failed to shutdown raft: %v", err)
			}
			return
		case <-c.Closed():
			if err := c.shutdownRaft(ctx); err != nil {
				log.Errorf(ctx, "Failed to shutdown raft: %v", err)
			}
		}
	}
}

func (c *cluster) recordMetrics(ctx context.Context) {
	numAppliedIndex.Set(ctx, float64(c.raft.AppliedIndex()), core.RaftServerIdTag(c.id))
	numLastIndex.Set(ctx, float64(c.raft.LastIndex()), core.RaftServerIdTag(c.id))
	if c.raft.State() == raft.Follower {
		numLastContact.Set(ctx, time.Since(c.raft.LastContact()).Seconds(), core.RaftServerIdTag(c.id))
	} else {
		numLastContact.Reset(ctx, core.RaftServerIdTag(c.id))
	}
	numState.Set(ctx, float64(c.raft.State()), core.RaftServerIdTag(c.id))
}

func (c *cluster) shutdownRaft(ctx context.Context) error {
	c.raft.DeregisterObserver(c.observer)
	err := c.raft.Shutdown().Error()
	if err != nil {
		return fmt.Errorf("failed to shutdown raft node cleanly: %w", err)
	}
	if err := c.trans.Close(); err != nil {
		return fmt.Errorf("failed to shutdown raft transport cleanly: %w", err)
	}
	// Only shutdown Bolt and SQLite when Raft is done.
	if err := c.ldb.Close(); err != nil {
		return fmt.Errorf("failed to shutdown raft log store cleanly: %w", err)
	}
	if err := c.sdb.Close(); err != nil {
		return fmt.Errorf("failed to shutdown raft stable store cleanly: %w", err)
	}
	return nil
}

// isBootstrappedPeer returns true if the given raft ID matches one of the peers in the static peer list.
// Assumes peers are DNS names of the form <raft-id>.<domain>:<port> and that --raft_id equals the
// pod HOSTNAME. These assumptions hold for the existing bootstrap flow.
func (c *cluster) isBootstrappedPeer(id string) bool {
	return slices.ContainsFunc(c.peers, func(peer string) bool {
		return strings.HasPrefix(peer, id+".") || peer == id
	})
}

// txn runs the given function in the main thread sync. Any signal that triggers a complex action must
// perform I/O or expensive parts outside txn and potentially use multiple txn calls.
func (c *cluster) txn(ctx context.Context, fn func() error) error {
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
