package cluster

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"github.com/hashicorp/raft"
	"math/rand"
	"sync"
	"time"
)

const (
	notifyInterval   = 5 * time.Second
	bootstrapTimeout = 1 * time.Minute
)

// Leader connection details
type Leader struct {
	ID   string
	Addr string
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
	leaderCh <-chan *Leader

	drain, initialized iox.AsyncCloser

	inject       chan func()
	bootstrapped bool
	notified     map[raft.ServerID]raft.ServerAddress
}

func New(cl clock.Clock, id raft.ServerID, addr raft.ServerAddress, r *raft.Raft, peers []string) (*Cluster, <-chan *Leader) {
	ret := &Cluster{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		id:          id,
		addr:        addr,
		raft:        r,
		peers:       peers,
		drain:       iox.NewAsyncCloser(),
		initialized: iox.NewAsyncCloser(),
		inject:      make(chan func()),
		notified:    make(map[raft.ServerID]raft.ServerAddress),
	}

	observeCh := make(chan raft.Observation)
	observer := raft.NewObserver(observeCh, true, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	r.RegisterObserver(observer)
	ret.observer = observer

	leaderCh := chanx.Map(observeCh, func(o raft.Observation) *Leader {
		leader, _ := o.Data.(raft.LeaderObservation)
		return &Leader{
			ID:   string(leader.LeaderID),
			Addr: string(leader.LeaderAddr),
		}
	})

	broadcast := chanx.NewBroadcaster[*Leader]()
	out1, _ := broadcast.Connect()
	out2, _ := broadcast.Connect()
	ret.leaderCh = out2
	broadcast.Forward(context.Background(), leaderCh)

	go ret.init(context.Background(), observeCh)

	return ret, out1
}

func (c *Cluster) Notify(ctx context.Context, id string, address string) error {
	raftID := raft.ServerID(id)
	raftAddress := raft.ServerAddress(address)

	log.Infof(ctx, "Received notify request: %v@%v", raftID, raftAddress)
	servers, err := syncx.Txn1(ctx, c.txn, func() ([]raft.Server, error) {
		if c.bootstrapped || c.HasLeader() {
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

func (c *Cluster) HasLeader() bool {
	addr, id := c.raft.LeaderWithID()
	return addr != "" || id != ""
}

func (c *Cluster) Drain(timeout time.Duration) {
	c.drain.Close()
	c.cl.AfterFunc(timeout, c.Close)
}

func (c *Cluster) init(ctx context.Context, observeCh chan raft.Observation) {
	defer c.Close()
	defer close(observeCh) // closes c.leaderCh

	notify := c.cl.NewTicker(notifyInterval + jitter(notifyInterval))

notifyLoop:
	for !c.HasLeader() {
		select {
		case <-notify.C:
			for _, peer := range c.peers {
				go func(peer string) {
					cc, err := grpcx.DialNonBlocking(ctx, peer, grpcx.WithInsecure())
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
				}(peer)
			}
		case <-c.leaderCh:
			// Stop notifying after someone is elected
			break notifyLoop
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

	c.initialized.Close()

	c.process(ctx)
}

func (c *Cluster) process(ctx context.Context) {
	for {
		select {
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
