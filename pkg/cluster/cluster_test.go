package cluster

import (
	"fmt"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"go.atoms.co/lib/testing/assertx"

	"go.atoms.co/splitter/pkg/service/leader"
	raftstorage "go.atoms.co/splitter/pkg/storage/raft"
)

func TestLeaderDirectives_ExistingLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newLeaderTestRaft(t)
		require.NoError(t, assertx.Element(t, d.heartbeat("remote", "remote:50053", 1)))

		_, directives := observeTestLeader(t, d.raft)
		require.Equal(t, leader.Directive{Type: leader.Follow, ID: "remote", Endpoint: "remote:50052"}, assertx.Element(t, directives))
	})
}

func TestLeaderDirectives_ObservationBeforeInitialRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newLeaderTestRaft(t)
		observations := make(chan raft.Observation)
		observer := raft.NewObserver(observations, true, func(o *raft.Observation) bool {
			_, ok := o.Data.(raft.LeaderObservation)
			return ok
		})
		d.raft.RegisterObserver(observer)

		done := d.heartbeat("remote", "remote:50053", 1)
		synctest.Wait() // Raft has updated its leader and is blocked sending the observation.

		c := &cluster{id: "local", raft: d.raft, port: 50052}
		directives := c.leaderDirectives(observations)
		t.Cleanup(func() {
			d.raft.DeregisterObserver(observer)
			close(observations)
			assertx.Closed(t, directives)
		})
		require.Equal(t, leader.Directive{Type: leader.Follow, ID: "remote", Endpoint: "remote:50052"}, assertx.Element(t, directives))
		require.NoError(t, assertx.Element(t, done))
		assertx.NoElement(t, directives)
	})
}

func TestLeaderDirectives_LocalLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newLeaderTestRaft(t)
		require.NoError(t, d.raft.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{{ID: "local", Address: d.addr, Suffrage: raft.Voter}},
		}).Error())
		require.Eventually(t, func() bool {
			_, id := d.raft.LeaderWithID()
			return d.raft.State() == raft.Leader && id == "local"
		}, 3*time.Second, 10*time.Millisecond)

		observations, directives := observeTestLeader(t, d.raft)
		require.Equal(t, leader.Directive{Type: leader.Lead, ID: "local"}, assertx.Element(t, directives))
		observations <- raft.Observation{Data: raft.LeaderObservation{LeaderID: "local", LeaderAddr: d.addr}}
		assertx.NoElement(t, directives)
	})
}

func TestLeaderDirectives_Changes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newLeaderTestRaft(t)
		observations, directives := observeTestLeader(t, d.raft)
		assertx.NoElement(t, directives) // An unknown initial leader must not end bootstrap.

		done := d.heartbeat("remote", "remote:50053", 1)
		require.Equal(t, leader.Directive{Type: leader.Follow, ID: "remote", Endpoint: "remote:50052"}, assertx.Element(t, directives))
		require.NoError(t, assertx.Element(t, done))

		observations <- raft.Observation{Data: raft.LeaderObservation{LeaderID: "remote", LeaderAddr: "remote:50053"}}
		assertx.NoElement(t, directives)

		// A new term clears the leader before learning it again, even for the same ID.
		done = d.heartbeat("remote", "remote:50053", 2)
		require.Equal(t, leader.Directive{Type: leader.Disconnect}, assertx.Element(t, directives))
		require.Equal(t, leader.Directive{Type: leader.Follow, ID: "remote", Endpoint: "remote:50052"}, assertx.Element(t, directives))
		require.NoError(t, assertx.Element(t, done))

		done = d.heartbeat("remote", "new-endpoint:50053", 2)
		require.Equal(t, leader.Directive{Type: leader.Follow, ID: "remote", Endpoint: "new-endpoint:50052"}, assertx.Element(t, directives))
		require.NoError(t, assertx.Element(t, done))

		done = d.heartbeat("other", "other:50053", 3)
		require.Equal(t, leader.Directive{Type: leader.Disconnect}, assertx.Element(t, directives))
		require.Equal(t, leader.Directive{Type: leader.Follow, ID: "other", Endpoint: "other:50052"}, assertx.Element(t, directives))
		require.NoError(t, assertx.Element(t, done))
	})
}

func TestLeaderDirectives_Observations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newLeaderTestRaft(t)
		require.NoError(t, assertx.Element(t, d.heartbeat("current", "current:50053", 1)))
		observations, directives := observeTestLeader(t, d.raft)
		require.Equal(t, leader.Directive{Type: leader.Follow, ID: "current", Endpoint: "current:50052"}, assertx.Element(t, directives))

		// Apply the loss even though Raft's current state still names the same leader.
		observations <- raft.Observation{Data: raft.LeaderObservation{}}
		require.Equal(t, leader.Directive{Type: leader.Disconnect}, assertx.Element(t, directives))
		assertx.NoElement(t, directives)

		observations <- raft.Observation{Data: raft.LeaderObservation{LeaderID: "current", LeaderAddr: "current:50053"}}
		require.Equal(t, leader.Directive{Type: leader.Follow, ID: "current", Endpoint: "current:50052"}, assertx.Element(t, directives))

		observations <- raft.Observation{Data: raft.LeaderObservation{LeaderID: "next", LeaderAddr: "next:50053"}}
		require.Equal(t, leader.Directive{Type: leader.Follow, ID: "next", Endpoint: "next:50052"}, assertx.Element(t, directives))
		assertx.NoElement(t, directives)
	})
}

func TestLeaderDirectives_ChangesDuringInitialization(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newLeaderTestRaft(t)
		require.NoError(t, assertx.Element(t, d.heartbeat("first", "first:50053", 1)))
		_, directives := observeTestLeader(t, d.raft)
		synctest.Wait() // Initial state has been read; its delivery is still blocked.

		done := d.heartbeat("second", "second:50053", 2)
		synctest.Wait()
		require.Equal(t, leader.Directive{Type: leader.Follow, ID: "first", Endpoint: "first:50052"}, assertx.Element(t, directives))
		require.Equal(t, leader.Directive{Type: leader.Disconnect}, assertx.Element(t, directives))
		require.Equal(t, leader.Directive{Type: leader.Follow, ID: "second", Endpoint: "second:50052"}, assertx.Element(t, directives))
		require.NoError(t, assertx.Element(t, done))
		assertx.NoElement(t, directives)
	})
}

func TestLeaderDirectives_CloseDuringInitialization(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newLeaderTestRaft(t)
		require.NoError(t, assertx.Element(t, d.heartbeat("remote", "remote:50053", 1)))
		observeTestLeader(t, d.raft)
		synctest.Wait()
		// Cleanup closes the input and verifies that the pending output drains and closes.
	})
}

func TestLeaderDirective(t *testing.T) {
	c := &cluster{id: "local", port: 50052}
	for _, tt := range []struct {
		name string
		id   raft.ServerID
		addr raft.ServerAddress
		want leader.Directive
	}{
		{name: "unknown", want: leader.Directive{Type: leader.Disconnect}},
		{name: "local", id: "local", addr: "local:50053", want: leader.Directive{Type: leader.Lead, ID: "local"}},
		{name: "remote", id: "remote", addr: "remote:50053", want: leader.Directive{Type: leader.Follow, ID: "remote", Endpoint: "remote:50052"}},
		{name: "invalid address", id: "remote", addr: "invalid", want: leader.Directive{Type: leader.Disconnect}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, c.leaderDirective(tt.id, tt.addr))
		})
	}
}

type leaderTestRaft struct {
	raft   *raft.Raft
	addr   raft.ServerAddress
	sender *raft.InmemTransport
}

func newLeaderTestRaft(t *testing.T) *leaderTestRaft {
	t.Helper()

	addr, transport := raft.NewInmemTransport("local:50053")
	_, sender := raft.NewInmemTransport("remote:50053")
	sender.Connect(addr, transport)
	t.Cleanup(func() {
		require.NoError(t, sender.Close())
		require.NoError(t, transport.Close())
	})

	config := raft.DefaultConfig()
	config.LocalID = "local"
	config.LogOutput = io.Discard
	store := raft.NewInmemStore()
	r, err := raft.NewRaft(config, raftstorage.NewFSM(), store, store, raft.NewInmemSnapshotStore(), transport)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Shutdown().Error()) })

	return &leaderTestRaft{raft: r, addr: addr, sender: sender}
}

func (d *leaderTestRaft) heartbeat(id raft.ServerID, addr raft.ServerAddress, term uint64) <-chan error {
	done := make(chan error, 1)
	go func() {
		request := &raft.AppendEntriesRequest{
			RPCHeader: raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax, ID: []byte(id), Addr: []byte(addr)},
			Term:      term,
		}
		var response raft.AppendEntriesResponse
		err := d.sender.AppendEntries("local", d.addr, request, &response)
		if err == nil && !response.Success {
			err = fmt.Errorf("AppendEntries rejected: %+v", response)
		}
		done <- err
	}()
	return done
}

func observeTestLeader(t *testing.T, r *raft.Raft) (chan raft.Observation, <-chan leader.Directive) {
	t.Helper()

	observations := make(chan raft.Observation)
	observer := raft.NewObserver(observations, true, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	r.RegisterObserver(observer)
	c := &cluster{id: "local", raft: r, port: 50052}
	directives := c.leaderDirectives(observations)
	t.Cleanup(func() {
		r.DeregisterObserver(observer)
		close(observations)
		assertx.Closed(t, directives)
	})
	return observations, directives
}
