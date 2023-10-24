package leader_test

import (
	"context"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pkg/service/worker"
	"go.atoms.co/splitter/pkg/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const (
	tenant1 model.TenantName = "tenant1"
)

func TestLeader(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()
	loc := location.New("centralus", "splitter-0")
	db := memory.New()

	tenant, err := model.NewTenant(tenant1, cl.Now())
	require.NoError(t, err)

	err = db.Update(ctx, core.NewTenantUpdate(model.NewTenantInfo(tenant, 1, cl.Now())))
	require.NoError(t, err)

	l := leader.New(ctx, cl, loc, db, leader.WithFastActivation())
	<-l.Initialized().Closed()

	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")

	in := make(chan leader.Message)

	out, err := l.Join(ctx, session.NewID(), w, nil, in)
	require.NoError(t, err, "worker failed to join leader")

	assign := readFn(t, out, isAssign)
	assert.Equal(t, tenant1, assign.Grant().Tenant())

	l.Close()
}

func isAssign(msg leader.Message) (worker.Assign, bool) {
	if msg.IsWorkerMessage() {
		w, _ := msg.WorkerMessage()
		return w.Assign()
	}
	return worker.Assign{}, false
}
func readFn[T any](t *testing.T, in <-chan leader.Message, fn func(message leader.Message) (T, bool)) T {
	t.Helper()
	for {
		select {
		case msg := <-in:
			if transform, ok := fn(msg); ok {
				return transform
			}
			continue
		case <-time.After(1 * time.Second):
			var transform T
			t.Fatal("no message read")
			return transform
		}
	}
}
