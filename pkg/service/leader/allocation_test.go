package leader_test

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestHasRegionAffinity(t *testing.T) {
	n1us := location.New("us", "n1")
	n2us := location.New("us", "n2")
	n1eu := location.New("eu", "n1")
	n3eu := location.New("eu", "n3")
	none := location.Location{}

	tests := []struct {
		worker, work location.Location
		expected     bool
	}{
		{n1us, n1us, true},
		{n1us, n2us, true},
		{n2us, n1us, true},

		{n1us, n1eu, false},
		{n1eu, n1us, false},
		{n1us, n3eu, false},

		{none, none, true},  // if the work has no region, no penalty ..
		{n1us, none, true},  // .. even if the worker has one, ..
		{none, n1us, false}, // .. but a region-less worker incurs it if the work has a region
	}

	for _, tt := range tests {
		worker := leader.Worker{ID: "id", Data: model.NewInstance(location.NewInstance(tt.worker), "")}
		work := leader.Work{Unit: model.QualifiedServiceName{Tenant: "t", Service: "s"}, Data: tt.work}

		assertx.Equal(t, leader.HasRegionAffinity(worker, work), tt.expected)
	}
}

func TestControl(t *testing.T) {
	t1, err := model.NewTenant("tenant1", time.Time{}, model.WithTenantConfig(
		model.NewTenantConfig(model.WithTenantBannedRegions("centralus")),
	))
	require.NoError(t, err)

	t2, err := model.NewTenant("tenant2", time.Time{}, model.WithTenantConfig(
		model.NewTenantConfig(model.WithTenantBannedRegions("northcentralus", "eastus2")),
	))
	require.NoError(t, err)

	s1, err := model.NewService(model.QualifiedServiceName{Tenant: t1.Name(), Service: "service1"}, time.Time{})
	require.NoError(t, err)

	s2, err := model.NewService(model.QualifiedServiceName{Tenant: t1.Name(), Service: "service2"}, time.Time{}, model.WithServiceConfig(
		model.NewServiceConfig(model.WithServiceBannedRegions("northcentralus"))),
	)
	require.NoError(t, err)

	s3, err := model.NewService(model.QualifiedServiceName{Tenant: t2.Name(), Service: "service3"}, time.Time{})
	require.NoError(t, err)

	st1 := core.NewState(model.NewTenantInfo(t1, 1, time.Time{}), []model.ServiceInfoEx{
		model.NewServiceInfoEx(model.NewServiceInfo(s1, 1, time.Time{}), nil),
		model.NewServiceInfoEx(model.NewServiceInfo(s2, 1, time.Time{}), nil),
	}, nil)
	st2 := core.NewState(model.NewTenantInfo(t2, 1, time.Time{}), []model.ServiceInfoEx{
		model.NewServiceInfoEx(model.NewServiceInfo(s3, 1, time.Time{}), nil),
	}, nil)

	ctrl := leader.NewControl(core.NewSnapshot(st1, st2))

	w1 := allocation.NewWorker[location.InstanceID, model.Instance](
		"worker1",
		model.NewInstance(location.NewInstance(location.New("centralus", "unknown")), ""),
	)

	w2 := allocation.NewWorker[location.InstanceID, model.Instance](
		"worker2",
		model.NewInstance(location.NewInstance(location.New("northcentralus", "unknown")), ""),
	)

	w3 := allocation.NewWorker[location.InstanceID, model.Instance](
		"worker3",
		model.NewInstance(location.NewInstance(location.New("eastus2", "unknown")), ""),
	)

	_, ok := ctrl.TryPlace(w1, allocation.Work[model.QualifiedServiceName, location.Location]{Unit: s1.Name()})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w1, allocation.Work[model.QualifiedServiceName, location.Location]{Unit: s2.Name()})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w1, allocation.Work[model.QualifiedServiceName, location.Location]{Unit: s3.Name()})
	assert.True(t, ok)

	_, ok = ctrl.TryPlace(w2, allocation.Work[model.QualifiedServiceName, location.Location]{Unit: s1.Name()})
	assert.True(t, ok)

	_, ok = ctrl.TryPlace(w2, allocation.Work[model.QualifiedServiceName, location.Location]{Unit: s2.Name()})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w2, allocation.Work[model.QualifiedServiceName, location.Location]{Unit: s3.Name()})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w3, allocation.Work[model.QualifiedServiceName, location.Location]{Unit: s1.Name()})
	assert.True(t, ok)

	_, ok = ctrl.TryPlace(w3, allocation.Work[model.QualifiedServiceName, location.Location]{Unit: s2.Name()})
	assert.True(t, ok)

	_, ok = ctrl.TryPlace(w3, allocation.Work[model.QualifiedServiceName, location.Location]{Unit: s3.Name()})
	assert.False(t, ok)
}
