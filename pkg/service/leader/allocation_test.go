package leader_test

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"testing"
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
