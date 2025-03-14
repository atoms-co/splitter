package model_test

import (
	"context"
	"testing"
	"time"

	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/splitter/pkg/model"
)

var (
	A1 = model.MustParseKey("00000aaa-0000-0000-0000-000000000001")
	A2 = model.MustParseKey("00000aaa-0000-0000-0000-000000000002")
	A3 = model.MustParseKey("00000aaa-0000-0000-0000-000000000003")
	B  = model.MustParseKey("00000bbb-0000-0000-0000-000000000000")
)

func TestRegionProvider(t *testing.T) {
	dist := model.NewDistribution("foo",
		model.DistributionSplit{Key: A2, Region: "bar"},
		model.DistributionSplit{Key: B, Region: "baz"},
	)
	provider := model.NewRegionProvider(dist)

	tests := []struct {
		key    model.Key
		region model.Region
	}{
		{model.ZeroKey, "foo"},
		{A1, "foo"},
		{A2, "bar"},
		{A3, "bar"},
		{B, "baz"},
	}

	for _, tt := range tests {
		assertx.Equal(t, provider.Find(tt.key), tt.region)
	}
}

func TestLiveRegionProvider(t *testing.T) {
	ctx := context.Background()

	cl := mockclock.NewUnsynchronized()
	cl.Set(time.Now())

	name, _ := model.ParseQualifiedPlacementNameStr("foo/bar")
	initial := model.NewPlacementInfo(model.NewPlacement(name, model.NewDistribution("foo")), 1, cl.Now())

	ch := make(chan model.PlacementInfo, 10)
	provider := model.NewLiveRegionProvider(ctx, cl, initial, func() (model.PlacementInfo, error) {
		return <-ch, nil
	})

	// (1) Initial

	assertx.Equal(t, provider.Find(B), "foo")

	cl.Add(time.Minute)
	time.Sleep(50 * time.Millisecond)

	// (2) Still initial

	assertx.Equal(t, provider.Find(B), "foo")

	ch <- initial
	cl.Add(5 * time.Minute)
	time.Sleep(50 * time.Millisecond)

	// (3) No-op update. Still initial

	assertx.Equal(t, provider.Find(B), "foo")

	// (4) Placement change, but not yet picked up

	ch <- model.NewPlacementInfo(model.NewPlacement(name, model.NewDistribution("bar")), 3, cl.Now())
	cl.Add(time.Minute)
	time.Sleep(50 * time.Millisecond)

	assertx.Equal(t, provider.Find(B), "foo")

	// (5) Picked up on next ticker

	cl.Add(5 * time.Minute)
	time.Sleep(50 * time.Millisecond)

	assertx.Equal(t, provider.Find(B), "bar")
}
