package model_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"go.atoms.co/lib/testing/assertx"
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
	synctest.Run(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		name, _ := model.ParseQualifiedPlacementNameStr("foo/bar")
		initial := model.NewPlacementInfo(model.NewPlacement(name, model.NewDistribution("foo")), 1, time.Now())

		ch := make(chan model.PlacementInfo, 10)
		provider := model.NewLiveRegionProvider(ctx, initial, func() (model.PlacementInfo, error) {
			return <-ch, nil
		})

		// (1) Initial

		assertx.Equal(t, provider.Find(B), "foo")

		time.Sleep(time.Minute)

		// (2) Still initial

		assertx.Equal(t, provider.Find(B), "foo")

		ch <- initial
		time.Sleep(5 * time.Minute)

		// (3) No-op update. Still initial

		assertx.Equal(t, provider.Find(B), "foo")

		// (4) Placement change, but not yet picked up

		ch <- model.NewPlacementInfo(model.NewPlacement(name, model.NewDistribution("bar")), 3, time.Now())
		time.Sleep(time.Minute)

		assertx.Equal(t, provider.Find(B), "foo")

		// (5) Picked up on next ticker

		time.Sleep(5 * time.Minute)

		assertx.Equal(t, provider.Find(B), "bar")
	})
}
