package model

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"go.atoms.co/lib/testing/assertx"
)

var (
	A1 = MustParseKey("00000aaa-0000-0000-0000-000000000001")
	A2 = MustParseKey("00000aaa-0000-0000-0000-000000000002")
	A3 = MustParseKey("00000aaa-0000-0000-0000-000000000003")
	B  = MustParseKey("00000bbb-0000-0000-0000-000000000000")
)

func TestRegionProvider(t *testing.T) {
	dist := NewDistribution("foo",
		DistributionSplit{Key: A2, Region: "bar"},
		DistributionSplit{Key: B, Region: "baz"},
	)
	provider := NewRegionProvider(dist)

	tests := []struct {
		key    Key
		region Region
	}{
		{ZeroKey, "foo"},
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
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		name, _ := ParseQualifiedPlacementNameStr("foo/bar")
		initial := NewPlacementInfo(NewPlacement(name, NewDistribution("foo")), 1, time.Now())

		ch := make(chan PlacementInfo, 10)
		provider := NewLiveRegionProvider(ctx, initial, func() (PlacementInfo, error) {
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

		ch <- NewPlacementInfo(NewPlacement(name, NewDistribution("bar")), 3, time.Now())
		time.Sleep(time.Minute)

		assertx.Equal(t, provider.Find(B), "foo")

		// (5) Picked up on next ticker

		time.Sleep(5 * time.Minute)

		assertx.Equal(t, provider.Find(B), "bar")
	})
}
