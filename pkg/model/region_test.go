package model_test

import (
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/splitter/pkg/model"
	"testing"
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
