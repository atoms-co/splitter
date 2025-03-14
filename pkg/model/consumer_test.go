package model_test

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/requirex"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/uuidx"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/testing/prefab"
)

var (
	d1 = prefab.QDN("t/s/d1")
	d2 = prefab.QDN("t/s/d2")
)

func TestShardIntersectRange(t *testing.T) {
	a := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	b := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	c := uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc")

	unit1 := model.Shard{Domain: d1, Type: model.Unit}
	unit2 := model.Shard{Domain: d2, Type: model.Unit}

	globalAB := model.Shard{Domain: d1, Type: model.Global, From: model.Key(a), To: model.Key(b)}
	globalBC := model.Shard{Domain: d2, Type: model.Global, From: model.Key(b), To: model.Key(c)}
	globalAC := model.Shard{Domain: d2, Type: model.Global, From: model.Key(a), To: model.Key(c)}

	region1 := model.Region("centralus")
	region2 := model.Region("eastus")
	region1AB := model.Shard{Domain: d1, Type: model.Regional, Region: region1, From: model.Key(a), To: model.Key(b)}
	region1BC := model.Shard{Domain: d2, Type: model.Regional, Region: region1, From: model.Key(b), To: model.Key(c)}
	region1AC := model.Shard{Domain: d2, Type: model.Regional, Region: region1, From: model.Key(a), To: model.Key(c)}
	region2AB := model.Shard{Domain: d2, Type: model.Regional, Region: region2, From: model.Key(a), To: model.Key(b)}
	region2AC := model.Shard{Domain: d2, Type: model.Regional, Region: region2, From: model.Key(a), To: model.Key(c)}

	tests := []struct {
		s1, s2     model.Shard
		intersects bool
	}{
		{unit1, unit2, true},
		{unit1, globalAB, true},
		{unit1, region1AC, true},

		{globalAB, globalBC, false},
		{globalAB, globalAC, true},
		{globalAB, region1AB, true},

		{region1AB, region1BC, false},
		{region1AB, region1AC, true},
		{region1AB, region2AB, false},
		{region1AB, region2AC, false},
	}
	for _, tt := range tests {
		intersects := tt.s1.IntersectsRange(tt.s2)
		if intersects != tt.intersects {
			t.Errorf("Shard1 : %v , Shard2 : %v. Expected %v, got %v.", tt.s1, tt.s2, tt.intersects, intersects)
		}
	}
}

func TestShard_String(t *testing.T) {
	for _, tt := range []struct {
		name  string
		shard model.Shard
		want  string
	}{
		{
			name: "empty min",
			shard: model.Shard{
				Domain: d1,
				Type:   model.Global,
				From:   model.Key(uuidx.Domain.From()),
				To:     model.Key(uuidx.Domain.From()),
			},
			want: "t/s/d1[0000-0000)",
		},
		{
			name: "empty max",
			shard: model.Shard{
				Domain: d1,
				Type:   model.Global,
				From:   model.Key(uuidx.Domain.To()),
				To:     model.Key(uuidx.Domain.To()),
			},
			want: "t/s/d1[ffff-ffff)",
		},
		{
			name: "full",
			shard: model.Shard{
				Domain: d1,
				Type:   model.Global,
				From:   model.Key(uuidx.Domain.From()),
				To:     model.Key(uuidx.Domain.To()),
			},
			want: "t/s/d1[0000-ffff)",
		},
		{
			name: "nil",
			shard: model.Shard{
				Domain: d1,
				Type:   model.Global,
			},
			want: "t/s/d1[0000-0000)",
		},
		{
			name: "global",
			shard: model.Shard{
				Domain: d1,
				Type:   model.Global,
				From:   model.Key(uuid.MustParse("12300000-0000-0000-0000-000000000000")),
				To:     model.Key(uuid.MustParse("45600000-0000-0000-0000-000000000000")),
			},
			want: "t/s/d1[1230-4560)",
		},
		{
			name: "region",
			shard: model.Shard{
				Domain: d1,
				Type:   model.Regional,
				Region: "region1",
				From:   model.Key(uuid.MustParse("12300000-0000-0000-0000-000000000000")),
				To:     model.Key(uuid.MustParse("45600000-0000-0000-0000-000000000000")),
			},
			want: "t/s/d1@region1[1230-4560)",
		},
		{
			name: "unit",
			shard: model.Shard{
				Domain: d1,
				Type:   model.Unit,
			},
			want: "t/s/d1",
		},
		{
			name: "invalid",
			shard: model.Shard{
				Domain: d1,
			},
			want: "invalid-shard",
		},
		{
			name:  "split by 12",
			shard: firstShardOf12Split(t),
			want:  "t/s/d1[0000-1555)",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assertx.Equal(t, tt.want, tt.shard.String())
		})
	}
}

func TestGrantStateCanAdvanceTo(t *testing.T) {
	states := slicex.New(model.AllocatedGrantState, model.LoadedGrantState, model.ActiveGrantState, model.RevokedGrantState, model.UnloadedGrantState)

	for _, from := range states {
		canAdvance := false
		for _, to := range states {
			t.Run(fmt.Sprintf("%v to %v", from, to), func(t *testing.T) {
				requirex.Equal(t, model.GrantStateCanAdvanceTo(from, to), canAdvance)
			})
			if from == to {
				canAdvance = true
			}
		}
	}
}

func firstShardOf12Split(t *testing.T) model.Shard {
	ranges, err := uuidx.Split(uuidx.Domain, 12)
	assert.Nil(t, err)
	return model.Shard{
		Domain: d1,
		Type:   model.Global,
		From:   model.Key(ranges[0].From()),
		To:     model.Key(ranges[0].To()),
	}
}
