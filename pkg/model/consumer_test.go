package model

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/requirex"
	"go.atoms.co/lib/uuidx"
	"go.atoms.co/slicex"
)

var (
	d1 = qdn("t/s/d1")
	d2 = qdn("t/s/d2")
)

func TestShardIntersectRange(t *testing.T) {
	a := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	b := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	c := uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc")

	unit1 := Shard{Domain: d1, Type: Unit}
	unit2 := Shard{Domain: d2, Type: Unit}

	globalAB := Shard{Domain: d1, Type: Global, From: Key(a), To: Key(b)}
	globalBC := Shard{Domain: d2, Type: Global, From: Key(b), To: Key(c)}
	globalAC := Shard{Domain: d2, Type: Global, From: Key(a), To: Key(c)}

	region1 := Region("centralus")
	region2 := Region("eastus")
	region1AB := Shard{Domain: d1, Type: Regional, Region: region1, From: Key(a), To: Key(b)}
	region1BC := Shard{Domain: d2, Type: Regional, Region: region1, From: Key(b), To: Key(c)}
	region1AC := Shard{Domain: d2, Type: Regional, Region: region1, From: Key(a), To: Key(c)}
	region2AB := Shard{Domain: d2, Type: Regional, Region: region2, From: Key(a), To: Key(b)}
	region2AC := Shard{Domain: d2, Type: Regional, Region: region2, From: Key(a), To: Key(c)}

	tests := []struct {
		s1, s2     Shard
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
		shard Shard
		want  string
	}{
		{
			name: "empty min",
			shard: Shard{
				Domain: d1,
				Type:   Global,
				From:   Key(uuidx.Domain.From()),
				To:     Key(uuidx.Domain.From()),
			},
			want: "t/s/d1[0000-0000)",
		},
		{
			name: "empty max",
			shard: Shard{
				Domain: d1,
				Type:   Global,
				From:   Key(uuidx.Domain.To()),
				To:     Key(uuidx.Domain.To()),
			},
			want: "t/s/d1[ffff-ffff)",
		},
		{
			name: "full",
			shard: Shard{
				Domain: d1,
				Type:   Global,
				From:   Key(uuidx.Domain.From()),
				To:     Key(uuidx.Domain.To()),
			},
			want: "t/s/d1[0000-ffff)",
		},
		{
			name: "nil",
			shard: Shard{
				Domain: d1,
				Type:   Global,
			},
			want: "t/s/d1[0000-0000)",
		},
		{
			name: "global",
			shard: Shard{
				Domain: d1,
				Type:   Global,
				From:   Key(uuid.MustParse("12300000-0000-0000-0000-000000000000")),
				To:     Key(uuid.MustParse("45600000-0000-0000-0000-000000000000")),
			},
			want: "t/s/d1[1230-4560)",
		},
		{
			name: "region",
			shard: Shard{
				Domain: d1,
				Type:   Regional,
				Region: "region1",
				From:   Key(uuid.MustParse("12300000-0000-0000-0000-000000000000")),
				To:     Key(uuid.MustParse("45600000-0000-0000-0000-000000000000")),
			},
			want: "t/s/d1@region1[1230-4560)",
		},
		{
			name: "unit",
			shard: Shard{
				Domain: d1,
				Type:   Unit,
			},
			want: "t/s/d1",
		},
		{
			name: "invalid",
			shard: Shard{
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
	states := slicex.New(AllocatedGrantState, LoadedGrantState, ActiveGrantState, RevokedGrantState, UnloadedGrantState)

	for _, from := range states {
		canAdvance := false
		for _, to := range states {
			t.Run(fmt.Sprintf("%v to %v", from, to), func(t *testing.T) {
				requirex.Equal(t, GrantStateCanAdvanceTo(from, to), canAdvance)
			})
			if from == to {
				canAdvance = true
			}
		}
	}
}

func firstShardOf12Split(t *testing.T) Shard {
	ranges, err := uuidx.Split(uuidx.Domain, 12)
	assert.Nil(t, err)
	return Shard{
		Domain: d1,
		Type:   Global,
		From:   Key(ranges[0].From()),
		To:     Key(ranges[0].To()),
	}
}
