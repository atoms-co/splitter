package model

import (
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/testing/prefab"
	"github.com/google/uuid"
	"testing"
)

func TestShardIntersectRange(t *testing.T) {
	a := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	b := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	c := uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc")

	d1 := prefab.QDN("t/s/d1")
	d2 := prefab.QDN("t/s/d2")

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
