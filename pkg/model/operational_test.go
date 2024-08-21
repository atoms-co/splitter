package model_test

import (
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/splitter/pkg/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestServiceOperational(t *testing.T) {
	op := model.NewServiceOperational()
	assert.Nil(t, op.BannedRegions())
	assert.False(t, op.DisableLoadBalance())

	op2 := model.NewServiceOperational(
		model.WithServiceOperationalBannedRegions("eastus2"),
		model.WithServiceOperationalDisableLoadBalance(true))
	assertx.Equal(t, op2.BannedRegions(), []model.Region{"eastus2"})
	assert.True(t, op2.DisableLoadBalance())
}
