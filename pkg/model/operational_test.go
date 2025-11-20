package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/splitter/pkg/model"
)

func TestServiceOperational(t *testing.T) {
	op := model.NewServiceOperational()
	assert.Nil(t, op.BannedRegions())
	assert.False(t, op.DisableLoadBalance())
	assert.False(t, op.VerboseLogging())

	op2 := model.NewServiceOperational(
		model.WithServiceOperationalBannedRegions("eastus2"),
		model.WithServiceOperationalDisableLoadBalance(true),
		model.WithServiceOperationalVerboseLogging(true))
	assertx.Equal(t, op2.BannedRegions(), []model.Region{"eastus2"})
	assert.True(t, op2.DisableLoadBalance())
	assert.True(t, op2.VerboseLogging())
}
