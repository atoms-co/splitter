package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.atoms.co/lib/testing/assertx"
)

func TestServiceOperational(t *testing.T) {
	op := NewServiceOperational()
	assert.Nil(t, op.BannedRegions())
	assert.False(t, op.DisableLoadBalance())
	assert.False(t, op.VerboseLogging())

	op2 := NewServiceOperational(
		WithServiceOperationalBannedRegions("eastus2"),
		WithServiceOperationalDisableLoadBalance(true),
		WithServiceOperationalVerboseLogging(true))
	assertx.Equal(t, op2.BannedRegions(), []Region{"eastus2"})
	assert.True(t, op2.DisableLoadBalance())
	assert.True(t, op2.VerboseLogging())
}
