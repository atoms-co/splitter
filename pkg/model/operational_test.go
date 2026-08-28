package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.atoms.co/lib/testing/assertx"
)

func TestServiceOperational(t *testing.T) {
	op := NewServiceOperational()
	assert.Nil(t, op.BannedRegions())
	assert.Equal(t, LoadBalanceModeEnabled, op.LoadBalanceMode())
	assert.False(t, op.VerboseLogging())

	op2 := NewServiceOperational(
		WithServiceOperationalBannedRegions("eastus2"),
		WithServiceOperationalLoadBalanceMode(LoadBalanceModeDisabled),
		WithServiceOperationalVerboseLogging(true))
	assertx.Equal(t, op2.BannedRegions(), []Region{"eastus2"})
	assert.Equal(t, LoadBalanceModeDisabled, op2.LoadBalanceMode())
	assert.True(t, op2.VerboseLogging())

	op3 := NewServiceOperational(WithServiceOperationalLoadBalanceMode(LoadBalanceModeDisabledDuringDeployment))
	assert.Equal(t, LoadBalanceModeDisabledDuringDeployment, op3.LoadBalanceMode())

	mode, ok := ParseLoadBalanceMode("enabled")
	assert.True(t, ok)
	assert.Equal(t, LoadBalanceModeEnabled, mode)

	mode, ok = ParseLoadBalanceMode("disabled")
	assert.True(t, ok)
	assert.Equal(t, LoadBalanceModeDisabled, mode)

	mode, ok = ParseLoadBalanceMode("disabled-during-deployment")
	assert.True(t, ok)
	assert.Equal(t, LoadBalanceModeDisabledDuringDeployment, mode)
}
