package core_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.atoms.co/splitter/pkg/core"
)

func TestParseBlockDistributionStr(t *testing.T) {
	tests := []struct {
		str      string
		expected core.BlockDistribution
	}{
		{"foo", core.NewBlockDistribution("foo")},
		{"foo/1:bar", core.NewBlockDistribution("foo", core.BlockDistributionSplit{Block: 1, Region: "bar"})},
		{"foo/23:bar", core.NewBlockDistribution("foo", core.BlockDistributionSplit{Block: 23, Region: "bar"})},
		{"foo/1023:bar", core.NewBlockDistribution("foo", core.BlockDistributionSplit{Block: 1023, Region: "bar"})},
		{"foo/23:bar/42:baz", core.NewBlockDistribution("foo", core.BlockDistributionSplit{Block: 23, Region: "bar"}, core.BlockDistributionSplit{Block: 42, Region: "baz"})},
	}

	for _, tt := range tests {
		actual, err := core.ParseBlockDistributionStr(tt.str)
		require.NoError(t, err, tt.str)
		assert.True(t, actual.Equals(tt.expected), "parse(%v)=%v, want %v", tt.str, actual, tt.expected)
	}
}

func TestParseBlockDistributionStr_Invalid(t *testing.T) {
	tests := []string{
		"",
		"foo/bar",
		"foo/32",
		"foo/1:bar/1:baz",
		"foo/1:/2:baz",
		"foo/42:bar/23:baz",
	}

	for _, tt := range tests {
		actual, err := core.ParseBlockDistributionStr(tt)
		require.Error(t, err, tt, actual)
	}
}

func TestMoveBlockDistribution(t *testing.T) {
	tests := []struct {
		current, target string
		n               int
		expected        string
	}{
		{"foo", "foo", 1, "foo"},
		{"foo", "bar", 1, "foo/1023:bar"},
		{"foo", "bar", 24, "foo/1000:bar"},
		{"foo", "bar", 1024, "bar"},
		{"foo/20:baz", "bar/20:baz", 1, "foo/19:bar/20:baz"},
		{"foo/20:baz", "bar/20:baz", 10, "foo/10:bar/20:baz"},
		{"foo/20:baz", "bar/20:baz", 20, "bar/20:baz"},
		{"foo/20:bar", "foo/15:bar", 1, "foo/19:bar"},
		{"foo/20:bar", "foo/25:bar", 1, "foo/20:bar/24:foo/25:bar"},
		{"foo/20:bar", "foo/25:bar", 2, "foo/20:bar/23:foo/25:bar"},
		{"foo/20:bar", "foo/25:bar", 10, "foo/25:bar"},
		{"foo/10:bar/20:baz", "bar/15:foo/20:baz", 1, "foo/10:bar/19:foo/20:baz"},
		{"foo/10:bar/20:baz", "bar/15:foo/20:baz", 5, "foo/10:bar/15:foo/20:baz"},
		{"foo/10:bar/20:baz", "bar/15:foo/20:baz", 6, "foo/9:bar/15:foo/20:baz"},
		{"foo/10:bar/20:baz", "bar/15:foo/20:baz", 10, "foo/5:bar/15:foo/20:baz"},
		{"foo/10:bar/20:baz", "bar/15:foo/20:baz", 15, "bar/15:foo/20:baz"},
	}

	for _, tt := range tests {
		current, err := core.ParseBlockDistributionStr(tt.current)
		require.NoError(t, err, tt.current)
		target, err := core.ParseBlockDistributionStr(tt.target)
		require.NoError(t, err, tt.target)
		expected, err := core.ParseBlockDistributionStr(tt.expected)
		require.NoError(t, err, tt.expected)

		actual := core.MoveBlockDistribution(current, target, tt.n)
		assert.True(t, actual.Equals(expected), "move(%v,%v,%v)=%v, want %v", tt.current, tt.target, tt.n, actual, tt.expected)
	}
}
