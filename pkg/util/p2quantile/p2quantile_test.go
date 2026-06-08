package p2quantile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testCaseData struct {
	data float64

	markerPos        [5]uint64
	desiredMarkerPos [5]float64
	height           [5]float64
	quantile         float64
}

// Published table came with two digital decimals,
// tolerance is the difference we tolerate when comparing data to that in original paper.
const tolerance = 2e-2

// dataPoints is the test data from Table 1 in the paper
var dataPoints = []testCaseData{
	{data: 0.02, markerPos: [5]uint64{1, 2, 3, 4, 1}, desiredMarkerPos: [5]float64{1, 2, 3, 4, 5}, height: [5]float64{0.02, 0, 0, 0, 0}, quantile: 0.02},
	{data: 0.15, markerPos: [5]uint64{1, 2, 3, 4, 2}, desiredMarkerPos: [5]float64{1, 2, 3, 4, 5}, height: [5]float64{0.02, 0.15, 0, 0, 0}, quantile: 0.15},
	{data: 0.74, markerPos: [5]uint64{1, 2, 3, 4, 3}, desiredMarkerPos: [5]float64{1, 2, 3, 4, 5}, height: [5]float64{0.02, 0.15, 0.74, 0, 0}, quantile: 0.15},
	{data: 3.39, markerPos: [5]uint64{1, 2, 3, 4, 4}, desiredMarkerPos: [5]float64{1, 2, 3, 4, 5}, height: [5]float64{0.02, 0.15, 0.74, 3.39, 0}, quantile: 0.74},
	{data: 0.83, markerPos: [5]uint64{1, 2, 3, 4, 5}, desiredMarkerPos: [5]float64{1, 2, 3, 4, 5}, height: [5]float64{0.02, 0.15, 0.74, 0.83, 3.39}, quantile: 0.74},
	{data: 22.37, markerPos: [5]uint64{1, 2, 3, 4, 6}, desiredMarkerPos: [5]float64{1, 2.25, 3.5, 4.75, 6}, height: [5]float64{0.02, 0.15, 0.74, 0.83, 22.37}, quantile: 0.74},
	{data: 10.15, markerPos: [5]uint64{1, 2, 3, 5, 7}, desiredMarkerPos: [5]float64{1, 2.5, 4, 5.5, 7}, height: [5]float64{0.02, 0.15, 0.74, 4.465, 22.37}, quantile: 0.74},
	{data: 15.43, markerPos: [5]uint64{1, 2, 4, 6, 8}, desiredMarkerPos: [5]float64{1, 2.75, 4.5, 6.25, 8}, height: [5]float64{0.02, 0.15, 2.18, 8.60, 22.37}, quantile: 2.18},
	{data: 38.62, markerPos: [5]uint64{1, 3, 5, 7, 9}, desiredMarkerPos: [5]float64{1, 3, 5, 7, 9}, height: [5]float64{0.02, 0.87, 4.75, 15.52, 38.62}, quantile: 4.75},
	{data: 15.92, markerPos: [5]uint64{1, 3, 5, 7, 10}, desiredMarkerPos: [5]float64{1, 3.25, 5.5, 7.75, 10}, height: [5]float64{0.02, 0.87, 4.75, 15.52, 38.62}, quantile: 4.75},
	{data: 34.60, markerPos: [5]uint64{1, 3, 6, 8, 11}, desiredMarkerPos: [5]float64{1, 3.5, 6, 8.5, 11}, height: [5]float64{0.02, 0.87, 9.28, 21.58, 38.62}, quantile: 9.28},
	{data: 10.28, markerPos: [5]uint64{1, 3, 6, 9, 12}, desiredMarkerPos: [5]float64{1, 3.75, 6.5, 9.25, 12}, height: [5]float64{0.02, 0.87, 9.28, 21.58, 38.62}, quantile: 9.28},
	{data: 1.47, markerPos: [5]uint64{1, 4, 7, 10, 13}, desiredMarkerPos: [5]float64{1, 4, 7, 10, 13}, height: [5]float64{0.02, 2.14, 9.28, 21.58, 38.62}, quantile: 9.28},
	{data: 0.40, markerPos: [5]uint64{1, 5, 8, 11, 14}, desiredMarkerPos: [5]float64{1, 4.25, 7.5, 10.75, 14}, height: [5]float64{0.02, 2.14, 9.28, 21.58, 38.62}, quantile: 9.28},
	{data: 0.05, markerPos: [5]uint64{1, 5, 8, 12, 15}, desiredMarkerPos: [5]float64{1, 4.5, 8, 11.5, 15}, height: [5]float64{0.02, 0.74, 6.30, 21.58, 38.62}, quantile: 6.30},
	{data: 11.39, markerPos: [5]uint64{1, 5, 8, 13, 16}, desiredMarkerPos: [5]float64{1, 4.75, 8.5, 12.25, 16}, height: [5]float64{0.02, 0.74, 6.30, 21.58, 38.62}, quantile: 6.30},
	{data: 0.27, markerPos: [5]uint64{1, 5, 9, 13, 17}, desiredMarkerPos: [5]float64{1, 5, 9, 13, 17}, height: [5]float64{0.02, 0.59, 6.30, 17.22, 38.62}, quantile: 6.30},
	{data: 0.42, markerPos: [5]uint64{1, 6, 10, 14, 18}, desiredMarkerPos: [5]float64{1, 5.25, 9.5, 13.75, 18}, height: [5]float64{0.02, 0.59, 6.30, 17.22, 38.62}, quantile: 6.30},
	{data: 0.09, markerPos: [5]uint64{1, 6, 10, 15, 19}, desiredMarkerPos: [5]float64{1, 5.5, 10, 14.5, 19}, height: [5]float64{0.02, 0.50, 4.44, 17.22, 38.62}, quantile: 4.44},
	{data: 11.37, markerPos: [5]uint64{1, 6, 10, 16, 20}, desiredMarkerPos: [5]float64{1, 5.75, 10.5, 15.25, 20}, height: [5]float64{0.02, 0.50, 4.44, 17.22, 38.62}, quantile: 4.44},
}

func TestP2Quantile_DataPoints(t *testing.T) {
	q, err := New(0.5)
	require.NoError(t, err)

	for i, dp := range dataPoints {
		q.Add(dp.data)
		for j := 0; j < 5; j++ {
			require.Equal(t, dp.markerPos[j], q.markerPos[j], "Added data point[%v] %v, expected markerPos[%v]=%v, got markerPos[%v]=%v", i, dp.data, j, dp.markerPos[j], j, q.markerPos[j])
			require.Equal(t, dp.desiredMarkerPos[j], q.desiredMarkerPos[j], "Added data point[%v] %v, expected desiredMarkerPos[%v]=%v, got desiredMarkerPos[%v]=%v", i, dp.data, j, dp.desiredMarkerPos[j], j, q.desiredMarkerPos[j])
			require.InDeltaf(t, dp.height[j], q.height[j], tolerance, "Added data point[%v] %v, expected height[%v]=%v, got height[%v]=%v", i, dp.data, j, dp.height[j], j, q.height[j])
		}

		actual, ok := q.Quantile()
		expected := dp.quantile
		require.True(t, ok)
		assert.InDeltaf(t, expected, actual, tolerance, "got %g want %g ± %g", actual, expected, tolerance)
	}
}
