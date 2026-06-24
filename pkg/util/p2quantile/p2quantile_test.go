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

func TestP2Quantile_NewSnapshot(t *testing.T) {
	t.Parallel()

	desired := []float64{1, 2.25, 3.5, 4.75, 6}
	s, err := NewSnapshot(0.5, []float64{0.02, 0.15, 0.74, 0.83, 22.37}, []uint64{1, 2, 3, 4, 6}, desired)
	require.NoError(t, err)
	require.Equal(t, 0.5, s.Percentile)
	require.Equal(t, [5]float64{0.02, 0.15, 0.74, 0.83, 22.37}, s.Height)
	require.Equal(t, [5]uint64{1, 2, 3, 4, 6}, s.MarkerPos)
	require.Equal(t, [5]float64{1, 2.25, 3.5, 4.75, 6}, s.DesiredMarkerPos)

	_, err = NewSnapshot(-0.1, make([]float64, 5), make([]uint64, 5), make([]float64, 5))
	require.Error(t, err)

	_, err = NewSnapshot(1.1, make([]float64, 5), make([]uint64, 5), make([]float64, 5))
	require.Error(t, err)

	_, err = NewSnapshot(0.5, []float64{1, 2, 3}, make([]uint64, 5), make([]float64, 5))
	require.Error(t, err)

	_, err = NewSnapshot(0.5, make([]float64, 5), []uint64{1, 2, 3}, make([]float64, 5))
	require.Error(t, err)

	_, err = NewSnapshot(0.5, make([]float64, 5), make([]uint64, 5), []float64{1, 2, 3})
	require.Error(t, err)

	s, err = NewSnapshot(0.5, make([]float64, 5), make([]uint64, 5), make([]float64, 5))
	require.NoError(t, err)

}

func TestP2Quantile_TakeSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	q, err := New(0.5)
	require.NoError(t, err)

	for _, dp := range dataPoints {
		q.Add(dp.data)

		snap := q.Snapshot()
		restored, err := NewSnapshot(snap.Percentile, snap.Height[:], snap.MarkerPos[:], snap.DesiredMarkerPos[:])
		require.NoError(t, err)

		q2 := Restore(restored)
		actual, ok := q2.Quantile()
		require.True(t, ok)
		assert.InDeltaf(t, dp.quantile, actual, tolerance, "got %g want %g ± %g", actual, dp.quantile, tolerance)
		require.Equal(t, snap, q2.Snapshot())
	}
}

func TestP2Quantile_RestoreContinuesStreaming(t *testing.T) {
	t.Parallel()

	q1, err := New(0.2)
	require.NoError(t, err)
	for i := 0; i < len(dataPoints)/2; i++ {
		q1.Add(dataPoints[i].data)
	}

	snap := q1.Snapshot()
	for i := len(dataPoints) / 2; i < len(dataPoints); i++ {
		q1.Add(dataPoints[i].data)
	}
	expected, ok := q1.Quantile()
	require.True(t, ok)

	q2 := Restore(snap)
	for i := len(dataPoints) / 2; i < len(dataPoints); i++ {
		q2.Add(dataPoints[i].data)
	}

	actual, ok := q2.Quantile()
	require.True(t, ok)
	assert.InDeltaf(t, expected, actual, tolerance, "got %g want %g ± %g", actual, expected, tolerance)
}
