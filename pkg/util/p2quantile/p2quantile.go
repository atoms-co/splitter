package p2quantile

import (
	"fmt"
	"math"
)

const epsilon = 1e-6

// Snapshot represents a Snapshot of P2Quantile, used to marshal to/unmarshal from proto message.
type Snapshot struct {
	Percentile       float64
	Height           [5]float64
	MarkerPos        [5]uint64
	DesiredMarkerPos [5]float64
}

func NewSnapshot(p float64, height []float64, markerPos []uint64, desiredMarkerPos []float64) (Snapshot, error) {
	if p < 0 || p > 1 {
		return Snapshot{}, fmt.Errorf("quantile %.2f must be between 0 and 1", p)
	}

	if len(height) != 5 {
		return Snapshot{}, fmt.Errorf("height must have 5 elements, but got %d", len(height))
	}

	if len(markerPos) != 5 {
		return Snapshot{}, fmt.Errorf("markerPos must have 5 elements, but got %d", len(markerPos))
	}

	if len(desiredMarkerPos) != 5 {
		return Snapshot{}, fmt.Errorf("desiredMarkerPos must have 5 elements, but got %d", len(desiredMarkerPos))
	}

	s := Snapshot{
		Percentile:       p,
		Height:           [5]float64(height),
		MarkerPos:        [5]uint64(markerPos),
		DesiredMarkerPos: [5]float64(desiredMarkerPos),
	}
	return s, nil
}

// P2Quantile is a data structure with O(1) time and space complexities for estimating the percentile quantile of
// a stream of observations based on "The P^2 Algorithm for Dynamic Calculation of Quantiles and
// Histograms Without Storing Observations" by RAJ JAIN and IIMRICH CHLAMTAC, Communications of the ACM, Oct. 1985
type P2Quantile struct {
	// The p-quantile to be tracked.
	// Corresponding to p in original paper.
	percentile float64
	// Height of markers, corresponding to quantile values.
	// It tracks minimum, the quantile/2-, quantile-, (1+quantile)/2-quantiles, and the maximum.
	// Corresponding to q in original paper.
	height [5]float64
	// Horizontal position of the markers.
	// It tracks number of observations. markerPos[4] == number of total observations.
	// Corresponding to n in original paper.
	markerPos [5]uint64

	// The desired horizontal position of markers.
	// Corresponding to n' in original paper
	desiredMarkerPos [5]float64
	// Incremental to desiredMarkerPos for each data point
	// Corresponding to dn' in original paper
	deltalMarkerPos [5]float64
}

// New creates and returns a P2Quantile.
// quantile must be between 0 and 1, or an error is returned.
func New(p float64) (*P2Quantile, error) {
	if p < 0 || p > 1 {
		return nil, fmt.Errorf("quantile %.2f must be between 0 and 1", p)
	}

	return &P2Quantile{
		percentile: p,
		markerPos:  [5]uint64{1, 2, 3, 4, 0},

		desiredMarkerPos: [5]float64{1, 1 + 2*p, 1 + 4*p, 3 + 2*p, 5},
		deltalMarkerPos:  [5]float64{0, p / 2, p, (1 + p) / 2, 1},
	}, nil
}

// Restore restores a P2Quantile instance from given values.
func Restore(s Snapshot) *P2Quantile {
	p := s.Percentile
	return &P2Quantile{
		percentile:       s.Percentile,
		height:           [5]float64(s.Height[:]),
		markerPos:        [5]uint64(s.MarkerPos[:]),
		desiredMarkerPos: [5]float64(s.DesiredMarkerPos[:]),
		deltalMarkerPos:  [5]float64{0, p / 2, p, (1 + p) / 2, 1},
	}
}

// Snapshot takes a snapshot from P2Quantile
func (p *P2Quantile) Snapshot() Snapshot {
	snap, _ := NewSnapshot(p.percentile, p.height[:], p.markerPos[:], p.desiredMarkerPos[:])
	return snap
}

// Add adds a data point.
func (p *P2Quantile) Add(value float64) {
	if p.markerPos[4] < 5 {
		// initialization
		count := p.markerPos[4]
		p.height[count] = value
		// sort first five observations
		for ; count > 0 && p.height[count-1] > p.height[count]; count-- {
			p.height[count-1], p.height[count] = p.height[count], p.height[count-1]
		}

		p.markerPos[4]++
		return
	}

	// find cell k such that p.height[k] <= value < p.height[k+1]
	var k int
	switch {
	case value < p.height[0]:
		p.height[0] = value
		k = 0
	case value < p.height[1]:
		k = 0
	case value < p.height[2]:
		k = 1
	case value < p.height[3]:
		k = 2
	case value < p.height[4]:
		k = 3
	default:
		p.height[4] = value
		k = 3
	}

	// Increment positions of markers k + 1 through 5
	for i := k + 1; i < 5; i++ {
		p.markerPos[i]++
	}

	// Update desired positions for all markers
	for i := 0; i < 5; i++ {
		p.desiredMarkerPos[i] += p.deltalMarkerPos[i]
	}

	// adjust heights of markers 2-4 if necessary
	for i := 1; i < 4; i++ {
		d := sign(p.desiredMarkerPos[i] - float64(p.markerPos[i]))
		if d > 0 && p.markerPos[i]+1 < p.markerPos[i+1] || d < 0 && p.markerPos[i-1]+1 < p.markerPos[i] {
			// try using the piecewise polynomial degree 2 formula
			qp := p.parabolic(i, d)
			if p.height[i-1] < qp && qp < p.height[i+1] {
				p.height[i] = qp
			} else {
				// use linear formula if degree 2 formula would result in out of order markers
				p.height[i] = p.linear(i, int(d))
			}
			if d > 0 {
				// increment the counter for the bin after adjustments were made
				p.markerPos[i]++
			} else {
				p.markerPos[i]--
			}
		}
	}
}

// Quantile returns quantile-quantile if observations were added.
func (p *P2Quantile) Quantile() (float64, bool) {
	count := p.markerPos[4]
	if count == 0 {
		return 0, false
	}

	if count <= 5 {
		cp := make([]float64, count)
		copy(cp, p.height[:count])
		index := int(math.Round(float64(count-1) * p.percentile))
		return cp[index], true
	}
	return p.height[2], true
}

func (p *P2Quantile) parabolic(i int, d float64) float64 {
	pp := p.markerPos[i-1]
	hp := p.height[i-1]
	pc := p.markerPos[i]
	hc := p.height[i]
	pn := p.markerPos[i+1]
	hn := p.height[i+1]
	return p.height[i] + d*((float64(pc-pp)+d)*(hn-hc)/float64(pn-pc)+(float64(pn-pc)-d)*(hc-hp)/float64(pc-pp))/float64(pn-pp)
}

func (p *P2Quantile) linear(i, d int) float64 {
	return p.height[i] + float64(d)*(p.height[i+d]-p.height[i])/(float64(p.markerPos[i+d])-float64(p.markerPos[i]))
}

func sign(x float64) float64 {
	if x > 1.0 || eq(x, 1.0) {
		return 1.0
	} else if x < -1.0 || eq(x, -1.0) {
		return -1.0
	}
	return 0
}

func eq(x, y float64) bool {
	return math.Abs(x-y) <= epsilon
}
