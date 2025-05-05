package uuidx

import (
	"fmt"
	"sort"

	"github.com/google/uuid"

	"go.atoms.co/lib/uuidx"
)

// RangesIntersect checks if any of the provided ranges intersect with each other.
func RangesIntersect(ranges []uuidx.Range) error {
	if len(ranges) <= 1 {
		return nil
	}

	sorted := make([]uuidx.Range, len(ranges))
	copy(sorted, ranges)
	sort.Slice(sorted, func(i, j int) bool {
		return uuidx.Less(sorted[i].From(), sorted[j].From())
	})

	for i := range len(ranges) - 1 {
		if _, intersects := ranges[i].Intersects(ranges[i+1]); intersects {
			return fmt.Errorf("ranges intersect: %v and %v", ranges[i], ranges[i+1])
		}
	}

	return nil
}

// SplitWithCustomRanges creates a set of ranges by first creating equal-sized
// ranges across the entire UUID space, then carving out any custom range provided.
// This may result in more ranges than baseRangeCount, but ensures custom ranges are preserved
// and the remaining space is divided as equally as possible.
func SplitWithCustomRanges(baseRangeCount int, customRanges []uuidx.Range) ([]uuidx.Range, error) {
	if err := RangesIntersect(customRanges); err != nil {
		return nil, fmt.Errorf("invalid custom ranges: %w", err)
	}

	baseRanges, err := uuidx.Split(uuidx.Domain, baseRangeCount)
	if err != nil {
		return nil, err
	}

	if len(customRanges) == 0 {
		return baseRanges, nil
	}

	type boundary struct {
		point    uuid.UUID // UUID value of this boundary
		isCustom bool      // Custom (true) or base (false) range
		index    int
	}

	var boundaries []boundary

	for i, b := range baseRanges {
		boundaries = append(boundaries, boundary{b.From(), false, i})
	}

	for i, c := range customRanges {
		boundaries = append(boundaries, boundary{c.From(), true, i})
	}

	// Sort boundaries by UUID value.
	// For identical UUIDs, custom range boundaries take precedence
	sort.Slice(boundaries, func(i, j int) bool {
		cmp := uuidx.Compare(boundaries[i].point, boundaries[j].point)
		if cmp == 0 {
			return boundaries[i].isCustom
		}
		return cmp < 0
	})

	var result []uuidx.Range
	n := len(boundaries)

	for i := 0; i < n; {
		b := boundaries[i]

		if b.isCustom {
			// Add custom range as is
			c := customRanges[b.index]
			result = append(result, c)

			// Skip all base ranges that are included in the current custom range
			for i < n && uuidx.Less(boundaries[i].point, c.To()) {
				i++
			}

			// Include a range from the end of the custom range to the start of the next range (custom or base).
			// Use last uuid if there are no more ranges.
			end := uuidx.Max
			if i < n {
				end = boundaries[i].point
			}

			if c.To() != end {
				r, err := uuidx.NewRange(c.To(), end)
				if err != nil {
					return nil, fmt.Errorf("failed to create range: %w", err)
				}
				result = append(result, r)
			}
		} else {
			c := baseRanges[b.index]
			if i == n-1 {
				// Add last range as is
				result = append(result, c)
			} else {
				// Add range from start of current base range to the next range (which can be a custom range
				// or another base range)
				r, err := uuidx.NewRange(c.From(), boundaries[i+1].point)
				if err != nil {
					return nil, fmt.Errorf("failed to create range: %w", err)
				}
				result = append(result, r)
			}
			i++
		}
	}

	return result, nil
}
