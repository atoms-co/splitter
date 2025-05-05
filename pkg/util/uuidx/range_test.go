package uuidx_test

import (
	"testing"

	"github.com/google/uuid"

	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/uuidx"
	splitteruuidx "go.atoms.co/splitter/pkg/util/uuidx"
)

func u(s string) uuid.UUID {
	return uuid.MustParse(s)
}

func r(from, to string) uuidx.Range {
	return uuidx.MustNewRange(u(from), u(to))
}

func TestSplitWithCustomRanges(t *testing.T) {
	testCases := []struct {
		name           string
		customRanges   []uuidx.Range
		baseRangeCount int
		expectedError  string
		expectedRanges []uuidx.Range
	}{
		{
			name:           "no custom ranges",
			customRanges:   []uuidx.Range{},
			baseRangeCount: 2,
			expectedRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
				r("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "one custom range at beginning",
			customRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
			},
			baseRangeCount: 2,
			expectedRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
				r("20000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
				r("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "one custom range in middle spanning boundary",
			customRanges: []uuidx.Range{
				r("70000000-0000-0000-0000-000000000000", "90000000-0000-0000-0000-000000000000"),
			},
			baseRangeCount: 2,
			expectedRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "70000000-0000-0000-0000-000000000000"),
				r("70000000-0000-0000-0000-000000000000", "90000000-0000-0000-0000-000000000000"),
				r("90000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "one custom range at end",
			customRanges: []uuidx.Range{
				r("e0000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
			baseRangeCount: 2,
			expectedRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
				r("80000000-0000-0000-0000-000000000000", "e0000000-0000-0000-0000-000000000000"),
				r("e0000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "custom range exactly matching equal range",
			customRanges: []uuidx.Range{
				r("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
			baseRangeCount: 2,
			expectedRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
				r("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "multiple gaps with exact fit",
			customRanges: []uuidx.Range{
				r("10000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
				r("30000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
				r("50000000-0000-0000-0000-000000000000", "60000000-0000-0000-0000-000000000000"),
			},
			baseRangeCount: 2,
			expectedRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "10000000-0000-0000-0000-000000000000"),
				r("10000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
				r("20000000-0000-0000-0000-000000000000", "30000000-0000-0000-0000-000000000000"),
				r("30000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
				r("40000000-0000-0000-0000-000000000000", "50000000-0000-0000-0000-000000000000"),
				r("50000000-0000-0000-0000-000000000000", "60000000-0000-0000-0000-000000000000"),
				r("60000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
				r("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "custom ranges with overlap",
			customRanges: []uuidx.Range{
				r("10000000-0000-0000-0000-000000000000", "30000000-0000-0000-0000-000000000000"),
				r("20000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
			},
			baseRangeCount: 2,
			expectedError:  "invalid custom ranges: ranges intersect: [10000000-0000-0000-0000-000000000000;30000000-0000-0000-0000-000000000000) and [20000000-0000-0000-0000-000000000000;40000000-0000-0000-0000-000000000000)",
			expectedRanges: nil,
		},
		{
			name: "invalid target range count",
			customRanges: []uuidx.Range{
				r("10000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
			},
			baseRangeCount: 0,
			expectedError:  "number of partitions should atleast be 1",
			expectedRanges: nil,
		},
		{
			name: "full coverage with custom ranges",
			customRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
				r("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
			baseRangeCount: 4,
			expectedRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
				r("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "complex fragmentation scenario",
			customRanges: []uuidx.Range{
				r("10000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
				r("50000000-0000-0000-0000-000000000000", "60000000-0000-0000-0000-000000000000"),
				r("90000000-0000-0000-0000-000000000000", "a0000000-0000-0000-0000-000000000000"),
			},
			baseRangeCount: 3,
			expectedRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "10000000-0000-0000-0000-000000000000"),
				r("10000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
				r("20000000-0000-0000-0000-000000000000", "50000000-0000-0000-0000-000000000000"),
				r("50000000-0000-0000-0000-000000000000", "60000000-0000-0000-0000-000000000000"),
				r("60000000-0000-0000-0000-000000000000", "90000000-0000-0000-0000-000000000000"),
				r("90000000-0000-0000-0000-000000000000", "a0000000-0000-0000-0000-000000000000"),
				r("a0000000-0000-0000-0000-000000000000", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
				r("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "merging adjacent fragments",
			customRanges: []uuidx.Range{
				r("55555555-5555-5555-5555-555555555555", "66666666-6666-6666-6666-666666666666"),
			},
			baseRangeCount: 3,
			expectedRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "55555555-5555-5555-5555-555555555555"),
				r("55555555-5555-5555-5555-555555555555", "66666666-6666-6666-6666-666666666666"),
				r("66666666-6666-6666-6666-666666666666", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
				r("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "multiple custom ranges in one equal range",
			customRanges: []uuidx.Range{
				r("10000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
				r("30000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
			},
			baseRangeCount: 2,
			expectedRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "10000000-0000-0000-0000-000000000000"),
				r("10000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
				r("20000000-0000-0000-0000-000000000000", "30000000-0000-0000-0000-000000000000"),
				r("30000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
				r("40000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
				r("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "multiple custom ranges",
			customRanges: []uuidx.Range{
				r("10000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
				r("30000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
				r("50000000-0000-0000-0000-000000000000", "60000000-0000-0000-0000-000000000000"),
			},
			baseRangeCount: 7,
			expectedRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "10000000-0000-0000-0000-000000000000"),
				r("10000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
				r("20000000-0000-0000-0000-000000000000", "24924924-9249-2492-4924-924924924924"),
				r("24924924-9249-2492-4924-924924924924", "30000000-0000-0000-0000-000000000000"),
				r("30000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
				r("40000000-0000-0000-0000-000000000000", "49249249-2492-4924-9249-249249249248"),
				r("49249249-2492-4924-9249-249249249248", "50000000-0000-0000-0000-000000000000"),
				r("50000000-0000-0000-0000-000000000000", "60000000-0000-0000-0000-000000000000"),
				r("60000000-0000-0000-0000-000000000000", "6db6db6d-b6db-6db6-db6d-b6db6db6db6c"),
				r("6db6db6d-b6db-6db6-db6d-b6db6db6db6c", "92492492-4924-9249-2492-492492492490"),
				r("92492492-4924-9249-2492-492492492490", "b6db6db6-db6d-b6db-6db6-db6db6db6db4"),
				r("b6db6db6-db6d-b6db-6db6-db6db6db6db4", "db6db6db-6db6-db6d-b6db-6db6db6db6d8"),
				r("db6db6db-6db6-db6d-b6db-6db6db6db6d8", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "single UUID Range",
			customRanges: []uuidx.Range{
				r("10000000-0000-0000-0000-000000000000", "10000000-0000-0000-0000-000000000001"),
			},
			baseRangeCount: 2,
			expectedRanges: []uuidx.Range{
				r("00000000-0000-0000-0000-000000000000", "10000000-0000-0000-0000-000000000000"),
				r("10000000-0000-0000-0000-000000000000", "10000000-0000-0000-0000-000000000001"),
				r("10000000-0000-0000-0000-000000000001", "80000000-0000-0000-0000-000000000000"),
				r("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := splitteruuidx.SplitWithCustomRanges(tc.baseRangeCount, tc.customRanges)

			if tc.expectedError != "" {
				assertx.Equal(t, err.Error(), tc.expectedError)
			} else {
				resultStrings := slicex.Map(result, uuidx.Range.String)
				expectedStrings := slicex.Map(tc.expectedRanges, uuidx.Range.String)

				assertx.Equal(t, expectedStrings, resultStrings)
			}
		})
	}
}
