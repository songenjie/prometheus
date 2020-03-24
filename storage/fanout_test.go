// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/util/testutil"
)

func TestMergeStringSlices(t *testing.T) {
	for _, tc := range []struct {
		input    [][]string
		expected []string
	}{
		{},
		{[][]string{{"foo"}}, []string{"foo"}},
		{[][]string{{"foo"}, {"bar"}}, []string{"bar", "foo"}},
		{[][]string{{"foo"}, {"bar"}, {"baz"}}, []string{"bar", "baz", "foo"}},
	} {
		testutil.Equals(t, tc.expected, mergeStringSlices(tc.input))
	}
}

func TestMergeTwoStringSlices(t *testing.T) {
	for _, tc := range []struct {
		a, b, expected []string
	}{
		{[]string{}, []string{}, []string{}},
		{[]string{"foo"}, nil, []string{"foo"}},
		{nil, []string{"bar"}, []string{"bar"}},
		{[]string{"foo"}, []string{"bar"}, []string{"bar", "foo"}},
		{[]string{"foo"}, []string{"bar", "baz"}, []string{"bar", "baz", "foo"}},
		{[]string{"foo"}, []string{"foo"}, []string{"foo"}},
	} {
		testutil.Equals(t, tc.expected, mergeTwoStringSlices(tc.a, tc.b))
	}
}

func TestMergeQuerierWithChainMerger(t *testing.T) {
	for _, tc := range []struct {
		name          string
		querierSeries [][]Series
		extraQueriers []Querier

		expected SeriesSet
	}{
		{
			name:          "1 querier with no series",
			querierSeries: [][]Series{{}},
			expected:      NewMockSeriesSet(),
		},
		{
			name:          "many queriers with no series",
			querierSeries: [][]Series{{}, {}, {}, {}, {}, {}, {}},
			expected:      NewMockSeriesSet(),
		},
		{
			name: "1 querier, two series",
			querierSeries: [][]Series{{
				NewListSeries(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}}),
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}}),
			}},
			expected: NewMockSeriesSet(
				NewListSeries(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}}),
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}}),
			),
		},
		{
			name: "2 queriers, 1 different series each",
			querierSeries: [][]Series{{
				NewListSeries(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}}),
			}, {
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}}),
			}},
			expected: NewMockSeriesSet(
				NewListSeries(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}}),
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}}),
			),
		},
		{
			name: "2 time unsorted queriers, 2 series each",
			querierSeries: [][]Series{{
				NewListSeries(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{5, 5}, sample{6, 6}}),
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}}),
			}, {
				NewListSeries(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}}),
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{3, 3}, sample{4, 4}}),
			}},
			expected: NewMockSeriesSet(
				NewListSeries(
					labels.FromStrings("bar", "baz"),
					[]tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{5, 5}, sample{6, 6}},
				),
				NewListSeries(
					labels.FromStrings("foo", "bar"),
					[]tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{4, 4}},
				),
			),
		},
		{
			name: "5 queriers, only 2 queriers have 2 time unsorted series each",
			querierSeries: [][]Series{{}, {}, {
				NewListSeries(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{5, 5}, sample{6, 6}}),
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}}),
			}, {
				NewListSeries(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}}),
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{3, 3}, sample{4, 4}}),
			}, {}},
			expected: NewMockSeriesSet(
				NewListSeries(
					labels.FromStrings("bar", "baz"),
					[]tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{5, 5}, sample{6, 6}},
				),
				NewListSeries(
					labels.FromStrings("foo", "bar"),
					[]tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{4, 4}},
				),
			),
		},
		{
			name: "2 queriers, only 2 queriers have 2 time unsorted series each, with 3 noop and one nil querier together",
			querierSeries: [][]Series{{}, {}, {
				NewListSeries(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{5, 5}, sample{6, 6}}),
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}}),
			}, {
				NewListSeries(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}}),
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{3, 3}, sample{4, 4}}),
			}, {}},
			extraQueriers: []Querier{NoopQuerier(), NoopQuerier(), nil, NoopQuerier()},
			expected: NewMockSeriesSet(
				NewListSeries(
					labels.FromStrings("bar", "baz"),
					[]tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{5, 5}, sample{6, 6}},
				),
				NewListSeries(
					labels.FromStrings("foo", "bar"),
					[]tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{4, 4}},
				),
			),
		},
		{
			name: "2 queriers, with 2 series, one is overlapping",
			querierSeries: [][]Series{{}, {}, {
				NewListSeries(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{2, 21}, sample{3, 31}, sample{5, 5}, sample{6, 6}}),
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}}),
			}, {
				NewListSeries(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 22}, sample{3, 32}}),
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{3, 3}, sample{4, 4}}),
			}, {}},
			expected: NewMockSeriesSet(
				NewListSeries(
					labels.FromStrings("bar", "baz"),
					[]tsdbutil.Sample{sample{1, 1}, sample{2, 21}, sample{3, 31}, sample{5, 5}, sample{6, 6}},
				),
				NewListSeries(
					labels.FromStrings("foo", "bar"),
					[]tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{4, 4}},
				),
			),
		},
		{
			name: "2 queries, one with NaN samples series",
			querierSeries: [][]Series{{
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, math.NaN()}}),
			}, {
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{1, 1}}),
			}},
			expected: NewMockSeriesSet(
				NewListSeries(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, math.NaN()}, sample{1, 1}}),
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var qs []Querier
			for _, in := range tc.querierSeries {
				qs = append(qs, &mockQuerier{toReturn: in})
			}
			qs = append(qs, tc.extraQueriers...)

			merged, _, _ := NewMergeQuerier(qs, 0, ChainingSeriesMerge).Select(false, nil)
			for merged.Next() {
				testutil.Assert(t, tc.expected.Next(), "Expected Next() to be true")
				actualSeries := merged.At()
				expectedSeries := tc.expected.At()
				testutil.Equals(t, expectedSeries.Labels(), actualSeries.Labels())

				expSmpl, expErr := ExpandSamples(expectedSeries.Iterator())
				actSmpl, actErr := ExpandSamples(actualSeries.Iterator())
				testutil.Equals(t, expErr, actErr)
				testutil.Equals(t, expSmpl, actSmpl)
			}
			testutil.Ok(t, merged.Err())
			testutil.Assert(t, !tc.expected.Next(), "Expected Next() to be false")
		})
	}
}

func TestMergeChunkQuerierWithNoVerticalChunkSeriesMerger(t *testing.T) {
	for _, tc := range []struct {
		name             string
		chkQuerierSeries [][]ChunkSeries
		extraQueriers    []ChunkQuerier

		expected ChunkSeriesSet
	}{
		{
			name:             "one querier with no series",
			chkQuerierSeries: [][]ChunkSeries{{}},
			expected:         NewMockChunkSeriesSet(),
		},
		{
			name:             "many queriers with no series",
			chkQuerierSeries: [][]ChunkSeries{{}, {}, {}, {}, {}, {}, {}},
			expected:         NewMockChunkSeriesSet(),
		},
		{
			name: "one querier, two series",
			chkQuerierSeries: [][]ChunkSeries{{
				NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{3, 3}}),
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}}, []tsdbutil.Sample{sample{2, 2}}),
			}},
			expected: NewMockChunkSeriesSet(
				NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{3, 3}}),
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}}, []tsdbutil.Sample{sample{2, 2}}),
			),
		},
		{
			name: "two queriers, one different series each",
			chkQuerierSeries: [][]ChunkSeries{{
				NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{3, 3}}),
			}, {
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}}, []tsdbutil.Sample{sample{2, 2}}),
			}},
			expected: NewMockChunkSeriesSet(
				NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{3, 3}}),
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}}, []tsdbutil.Sample{sample{2, 2}}),
			),
		},
		{
			name: "two queriers, two not in time order series each",
			chkQuerierSeries: [][]ChunkSeries{{
				NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{5, 5}}, []tsdbutil.Sample{sample{6, 6}}),
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}}, []tsdbutil.Sample{sample{2, 2}}),
			}, {
				NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{3, 3}}),
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{3, 3}}, []tsdbutil.Sample{sample{4, 4}}),
			}},
			expected: NewMockChunkSeriesSet(
				NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"),
					[]tsdbutil.Sample{sample{1, 1}, sample{2, 2}},
					[]tsdbutil.Sample{sample{3, 3}},
					[]tsdbutil.Sample{sample{5, 5}},
					[]tsdbutil.Sample{sample{6, 6}},
				),
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"),
					[]tsdbutil.Sample{sample{0, 0}, sample{1, 1}},
					[]tsdbutil.Sample{sample{2, 2}},
					[]tsdbutil.Sample{sample{3, 3}},
					[]tsdbutil.Sample{sample{4, 4}},
				),
			),
		},
		{
			name: "five queriers, only two have two not in time order series each",
			chkQuerierSeries: [][]ChunkSeries{{}, {}, {
				NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{5, 5}}, []tsdbutil.Sample{sample{6, 6}}),
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}}, []tsdbutil.Sample{sample{2, 2}}),
			}, {
				NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{3, 3}}),
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{3, 3}}, []tsdbutil.Sample{sample{4, 4}}),
			}, {}},
			expected: NewMockChunkSeriesSet(
				NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"),
					[]tsdbutil.Sample{sample{1, 1}, sample{2, 2}},
					[]tsdbutil.Sample{sample{3, 3}},
					[]tsdbutil.Sample{sample{5, 5}},
					[]tsdbutil.Sample{sample{6, 6}},
				),
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"),
					[]tsdbutil.Sample{sample{0, 0}, sample{1, 1}},
					[]tsdbutil.Sample{sample{2, 2}},
					[]tsdbutil.Sample{sample{3, 3}},
					[]tsdbutil.Sample{sample{4, 4}},
				),
			),
		},
		{
			name: "two queriers, with two not in time order series each, with 3 noop queries and one nil together",
			chkQuerierSeries: [][]ChunkSeries{{
				NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{5, 5}}, []tsdbutil.Sample{sample{6, 6}}),
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, 0}, sample{1, 1}}, []tsdbutil.Sample{sample{2, 2}}),
			}, {
				NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{3, 3}}),
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{3, 3}}, []tsdbutil.Sample{sample{4, 4}}),
			}},
			extraQueriers: []ChunkQuerier{NoopChunkedQuerier(), NoopChunkedQuerier(), nil, NoopChunkedQuerier()},
			expected: NewMockChunkSeriesSet(
				NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"),
					[]tsdbutil.Sample{sample{1, 1}, sample{2, 2}},
					[]tsdbutil.Sample{sample{3, 3}},
					[]tsdbutil.Sample{sample{5, 5}},
					[]tsdbutil.Sample{sample{6, 6}},
				),
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"),
					[]tsdbutil.Sample{sample{0, 0}, sample{1, 1}},
					[]tsdbutil.Sample{sample{2, 2}},
					[]tsdbutil.Sample{sample{3, 3}},
					[]tsdbutil.Sample{sample{4, 4}},
				),
			),
		},
		{
			name: "two queries, one with NaN samples series",
			chkQuerierSeries: [][]ChunkSeries{{
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, math.NaN()}}),
			}, {
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{1, 1}}),
			}},
			expected: NewMockChunkSeriesSet(
				NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), []tsdbutil.Sample{sample{0, math.NaN()}}, []tsdbutil.Sample{sample{1, 1}}),
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var qs []ChunkQuerier
			for _, in := range tc.chkQuerierSeries {
				qs = append(qs, &mockChunkQurier{toReturn: in})
			}
			qs = append(qs, tc.extraQueriers...)

			merged, _, _ := NewMergeChunkQuerier(qs, 0, NewCompactingChunkSeriesMerger(nil)).Select(false, nil)
			for merged.Next() {
				testutil.Assert(t, tc.expected.Next(), "Expected Next() to be true")
				actualSeries := merged.At()
				expectedSeries := tc.expected.At()
				testutil.Equals(t, expectedSeries.Labels(), actualSeries.Labels())

				expChks, expErr := ExpandChunks(expectedSeries.Iterator())
				actChks, actErr := ExpandChunks(actualSeries.Iterator())
				testutil.Equals(t, expErr, actErr)
				testutil.Equals(t, expChks, actChks)

			}
			testutil.Ok(t, merged.Err())
			testutil.Assert(t, !tc.expected.Next(), "Expected Next() to be false")
		})
	}
}

type mockQuerier struct {
	LabelQuerier

	toReturn []Series
}

type seriesByLabel []Series

func (a seriesByLabel) Len() int           { return len(a) }
func (a seriesByLabel) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a seriesByLabel) Less(i, j int) bool { return labels.Compare(a[i].Labels(), a[j].Labels()) < 0 }

func (m *mockQuerier) Select(sortSeries bool, _ *SelectHints, _ ...*labels.Matcher) (SeriesSet, Warnings, error) {
	cpy := make([]Series, len(m.toReturn))
	copy(cpy, m.toReturn)
	if sortSeries {
		sort.Sort(seriesByLabel(cpy))
	}

	return NewMockSeriesSet(cpy...), nil, nil
}

type mockChunkQurier struct {
	LabelQuerier

	toReturn []ChunkSeries
}

type chunkSeriesByLabel []ChunkSeries

func (a chunkSeriesByLabel) Len() int      { return len(a) }
func (a chunkSeriesByLabel) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a chunkSeriesByLabel) Less(i, j int) bool {
	return labels.Compare(a[i].Labels(), a[j].Labels()) < 0
}

func (m *mockChunkQurier) Select(sortSeries bool, _ *SelectHints, _ ...*labels.Matcher) (ChunkSeriesSet, Warnings, error) {
	cpy := make([]ChunkSeries, len(m.toReturn))
	copy(cpy, m.toReturn)
	if sortSeries {
		sort.Sort(chunkSeriesByLabel(cpy))
	}

	return NewMockChunkSeriesSet(cpy...), nil, nil
}

type mockSeriesSet struct {
	idx    int
	series []Series
}

func NewMockSeriesSet(series ...Series) SeriesSet {
	return &mockSeriesSet{
		idx:    -1,
		series: series,
	}
}

func (m *mockSeriesSet) Next() bool {
	m.idx++
	return m.idx < len(m.series)
}

func (m *mockSeriesSet) At() Series { return m.series[m.idx] }

func (m *mockSeriesSet) Err() error { return nil }

type mockChunkSeriesSet struct {
	idx    int
	series []ChunkSeries
}

func NewMockChunkSeriesSet(series ...ChunkSeries) ChunkSeriesSet {
	return &mockChunkSeriesSet{
		idx:    -1,
		series: series,
	}
}

func (m *mockChunkSeriesSet) Next() bool {
	m.idx++
	return m.idx < len(m.series)
}

func (m *mockChunkSeriesSet) At() ChunkSeries { return m.series[m.idx] }

func (m *mockChunkSeriesSet) Err() error { return nil }

func TestChainSampleIterator(t *testing.T) {
	for _, tc := range []struct {
		input    []chunkenc.Iterator
		expected []tsdbutil.Sample
	}{
		{
			input: []chunkenc.Iterator{
				NewListSeriesIterator(samples{sample{0, 0}, sample{1, 1}}),
			},
			expected: []tsdbutil.Sample{sample{0, 0}, sample{1, 1}},
		},
		{
			input: []chunkenc.Iterator{
				NewListSeriesIterator(samples{sample{0, 0}, sample{1, 1}}),
				NewListSeriesIterator(samples{sample{2, 2}, sample{3, 3}}),
			},
			expected: []tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}, sample{3, 3}},
		},
		{
			input: []chunkenc.Iterator{
				NewListSeriesIterator(samples{sample{0, 0}, sample{3, 3}}),
				NewListSeriesIterator(samples{sample{1, 1}, sample{4, 4}}),
				NewListSeriesIterator(samples{sample{2, 2}, sample{5, 5}}),
			},
			expected: []tsdbutil.Sample{
				sample{0, 0}, sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{4, 4}, sample{5, 5}},
		},
		// Overlap.
		{
			input: []chunkenc.Iterator{
				NewListSeriesIterator(samples{sample{0, 0}, sample{1, 1}}),
				NewListSeriesIterator(samples{sample{0, 0}, sample{2, 2}}),
				NewListSeriesIterator(samples{sample{2, 2}, sample{3, 3}}),
				NewListSeriesIterator(samples{}),
				NewListSeriesIterator(samples{}),
				NewListSeriesIterator(samples{}),
			},
			expected: []tsdbutil.Sample{sample{0, 0}, sample{1, 1}, sample{2, 2}, sample{3, 3}},
		},
	} {
		merged := newChainSampleIterator(tc.input)
		actual, err := ExpandSamples(merged)
		testutil.Ok(t, err)
		testutil.Equals(t, tc.expected, actual)
	}
}

func TestChainSampleIteratorSeek(t *testing.T) {
	for _, tc := range []struct {
		input    []chunkenc.Iterator
		seek     int64
		expected []tsdbutil.Sample
	}{
		{
			input: []chunkenc.Iterator{
				NewListSeriesIterator(samples{sample{0, 0}, sample{1, 1}, sample{2, 2}}),
			},
			seek:     1,
			expected: []tsdbutil.Sample{sample{1, 1}, sample{2, 2}},
		},
		{
			input: []chunkenc.Iterator{
				NewListSeriesIterator(samples{sample{0, 0}, sample{1, 1}}),
				NewListSeriesIterator(samples{sample{2, 2}, sample{3, 3}}),
			},
			seek:     2,
			expected: []tsdbutil.Sample{sample{2, 2}, sample{3, 3}},
		},
		{
			input: []chunkenc.Iterator{
				NewListSeriesIterator(samples{sample{0, 0}, sample{3, 3}}),
				NewListSeriesIterator(samples{sample{1, 1}, sample{4, 4}}),
				NewListSeriesIterator(samples{sample{2, 2}, sample{5, 5}}),
			},
			seek:     2,
			expected: []tsdbutil.Sample{sample{2, 2}, sample{3, 3}, sample{4, 4}, sample{5, 5}},
		},
	} {
		merged := newChainSampleIterator(tc.input)
		actual := []tsdbutil.Sample{}
		if merged.Seek(tc.seek) {
			t, v := merged.At()
			actual = append(actual, sample{t, v})
		}
		s, err := ExpandSamples(merged)
		testutil.Ok(t, err)
		actual = append(actual, s...)
		testutil.Equals(t, tc.expected, actual)
	}
}

var result []tsdbutil.Sample

func makeSeriesSet(numSeries, numSamples int) SeriesSet {
	series := []Series{}
	for j := 0; j < numSeries; j++ {
		labels := labels.Labels{{Name: "foo", Value: fmt.Sprintf("bar%d", j)}}
		samples := []tsdbutil.Sample{}
		for k := 0; k < numSamples; k++ {
			samples = append(samples, sample{t: int64(k), v: float64(k)})
		}
		series = append(series, NewListSeries(labels, samples))
	}
	return NewMockSeriesSet(series...)
}

func makeMergeSeriesSet(numSeriesSets, numSeries, numSamples int) SeriesSet {
	seriesSets := []genericSeriesSet{}
	for i := 0; i < numSeriesSets; i++ {
		seriesSets = append(seriesSets, &genericSeriesSetAdapter{makeSeriesSet(numSeries, numSamples)})
	}
	return &seriesSetAdapter{newGenericMergeSeriesSet(seriesSets, nil, (&seriesMergerAdapter{VerticalSeriesMergeFunc: ChainingSeriesMerge}).Merge)}
}

func benchmarkDrain(seriesSet SeriesSet, b *testing.B) {
	var err error
	for n := 0; n < b.N; n++ {
		for seriesSet.Next() {
			result, err = ExpandSamples(seriesSet.At().Iterator())
			testutil.Ok(b, err)
		}
	}
}

func BenchmarkNoMergeSeriesSet_100_100(b *testing.B) {
	seriesSet := makeSeriesSet(100, 100)
	benchmarkDrain(seriesSet, b)
}

func BenchmarkMergeSeriesSet(b *testing.B) {
	for _, bm := range []struct {
		numSeriesSets, numSeries, numSamples int
	}{
		{1, 100, 100},
		{10, 100, 100},
		{100, 100, 100},
	} {
		seriesSet := makeMergeSeriesSet(bm.numSeriesSets, bm.numSeries, bm.numSamples)
		b.Run(fmt.Sprintf("%d_%d_%d", bm.numSeriesSets, bm.numSeries, bm.numSamples), func(b *testing.B) {
			benchmarkDrain(seriesSet, b)
		})
	}
}

// TODO: Incorporate this test from db_test.go to test our ChainSeriesMerge with those test cases.
//
//func TestVerticalCompaction(t *testing.T) {
//	cases := []struct {
//		blockSeries          [][]storage.Series
//		expSeries            map[string][]tsdbutil.Sample
//		expBlockNum          int
//		expOverlappingBlocks int
//	}{
//		// Case 0
//		// |--------------|
//		//        |----------------|
//		{
//			blockSeries: [][]storage.Series{
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{4, 0},
//						sample{5, 0}, sample{7, 0}, sample{8, 0}, sample{9, 0},
//					}),
//				},
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{3, 99}, sample{5, 99}, sample{6, 99}, sample{7, 99},
//						sample{8, 99}, sample{9, 99}, sample{10, 99}, sample{11, 99},
//						sample{12, 99}, sample{13, 99}, sample{14, 99},
//					}),
//				},
//			},
//			expSeries: map[string][]tsdbutil.Sample{`{a="b"}`: {
//				sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{3, 99},
//				sample{4, 0}, sample{5, 99}, sample{6, 99}, sample{7, 99},
//				sample{8, 99}, sample{9, 99}, sample{10, 99}, sample{11, 99},
//				sample{12, 99}, sample{13, 99}, sample{14, 99},
//			}},
//			expBlockNum:          1,
//			expOverlappingBlocks: 1,
//		},
//		// Case 1
//		// |-------------------------------|
//		//        |----------------|
//		{
//			blockSeries: [][]storage.Series{
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{4, 0},
//						sample{5, 0}, sample{7, 0}, sample{8, 0}, sample{9, 0},
//						sample{11, 0}, sample{13, 0}, sample{17, 0},
//					}),
//				},
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{3, 99}, sample{5, 99}, sample{6, 99}, sample{7, 99},
//						sample{8, 99}, sample{9, 99}, sample{10, 99},
//					}),
//				},
//			},
//			expSeries: map[string][]tsdbutil.Sample{`{a="b"}`: {
//				sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{3, 99},
//				sample{4, 0}, sample{5, 99}, sample{6, 99}, sample{7, 99},
//				sample{8, 99}, sample{9, 99}, sample{10, 99}, sample{11, 0},
//				sample{13, 0}, sample{17, 0},
//			}},
//			expBlockNum:          1,
//			expOverlappingBlocks: 1,
//		},
//		// Case 2
//		// |-------------------------------|
//		//        |------------|
//		//                           |--------------------|
//		{
//			blockSeries: [][]storage.Series{
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{4, 0},
//						sample{5, 0}, sample{7, 0}, sample{8, 0}, sample{9, 0},
//						sample{11, 0}, sample{13, 0}, sample{17, 0},
//					}),
//				},
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{3, 99}, sample{5, 99}, sample{6, 99}, sample{7, 99},
//						sample{8, 99}, sample{9, 99},
//					}),
//				},
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{14, 59}, sample{15, 59}, sample{17, 59}, sample{20, 59},
//						sample{21, 59}, sample{22, 59},
//					}),
//				},
//			},
//			expSeries: map[string][]tsdbutil.Sample{`{a="b"}`: {
//				sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{3, 99},
//				sample{4, 0}, sample{5, 99}, sample{6, 99}, sample{7, 99},
//				sample{8, 99}, sample{9, 99}, sample{11, 0}, sample{13, 0},
//				sample{14, 59}, sample{15, 59}, sample{17, 59}, sample{20, 59},
//				sample{21, 59}, sample{22, 59},
//			}},
//			expBlockNum:          1,
//			expOverlappingBlocks: 1,
//		},
//		// Case 3
//		// |-------------------|
//		//                           |--------------------|
//		//               |----------------|
//		{
//			blockSeries: [][]storage.Series{
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{4, 0},
//						sample{5, 0}, sample{8, 0}, sample{9, 0},
//					}),
//				},
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{14, 59}, sample{15, 59}, sample{17, 59}, sample{20, 59},
//						sample{21, 59}, sample{22, 59},
//					}),
//				},
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{5, 99}, sample{6, 99}, sample{7, 99}, sample{8, 99},
//						sample{9, 99}, sample{10, 99}, sample{13, 99}, sample{15, 99},
//						sample{16, 99}, sample{17, 99},
//					}),
//				},
//			},
//			expSeries: map[string][]tsdbutil.Sample{`{a="b"}`: {
//				sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{4, 0},
//				sample{5, 99}, sample{6, 99}, sample{7, 99}, sample{8, 99},
//				sample{9, 99}, sample{10, 99}, sample{13, 99}, sample{14, 59},
//				sample{15, 59}, sample{16, 99}, sample{17, 59}, sample{20, 59},
//				sample{21, 59}, sample{22, 59},
//			}},
//			expBlockNum:          1,
//			expOverlappingBlocks: 1,
//		},
//		// Case 4
//		// |-------------------------------------|
//		//            |------------|
//		//      |-------------------------|
//		{
//			blockSeries: [][]storage.Series{
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{4, 0},
//						sample{5, 0}, sample{8, 0}, sample{9, 0}, sample{10, 0},
//						sample{13, 0}, sample{15, 0}, sample{16, 0}, sample{17, 0},
//						sample{20, 0}, sample{22, 0},
//					}),
//				},
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{7, 59}, sample{8, 59}, sample{9, 59}, sample{10, 59},
//						sample{11, 59},
//					}),
//				},
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{3, 99}, sample{5, 99}, sample{6, 99}, sample{8, 99},
//						sample{9, 99}, sample{10, 99}, sample{13, 99}, sample{15, 99},
//						sample{16, 99}, sample{17, 99},
//					}),
//				},
//			},
//			expSeries: map[string][]tsdbutil.Sample{`{a="b"}`: {
//				sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{3, 99},
//				sample{4, 0}, sample{5, 99}, sample{6, 99}, sample{7, 59},
//				sample{8, 59}, sample{9, 59}, sample{10, 59}, sample{11, 59},
//				sample{13, 99}, sample{15, 99}, sample{16, 99}, sample{17, 99},
//				sample{20, 0}, sample{22, 0},
//			}},
//			expBlockNum:          1,
//			expOverlappingBlocks: 1,
//		},
//		// Case 5: series are merged properly when there are multiple series.
//		// |-------------------------------------|
//		//            |------------|
//		//      |-------------------------|
//		{
//			blockSeries: [][]storage.Series{
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{4, 0},
//						sample{5, 0}, sample{8, 0}, sample{9, 0}, sample{10, 0},
//						sample{13, 0}, sample{15, 0}, sample{16, 0}, sample{17, 0},
//						sample{20, 0}, sample{22, 0},
//					}),
//					newSeries(map[string]string{"b": "c"}, []tsdbutil.Sample{
//						sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{4, 0},
//						sample{5, 0}, sample{8, 0}, sample{9, 0}, sample{10, 0},
//						sample{13, 0}, sample{15, 0}, sample{16, 0}, sample{17, 0},
//						sample{20, 0}, sample{22, 0},
//					}),
//					newSeries(map[string]string{"c": "d"}, []tsdbutil.Sample{
//						sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{4, 0},
//						sample{5, 0}, sample{8, 0}, sample{9, 0}, sample{10, 0},
//						sample{13, 0}, sample{15, 0}, sample{16, 0}, sample{17, 0},
//						sample{20, 0}, sample{22, 0},
//					}),
//				},
//				{
//					newSeries(map[string]string{"__name__": "a"}, []tsdbutil.Sample{
//						sample{7, 59}, sample{8, 59}, sample{9, 59}, sample{10, 59},
//						sample{11, 59},
//					}),
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{7, 59}, sample{8, 59}, sample{9, 59}, sample{10, 59},
//						sample{11, 59},
//					}),
//					newSeries(map[string]string{"aa": "bb"}, []tsdbutil.Sample{
//						sample{7, 59}, sample{8, 59}, sample{9, 59}, sample{10, 59},
//						sample{11, 59},
//					}),
//					newSeries(map[string]string{"c": "d"}, []tsdbutil.Sample{
//						sample{7, 59}, sample{8, 59}, sample{9, 59}, sample{10, 59},
//						sample{11, 59},
//					}),
//				},
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{3, 99}, sample{5, 99}, sample{6, 99}, sample{8, 99},
//						sample{9, 99}, sample{10, 99}, sample{13, 99}, sample{15, 99},
//						sample{16, 99}, sample{17, 99},
//					}),
//					newSeries(map[string]string{"aa": "bb"}, []tsdbutil.Sample{
//						sample{3, 99}, sample{5, 99}, sample{6, 99}, sample{8, 99},
//						sample{9, 99}, sample{10, 99}, sample{13, 99}, sample{15, 99},
//						sample{16, 99}, sample{17, 99},
//					}),
//					newSeries(map[string]string{"c": "d"}, []tsdbutil.Sample{
//						sample{3, 99}, sample{5, 99}, sample{6, 99}, sample{8, 99},
//						sample{9, 99}, sample{10, 99}, sample{13, 99}, sample{15, 99},
//						sample{16, 99}, sample{17, 99},
//					}),
//				},
//			},
//			expSeries: map[string][]tsdbutil.Sample{
//				`{__name__="a"}`: {
//					sample{7, 59}, sample{8, 59}, sample{9, 59}, sample{10, 59},
//					sample{11, 59},
//				},
//				`{a="b"}`: {
//					sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{3, 99},
//					sample{4, 0}, sample{5, 99}, sample{6, 99}, sample{7, 59},
//					sample{8, 59}, sample{9, 59}, sample{10, 59}, sample{11, 59},
//					sample{13, 99}, sample{15, 99}, sample{16, 99}, sample{17, 99},
//					sample{20, 0}, sample{22, 0},
//				},
//				`{aa="bb"}`: {
//					sample{3, 99}, sample{5, 99}, sample{6, 99}, sample{7, 59},
//					sample{8, 59}, sample{9, 59}, sample{10, 59}, sample{11, 59},
//					sample{13, 99}, sample{15, 99}, sample{16, 99}, sample{17, 99},
//				},
//				`{b="c"}`: {
//					sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{4, 0},
//					sample{5, 0}, sample{8, 0}, sample{9, 0}, sample{10, 0},
//					sample{13, 0}, sample{15, 0}, sample{16, 0}, sample{17, 0},
//					sample{20, 0}, sample{22, 0},
//				},
//				`{c="d"}`: {
//					sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{3, 99},
//					sample{4, 0}, sample{5, 99}, sample{6, 99}, sample{7, 59},
//					sample{8, 59}, sample{9, 59}, sample{10, 59}, sample{11, 59},
//					sample{13, 99}, sample{15, 99}, sample{16, 99}, sample{17, 99},
//					sample{20, 0}, sample{22, 0},
//				},
//			},
//			expBlockNum:          1,
//			expOverlappingBlocks: 1,
//		},
//		// Case 6
//		// |--------------|
//		//        |----------------|
//		//                                         |--------------|
//		//                                                  |----------------|
//		{
//			blockSeries: [][]storage.Series{
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{4, 0},
//						sample{5, 0}, sample{7, 0}, sample{8, 0}, sample{9, 0},
//					}),
//				},
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{3, 99}, sample{5, 99}, sample{6, 99}, sample{7, 99},
//						sample{8, 99}, sample{9, 99}, sample{10, 99}, sample{11, 99},
//						sample{12, 99}, sample{13, 99}, sample{14, 99},
//					}),
//				},
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{20, 0}, sample{21, 0}, sample{22, 0}, sample{24, 0},
//						sample{25, 0}, sample{27, 0}, sample{28, 0}, sample{29, 0},
//					}),
//				},
//				{
//					newSeries(map[string]string{"a": "b"}, []tsdbutil.Sample{
//						sample{23, 99}, sample{25, 99}, sample{26, 99}, sample{27, 99},
//						sample{28, 99}, sample{29, 99}, sample{30, 99}, sample{31, 99},
//					}),
//				},
//			},
//			expSeries: map[string][]tsdbutil.Sample{`{a="b"}`: {
//				sample{0, 0}, sample{1, 0}, sample{2, 0}, sample{3, 99},
//				sample{4, 0}, sample{5, 99}, sample{6, 99}, sample{7, 99},
//				sample{8, 99}, sample{9, 99}, sample{10, 99}, sample{11, 99},
//				sample{12, 99}, sample{13, 99}, sample{14, 99},
//				sample{20, 0}, sample{21, 0}, sample{22, 0}, sample{23, 99},
//				sample{24, 0}, sample{25, 99}, sample{26, 99}, sample{27, 99},
//				sample{28, 99}, sample{29, 99}, sample{30, 99}, sample{31, 99},
//			}},
//			expBlockNum:          2,
//			expOverlappingBlocks: 2,
//		},
//	}
//
//	defaultMatcher := labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".*")
//	for _, c := range cases {
//		if ok := t.Run("", func(t *testing.T) {
//
//			tmpdir, err := ioutil.TempDir("", "data")
//			testutil.Ok(t, err)
//			defer func() {
//				testutil.Ok(t, os.RemoveAll(tmpdir))
//			}()
//
//			for _, series := range c.blockSeries {
//				createBlock(t, tmpdir, series)
//			}
//			opts := DefaultOptions()
//			opts.AllowOverlappingBlocks = true
//			db, err := Open(tmpdir, nil, nil, opts)
//			testutil.Ok(t, err)
//			defer func() {
//				testutil.Ok(t, db.Close())
//			}()
//			db.DisableCompactions()
//			testutil.Assert(t, len(db.blocks) == len(c.blockSeries), "Wrong number of blocks [before compact].")
//
//			// Vertical Query Merging test.
//			querier, err := db.Querier(context.TODO(), 0, 100)
//			testutil.Ok(t, err)
//			actSeries := query(t, querier, defaultMatcher)
//			testutil.Equals(t, c.expSeries, actSeries)
//
//			// Vertical compaction.
//			lc := db.compactor.(*LeveledCompactor)
//			testutil.Equals(t, 0, int(prom_testutil.ToFloat64(lc.metrics.overlappingBlocks)), "overlapping blocks count should be still 0 here")
//			err = db.Compact()
//			testutil.Ok(t, err)
//			testutil.Equals(t, c.expBlockNum, len(db.Blocks()), "Wrong number of blocks [after compact]")
//
//			testutil.Equals(t, c.expOverlappingBlocks, int(prom_testutil.ToFloat64(lc.metrics.overlappingBlocks)), "overlapping blocks count mismatch")
//
//			// Query test after merging the overlapping blocks.
//			querier, err = db.Querier(context.TODO(), 0, 100)
//			testutil.Ok(t, err)
//			actSeries = query(t, querier, defaultMatcher)
//			testutil.Equals(t, c.expSeries, actSeries)
//		}); !ok {
//			return
//		}
//	}
//}
//
// TODO: And those tests from querier_test
//
//func TestMergedSeriesSet(t *testing.T) {
//	cases := []struct {
//		// The input sets in order (samples in series in b are strictly
//		// after those in a).
//		a, b storage.SeriesSet
//		// The composition of a and b in the partition series set must yield
//		// results equivalent to the result series set.
//		exp storage.SeriesSet
//	}{
//		{
//			a: newMockSeriesSet([]storage.Series{
//				newListSeries(
//					labels.Labels{{"a", "a"}},
//					[]tsdbutil.Sample{
//						sample{t: 1, v: 1},
//					}),
//			}),
//			b: newMockSeriesSet([]storage.Series{
//				newListSeries(
//					labels.Labels{{"a", "a"}},
//					[]tsdbutil.Sample{
//						sample{t: 2, v: 2},
//					}),
//				newListSeries(
//					labels.Labels{{"b", "b"}},
//					[]tsdbutil.Sample{
//						sample{t: 1, v: 1},
//					}),
//			}),
//			exp: newMockSeriesSet([]storage.Series{
//				newListSeries(
//					labels.Labels{{"a", "a"}},
//					[]tsdbutil.Sample{
//						sample{t: 1, v: 1},
//						sample{t: 2, v: 2},
//					}),
//				newListSeries(
//					labels.Labels{{"b", "b"}},
//					[]tsdbutil.Sample{
//						sample{t: 1, v: 1},
//					}),
//			}),
//		},
//		{
//			a: newMockSeriesSet([]storage.Series{
//				newListSeries(
//					labels.Labels{
//						{"handler", "prometheus"},
//						{"instance", "127.0.0.1:9090"},
//					}, []tsdbutil.Sample{
//						sample{t: 1, v: 1},
//					}),
//				newListSeries(
//					labels.Labels{
//						{"handler", "prometheus"},
//						{"instance", "localhost:9090"},
//					}, []tsdbutil.Sample{
//						sample{t: 1, v: 2},
//					}),
//			}),
//			b: newMockSeriesSet([]storage.Series{
//				newListSeries(
//					labels.Labels{
//						{"handler", "prometheus"},
//						{"instance", "127.0.0.1:9090"},
//					}, []tsdbutil.Sample{
//						sample{t: 2, v: 1},
//					}),
//				newListSeries(
//					labels.Labels{
//						{"handler", "query"},
//						{"instance", "localhost:9090"},
//					}, []tsdbutil.Sample{
//						sample{t: 2, v: 2},
//					}),
//			}),
//			exp: newMockSeriesSet([]storage.Series{
//				newListSeries(
//					labels.Labels{
//						{"handler", "prometheus"},
//						{"instance", "127.0.0.1:9090"},
//					}, []tsdbutil.Sample{
//						sample{t: 1, v: 1},
//						sample{t: 2, v: 1},
//					}),
//				newListSeries(
//					labels.Labels{
//						{"handler", "prometheus"},
//						{"instance", "localhost:9090"},
//					}, []tsdbutil.Sample{
//						sample{t: 1, v: 2},
//					}),
//				newListSeries(
//					labels.Labels{
//						{"handler", "query"},
//						{"instance", "localhost:9090"},
//					}, []tsdbutil.Sample{
//						sample{t: 2, v: 2},
//					}),
//			}),
//		},
//	}
//
//Outer:
//	for _, c := range cases {
//		res := NewMergedSeriesSet([]storage.SeriesSet{c.a, c.b})
//
//		for {
//			eok, rok := c.exp.Next(), res.Next()
//			testutil.Equals(t, eok, rok)
//
//			if !eok {
//				continue Outer
//			}
//			sexp := c.exp.At()
//			sres := res.At()
//
//			testutil.Equals(t, sexp.Labels(), sres.Labels())
//
//			smplExp, errExp := expandSeriesIterator(sexp.Iterator())
//			smplRes, errRes := expandSeriesIterator(sres.Iterator())
//
//			testutil.Equals(t, errExp, errRes)
//			testutil.Equals(t, smplExp, smplRes)
//		}
//	}
//}

//
