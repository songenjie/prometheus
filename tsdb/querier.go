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

package tsdb

import (
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

type labelQueriers interface {
	Len() int
	Get(i int) storage.LabelQuerier
	SplitByHalf() (labelQueriers, labelQueriers)
}

type labelSampleQueriers []storage.Querier

func (l labelSampleQueriers) Len() int                       { return len(l) }
func (l labelSampleQueriers) Get(i int) storage.LabelQuerier { return l[i] }
func (l labelSampleQueriers) SplitByHalf() (labelQueriers, labelQueriers) {
	i := len(l) / 2
	return l[:i], l[i:]
}

type labelChunkQueriers []storage.ChunkQuerier

func (l labelChunkQueriers) Len() int                       { return len(l) }
func (l labelChunkQueriers) Get(i int) storage.LabelQuerier { return l[i] }
func (l labelChunkQueriers) SplitByHalf() (labelQueriers, labelQueriers) {
	i := len(l) / 2
	return l[:i], l[i:]
}

type baseQuerier struct {
	qs labelQueriers
}

func (q *baseQuerier) LabelValues(n string) ([]string, storage.Warnings, error) {
	return q.lvals(q.qs, n)
}

func (q *baseQuerier) lvals(lq labelQueriers, n string) ([]string, storage.Warnings, error) {
	if lq.Len() == 0 {
		return nil, nil, nil
	}
	if lq.Len() == 1 {
		return lq.Get(0).LabelValues(n)
	}
	a, b := lq.SplitByHalf()

	var ws storage.Warnings
	s1, w, err := q.lvals(a, n)
	ws = append(ws, w...)
	if err != nil {
		return nil, ws, err
	}
	s2, ws, err := q.lvals(b, n)
	ws = append(ws, w...)
	if err != nil {
		return nil, ws, err
	}
	return mergeStrings(s1, s2), ws, nil
}

// LabelNames returns all the unique label names present querier blocks.
func (q *baseQuerier) LabelNames() ([]string, storage.Warnings, error) {
	labelNamesMap := make(map[string]struct{})
	var ws storage.Warnings
	for i := 0; i < q.qs.Len(); i++ {
		names, w, err := q.qs.Get(i).LabelNames()
		ws = append(ws, w...)
		if err != nil {
			return nil, ws, errors.Wrap(err, "LabelNames() from Querier")
		}
		for _, name := range names {
			labelNamesMap[name] = struct{}{}
		}
	}

	labelNames := make([]string, 0, len(labelNamesMap))
	for name := range labelNamesMap {
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)

	return labelNames, ws, nil
}

func (q *baseQuerier) Close() error {
	var merr tsdb_errors.MultiError

	for i := 0; i < q.qs.Len(); i++ {
		merr.Add(q.qs.Get(i).Close())
	}
	return merr.Err()
}

// querier aggregates querying results from time blocks within
// a single partition.
type querier struct {
	baseQuerier

	blocks []storage.Querier
}

func newQuerier(blocks []storage.Querier) storage.Querier {
	return &querier{
		baseQuerier: baseQuerier{qs: labelSampleQueriers(blocks)},
		blocks:      blocks,
	}
}

func (q *querier) Select(sortSeries bool, hints *storage.SelectHints, ms ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	if len(q.blocks) == 0 {
		return storage.EmptySeriesSet(), nil, nil
	}
	if len(q.blocks) == 1 {
		// Sorting Head series is slow, and unneeded when only the
		// Head is being queried.
		return q.blocks[0].Select(sortSeries, hints, ms...)
	}

	ss := make([]storage.SeriesSet, len(q.blocks))
	var ws storage.Warnings
	for i, b := range q.blocks {
		// We have to sort if blocks > 1 as MergedSeriesSet requires it.
		s, w, err := b.Select(true, hints, ms...)
		ws = append(ws, w...)
		if err != nil {
			return nil, ws, err
		}
		ss[i] = s
	}

	return storage.NewMergeSeriesSet(ss, storage.ChainingSeriesMerge), ws, nil
}

// chunkQuerier aggregates querying results from time blocks within
// a single partition.
type chunkQuerier struct {
	baseQuerier

	blocks []storage.ChunkQuerier
}

func newChunkQuerier(blocks []storage.ChunkQuerier) storage.ChunkQuerier {
	return &chunkQuerier{
		baseQuerier: baseQuerier{qs: labelChunkQueriers(blocks)},
		blocks:      blocks,
	}
}

func (q *chunkQuerier) Select(sortSeries bool, hints *storage.SelectHints, ms ...*labels.Matcher) (storage.ChunkSeriesSet, storage.Warnings, error) {
	if len(q.blocks) == 0 {
		return storage.EmptyChunkSeriesSet(), nil, nil
	}
	if len(q.blocks) == 1 {
		// Sorting Head series is slow, and unneeded when only the
		// Head is being queried.
		return q.blocks[0].Select(sortSeries, hints, ms...)
	}

	ss := make([]storage.ChunkSeriesSet, len(q.blocks))
	var ws storage.Warnings
	for i, b := range q.blocks {
		// We have to sort if blocks > 1 as MergedSeriesSet requires it.
		s, w, err := b.Select(true, hints, ms...)
		ws = append(ws, w...)
		if err != nil {
			return nil, ws, err
		}
		ss[i] = s
	}

	return storage.NewMergeChunkSeriesSet(ss, storage.NewCompactingChunkSeriesMerger(storage.ChainingSeriesMerge)), ws, nil
}

// NewBlockQuerier returns a chunk querier against the reader.
func NewBlockQuerier(b BlockReader, mint, maxt int64) (storage.Querier, error) {
	// Since everything inside block is in chunks, just use ChunkQuerier and convert to Querier.
	q, err := NewBlockChunkQuerier(b, mint, maxt)
	if err != nil {
		return nil, err
	}
	return &blockQuerier{ChunkQuerier: q}, nil
}

type blockQuerier struct {
	storage.ChunkQuerier
}

func (q *blockQuerier) Select(sortSeries bool, hints *storage.SelectHints, ms ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	ss, w, err := q.ChunkQuerier.Select(sortSeries, hints, ms...)
	if err != nil {
		return nil, w, err
	}
	return storage.NewSeriesSetFromChunkSeriesSet(ss), w, nil
}

// NewBlockChunkQuerier returns a chunk querier against the reader.
func NewBlockChunkQuerier(b BlockReader, mint, maxt int64) (storage.ChunkQuerier, error) {
	indexr, err := b.Index(mint, maxt)
	if err != nil {
		return nil, errors.Wrapf(err, "open index reader")
	}
	chunkr, err := b.Chunks()
	if err != nil {
		indexr.Close()
		return nil, errors.Wrapf(err, "open chunk reader")
	}
	tombsr, err := b.Tombstones()
	if err != nil {
		indexr.Close()
		chunkr.Close()
		return nil, errors.Wrapf(err, "open tombstone reader")
	}
	return &blockChunkQuerier{
		mint:       mint,
		maxt:       maxt,
		index:      indexr,
		chunks:     chunkr,
		tombstones: tombsr,
	}, nil
}

// blockChunkQuerier provides chunk querying access to a single block database.
type blockChunkQuerier struct {
	index      IndexReader
	chunks     ChunkReader
	tombstones tombstones.Reader

	closed bool

	mint, maxt int64
}

func (q *blockChunkQuerier) Select(sortSeries bool, hints *storage.SelectHints, ms ...*labels.Matcher) (storage.ChunkSeriesSet, storage.Warnings, error) {
	mint := q.mint
	maxt := q.maxt
	if hints != nil {
		mint = hints.Start
		maxt = hints.End
	}

	ss, err := LookupChunkSeries(sortSeries, q.index, q.tombstones, q.chunks, mint, maxt, ms...)
	return ss, nil, err
}

func (q *blockChunkQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	res, err := q.index.LabelValues(name)
	return res, nil, err
}

func (q *blockChunkQuerier) LabelNames() ([]string, storage.Warnings, error) {
	res, err := q.index.LabelNames()
	return res, nil, err
}

func (q *blockChunkQuerier) Close() error {
	if q.closed {
		return errors.New("block querier already closed")
	}

	var merr tsdb_errors.MultiError
	merr.Add(q.index.Close())
	merr.Add(q.chunks.Close())
	merr.Add(q.tombstones.Close())
	q.closed = true
	return merr.Err()
}

// Bitmap used by func isRegexMetaCharacter to check whether a character needs to be escaped.
var regexMetaCharacterBytes [16]byte

// isRegexMetaCharacter reports whether byte b needs to be escaped.
func isRegexMetaCharacter(b byte) bool {
	return b < utf8.RuneSelf && regexMetaCharacterBytes[b%16]&(1<<(b/16)) != 0
}

func init() {
	for _, b := range []byte(`.+*?()|[]{}^$`) {
		regexMetaCharacterBytes[b%16] |= 1 << (b / 16)
	}
}

func findSetMatches(pattern string) []string {
	// Return empty matches if the wrapper from Prometheus is missing.
	if len(pattern) < 6 || pattern[:4] != "^(?:" || pattern[len(pattern)-2:] != ")$" {
		return nil
	}
	escaped := false
	sets := []*strings.Builder{{}}
	for i := 4; i < len(pattern)-2; i++ {
		if escaped {
			switch {
			case isRegexMetaCharacter(pattern[i]):
				sets[len(sets)-1].WriteByte(pattern[i])
			case pattern[i] == '\\':
				sets[len(sets)-1].WriteByte('\\')
			default:
				return nil
			}
			escaped = false
		} else {
			switch {
			case isRegexMetaCharacter(pattern[i]):
				if pattern[i] == '|' {
					sets = append(sets, &strings.Builder{})
				} else {
					return nil
				}
			case pattern[i] == '\\':
				escaped = true
			default:
				sets[len(sets)-1].WriteByte(pattern[i])
			}
		}
	}
	matches := make([]string, 0, len(sets))
	for _, s := range sets {
		if s.Len() > 0 {
			matches = append(matches, s.String())
		}
	}
	return matches
}

// PostingsForMatchers assembles a single postings iterator against the index reader
// based on the given matchers. The resulting postings are not ordered by series.
func PostingsForMatchers(ix IndexReader, ms ...*labels.Matcher) (index.Postings, error) {
	var its, notIts []index.Postings
	// See which label must be non-empty.
	// Optimization for case like {l=~".", l!="1"}.
	labelMustBeSet := make(map[string]bool, len(ms))
	for _, m := range ms {
		if !m.Matches("") {
			labelMustBeSet[m.Name] = true
		}
	}

	for _, m := range ms {
		if labelMustBeSet[m.Name] {
			// If this matcher must be non-empty, we can be smarter.
			matchesEmpty := m.Matches("")
			isNot := m.Type == labels.MatchNotEqual || m.Type == labels.MatchNotRegexp
			if isNot && matchesEmpty { // l!="foo"
				// If the label can't be empty and is a Not and the inner matcher
				// doesn't match empty, then subtract it out at the end.
				inverse, err := m.Inverse()
				if err != nil {
					return nil, err
				}

				it, err := postingsForMatcher(ix, inverse)
				if err != nil {
					return nil, err
				}
				notIts = append(notIts, it)
			} else if isNot && !matchesEmpty { // l!=""
				// If the label can't be empty and is a Not, but the inner matcher can
				// be empty we need to use inversePostingsForMatcher.
				inverse, err := m.Inverse()
				if err != nil {
					return nil, err
				}

				it, err := inversePostingsForMatcher(ix, inverse)
				if err != nil {
					return nil, err
				}
				its = append(its, it)
			} else { // l="a"
				// Non-Not matcher, use normal postingsForMatcher.
				it, err := postingsForMatcher(ix, m)
				if err != nil {
					return nil, err
				}
				its = append(its, it)
			}
		} else { // l=""
			// If the matchers for a labelname selects an empty value, it selects all
			// the series which don't have the label name set too. See:
			// https://github.com/prometheus/prometheus/issues/3575 and
			// https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
			it, err := inversePostingsForMatcher(ix, m)
			if err != nil {
				return nil, err
			}
			notIts = append(notIts, it)
		}
	}

	// If there's nothing to subtract from, add in everything and remove the notIts later.
	if len(its) == 0 && len(notIts) != 0 {
		k, v := index.AllPostingsKey()
		allPostings, err := ix.Postings(k, v)
		if err != nil {
			return nil, err
		}
		its = append(its, allPostings)
	}

	it := index.Intersect(its...)

	for _, n := range notIts {
		it = index.Without(it, n)
	}

	return it, nil
}

func postingsForMatcher(ix IndexReader, m *labels.Matcher) (index.Postings, error) {
	// This method will not return postings for missing labels.

	// Fast-path for equal matching.
	if m.Type == labels.MatchEqual {
		return ix.Postings(m.Name, m.Value)
	}

	// Fast-path for set matching.
	if m.Type == labels.MatchRegexp {
		setMatches := findSetMatches(m.GetRegexString())
		if len(setMatches) > 0 {
			sort.Strings(setMatches)
			return ix.Postings(m.Name, setMatches...)
		}
	}

	vals, err := ix.LabelValues(m.Name)
	if err != nil {
		return nil, err
	}

	var res []string
	for _, val := range vals {
		if m.Matches(val) {
			res = append(res, val)
		}
	}

	if len(res) == 0 {
		return index.EmptyPostings(), nil
	}

	return ix.Postings(m.Name, res...)
}

// inversePostingsForMatcher returns the postings for the series with the label name set but not matching the matcher.
func inversePostingsForMatcher(ix IndexReader, m *labels.Matcher) (index.Postings, error) {
	vals, err := ix.LabelValues(m.Name)
	if err != nil {
		return nil, err
	}

	var res []string
	for _, val := range vals {
		if !m.Matches(val) {
			res = append(res, val)
		}
	}

	return ix.Postings(m.Name, res...)
}

func mergeStrings(a, b []string) []string {
	maxl := len(a)
	if len(b) > len(a) {
		maxl = len(b)
	}
	res := make([]string, 0, maxl*10/9)

	for len(a) > 0 && len(b) > 0 {
		d := strings.Compare(a[0], b[0])

		if d == 0 {
			res = append(res, a[0])
			a, b = a[1:], b[1:]
		} else if d < 0 {
			res = append(res, a[0])
			a = a[1:]
		} else if d > 0 {
			res = append(res, b[0])
			b = b[1:]
		}
	}

	// Append all remaining elements.
	res = append(res, a...)
	res = append(res, b...)
	return res
}

// LookupChunkSeries retrieves all series for the given matchers and returns a ChunkSeriesSet
// over them. It drops chunks based on tombstones in the given reader.
func LookupChunkSeries(sorted bool, ir IndexReader, tr tombstones.Reader, c ChunkReader, minTime, maxTime int64, ms ...*labels.Matcher) (storage.ChunkSeriesSet, error) {
	if tr == nil {
		tr = tombstones.NewMemTombstones()
	}
	p, err := PostingsForMatchers(ir, ms...)
	if err != nil {
		return nil, err
	}
	if sorted {
		p = ir.SortedPostings(p)
	}
	return newBlockChunkSeriesSet(ir, c, tr, p, minTime, maxTime), nil
}

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type chunkSeriesIterator struct {
	chunks []chunks.Meta

	i          int
	cur        chunkenc.Iterator
	bufDelIter *deletedIterator

	maxt, mint int64

	intervals tombstones.Intervals
}

func newChunkSeriesIterator(cs []chunks.Meta, dranges tombstones.Intervals, mint, maxt int64) *chunkSeriesIterator {
	csi := &chunkSeriesIterator{
		chunks: cs,
		i:      0,

		mint: mint,
		maxt: maxt,

		intervals: dranges,
	}
	csi.resetCurIterator()

	return csi
}

func (it *chunkSeriesIterator) resetCurIterator() {
	if len(it.intervals) == 0 {
		it.cur = it.chunks[it.i].Chunk.Iterator(it.cur)
		return
	}
	if it.bufDelIter == nil {
		it.bufDelIter = &deletedIterator{
			intervals: it.intervals,
		}
	}
	it.bufDelIter.it = it.chunks[it.i].Chunk.Iterator(it.bufDelIter.it)
	it.cur = it.bufDelIter
}

func (it *chunkSeriesIterator) Seek(t int64) (ok bool) {
	if t > it.maxt {
		return false
	}

	// Seek to the first valid value after t.
	if t < it.mint {
		t = it.mint
	}

	for ; it.chunks[it.i].MaxTime < t; it.i++ {
		if it.i == len(it.chunks)-1 {
			return false
		}
	}

	it.resetCurIterator()

	for it.cur.Next() {
		t0, _ := it.cur.At()
		if t0 >= t {
			return true
		}
	}
	return false
}

func (it *chunkSeriesIterator) At() (t int64, v float64) {
	return it.cur.At()
}

func (it *chunkSeriesIterator) Next() bool {
	if it.cur.Next() {
		t, _ := it.cur.At()

		if t < it.mint {
			if !it.Seek(it.mint) {
				return false
			}
			t, _ = it.At()

			return t <= it.maxt
		}
		if t > it.maxt {
			return false
		}
		return true
	}
	if err := it.cur.Err(); err != nil {
		return false
	}
	if it.i == len(it.chunks)-1 {
		return false
	}

	it.i++
	it.resetCurIterator()

	return it.Next()
}

func (it *chunkSeriesIterator) Err() error {
	return it.cur.Err()
}

// deletedIterator wraps an Iterator and makes sure any deleted metrics are not
// returned.
type deletedIterator struct {
	it chunkenc.Iterator

	intervals tombstones.Intervals
}

func (it *deletedIterator) At() (int64, float64) {
	return it.it.At()
}

func (it *deletedIterator) Seek(t int64) bool {
	if it.it.Err() != nil {
		return false
	}
	return it.it.Seek(t)
}

func (it *deletedIterator) Next() bool {
Outer:
	for it.it.Next() {
		ts, _ := it.it.At()

		for _, tr := range it.intervals {
			if tr.InBounds(ts) {
				continue Outer
			}

			if ts <= tr.Maxt {
				return true

			}
			it.intervals = it.intervals[1:]
		}
		return true
	}
	return false
}

func (it *deletedIterator) Err() error { return it.it.Err() }
