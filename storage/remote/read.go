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

package remote

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type sampleAndChunkQueryableClient struct {
	client           ReadClient
	externalLabels   labels.Labels
	requiredMatchers []*labels.Matcher
	readRecent       bool
	callback         startTimeCallback
}

func (c *sampleAndChunkQueryableClient) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	q := &querier{
		ctx:              ctx,
		mint:             mint,
		maxt:             maxt,
		client:           c.client,
		externalLabels:   c.externalLabels,
		requiredMatchers: c.requiredMatchers,
	}
	if c.readRecent {
		return q, nil
	}

	var noop bool
	var err error
	q.maxt, noop, err = c.preferLocalStorage(mint, maxt)
	if err != nil {
		return nil, err
	}
	if noop {
		return storage.NoopQuerier(), nil
	}
	return q, nil
}

func (c *sampleAndChunkQueryableClient) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	cq := &chunkQuerier{
		querier: querier{
			ctx:              ctx,
			mint:             mint,
			maxt:             maxt,
			client:           c.client,
			externalLabels:   c.externalLabels,
			requiredMatchers: c.requiredMatchers,
		},
	}
	if c.readRecent {
		return cq, nil
	}

	var noop bool
	var err error
	cq.querier.maxt, noop, err = c.preferLocalStorage(mint, maxt)
	if err != nil {
		return nil, err
	}
	if noop {
		return storage.NoopChunkedQuerier(), nil
	}
	return cq, nil
}

// Prefer local storage allows to return noop if requested timeframe can be answered completely by the local TSDB, and
// reduces maxt if the timeframe can be partially answered by TSDB.
func (c *sampleAndChunkQueryableClient) preferLocalStorage(mint, maxt int64) (cmaxt int64, noop bool, err error) {
	localStartTime, err := c.callback()
	if err != nil {
		return 0, false, err
	}
	cmaxt = maxt

	// Avoid queries whose time range is later than the first timestamp in local DB.
	if mint > localStartTime {
		return 0, true, nil
	}
	// Query only samples older than the first timestamp in local DB.
	if maxt > localStartTime {
		cmaxt = localStartTime
	}
	return cmaxt, false, nil
}

// NewSampleAndChunkQueryableClient returns a storage.SampleAndChunkQueryable which queries the given client to select series sets.
func NewSampleAndChunkQueryableClient(
	c ReadClient,
	externalLabels labels.Labels,
	requiredMatchers []*labels.Matcher,
	readRecent bool,
	callback startTimeCallback,
) storage.SampleAndChunkQueryable {
	return &sampleAndChunkQueryableClient{
		client: c,

		externalLabels:   externalLabels,
		requiredMatchers: requiredMatchers,
		readRecent:       readRecent,
		callback:         callback,
	}
}

// querier is an adapter to make a client usable as a storage.Querier.
type querier struct {
	ctx        context.Context
	mint, maxt int64
	client     ReadClient

	// Derived from configuration.
	externalLabels   labels.Labels
	requiredMatchers []*labels.Matcher
}

// Select implements storage.Querier and uses the given matchers to read series sets from the client.
// Select also adds equality matchers for all external labels to the list of matchers before calling remote endpoint.
// The added external labels are removed from the returned series sets.
//
// If requiredMatchers are given, select returns a NoopSeriesSet if the given matchers don't match the label set of the
// requiredMatchers. Otherwise it'll just call remote endpoint.
func (q *querier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	if ms := q.requiredMatchers; len(ms) > 0 {
		for _, m := range matchers {
			for i, r := range ms {
				if m.Type == labels.MatchEqual && m.Name == r.Name && m.Value == r.Value {
					ms = append(ms[:i], ms[i+1:]...)
					break
				}
			}
			if len(ms) == 0 {
				break
			}
		}
		if len(ms) > 0 {
			return storage.NoopSeriesSet(), nil, nil
		}
	}

	m, added := q.addExternalLabels(matchers)
	query, err := ToQuery(q.mint, q.maxt, m, hints)
	if err != nil {
		return nil, nil, err
	}

	res, err := q.client.Read(q.ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("remote_read: %v", err)
	}

	return newSeriesSetFilter(FromQueryResult(sortSeries, res), added), nil, nil
}

// addExternalLabels adds matchers for each external label. External labels
// that already have a corresponding user-supplied matcher are skipped, as we
// assume that the user explicitly wants to select a different value for them.
// We return the new set of matchers, along with a map of labels for which
// matchers were added, so that these can later be removed from the result
// time series again.
func (q querier) addExternalLabels(ms []*labels.Matcher) ([]*labels.Matcher, labels.Labels) {
	el := make(labels.Labels, len(q.externalLabels))
	copy(el, q.externalLabels)

	// ms won't be sorted, so have to O(n^2) the search.
	for _, m := range ms {
		for i := 0; i < len(el); {
			if el[i].Name == m.Name {
				el = el[:i+copy(el[i:], el[i+1:])]
				continue
			}
			i++
		}
	}

	for _, l := range el {
		m, err := labels.NewMatcher(labels.MatchEqual, l.Name, l.Value)
		if err != nil {
			panic(err)
		}
		ms = append(ms, m)
	}
	return ms, el
}

// LabelValues implements storage.Querier and is a noop.
func (q *querier) LabelValues(string) ([]string, storage.Warnings, error) {
	// TODO implement?
	return nil, nil, nil
}

// LabelNames implements storage.Querier and is a noop.
func (q *querier) LabelNames() ([]string, storage.Warnings, error) {
	// TODO implement?
	return nil, nil, nil
}

// Close implements storage.Querier and is a noop.
func (q *querier) Close() error {
	return nil
}

// chunkQuerier is an adapter to make a client usable as a storage.ChunkQuerier.
type chunkQuerier struct {
	querier
}

// Select implements storage.ChunkQuerier and uses the given matchers to read chunk series sets from the client.
// It uses remote.querier.Select so it supports external labels and required matchers if specified.
func (q *chunkQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) (storage.ChunkSeriesSet, storage.Warnings, error) {
	ss, w, err := q.querier.Select(sortSeries, hints, matchers...)
	if err != nil {
		return nil, w, err
	}

	// TODO(bwplotka) Support remote read chunked and allow returning chunks directly (TODO ticket).
	return storage.NewSeriesSetToChunkSet(ss), w, nil
}

func newSeriesSetFilter(ss storage.SeriesSet, toFilter labels.Labels) storage.SeriesSet {
	return &seriesSetFilter{
		SeriesSet: ss,
		toFilter:  toFilter,
	}
}

type seriesSetFilter struct {
	storage.SeriesSet
	toFilter labels.Labels
	querier  storage.Querier
}

func (ssf *seriesSetFilter) GetQuerier() storage.Querier {
	return ssf.querier
}

func (ssf *seriesSetFilter) SetQuerier(querier storage.Querier) {
	ssf.querier = querier
}

func (ssf seriesSetFilter) At() storage.Series {
	return seriesFilter{
		Series:   ssf.SeriesSet.At(),
		toFilter: ssf.toFilter,
	}
}

type seriesFilter struct {
	storage.Series
	toFilter labels.Labels
}

func (sf seriesFilter) Labels() labels.Labels {
	labels := sf.Series.Labels()
	for i, j := 0, 0; i < len(labels) && j < len(sf.toFilter); {
		if labels[i].Name < sf.toFilter[j].Name {
			i++
		} else if labels[i].Name > sf.toFilter[j].Name {
			j++
		} else {
			labels = labels[:i+copy(labels[i:], labels[i+1:])]
			j++
		}
	}
	return labels
}
