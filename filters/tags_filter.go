// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package filters

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/m3db/m3metrics/metric/id"
)

// tagFilter is a filter associated with a given tag.
type tagFilter struct {
	name        []byte
	valueFilter Filter
}

func (f *tagFilter) String() string {
	return fmt.Sprintf("%s:%s", string(f.name), f.valueFilter.String())
}

type tagFiltersByNameAsc []tagFilter

func (tn tagFiltersByNameAsc) Len() int           { return len(tn) }
func (tn tagFiltersByNameAsc) Swap(i, j int)      { tn[i], tn[j] = tn[j], tn[i] }
func (tn tagFiltersByNameAsc) Less(i, j int) bool { return bytes.Compare(tn[i].name, tn[j].name) < 0 }

// TagsFilterOptions provide a set of tag filter options.
type TagsFilterOptions struct {
	// Name of the name tag.
	NameTagName []byte

	// Function to extract name and tags from an id.
	NameAndTagsFn id.NameAndTagsFn

	// Function to create a new sorted tag iterator from id tags.
	SortedTagIteratorFn id.SortedTagIteratorFn
}

// tagsFilter contains a list of tag filters.
type tagsFilter struct {
	nameFilter Filter
	tagFilters []tagFilter
	op         LogicalOp
	opts       TagsFilterOptions
}

// NewTagsFilter creates a new tags filter.
func NewTagsFilter(
	filters map[string]string,
	op LogicalOp,
	opts TagsFilterOptions,
) (Filter, error) {
	var (
		nameFilter Filter
		tagFilters = make([]tagFilter, 0, len(filters))
	)
	for name, value := range filters {
		valFilter, err := NewFilter([]byte(value))
		if err != nil {
			return nil, err
		}
		bName := []byte(name)
		if bytes.Equal(opts.NameTagName, bName) {
			nameFilter = valFilter
		} else {
			tagFilters = append(tagFilters, tagFilter{
				name:        bName,
				valueFilter: valFilter,
			})
		}
	}
	sort.Sort(tagFiltersByNameAsc(tagFilters))
	return &tagsFilter{
		nameFilter: nameFilter,
		tagFilters: tagFilters,
		op:         op,
		opts:       opts,
	}, nil
}

func (f *tagsFilter) String() string {
	separator := " " + string(f.op) + " "
	var buf bytes.Buffer
	numTagFilters := len(f.tagFilters)
	if f.nameFilter != nil {
		buf.WriteString(fmt.Sprintf("%s:%s", f.opts.NameTagName, f.nameFilter.String()))
		if numTagFilters > 0 {
			buf.WriteString(separator)
		}
	}
	for i := 0; i < numTagFilters; i++ {
		buf.WriteString(f.tagFilters[i].String())
		if i < numTagFilters-1 {
			buf.WriteString(separator)
		}
	}
	return buf.String()
}

func (f *tagsFilter) Matches(id []byte) bool {
	if f.nameFilter == nil && len(f.tagFilters) == 0 {
		return true
	}

	name, tags, err := f.opts.NameAndTagsFn(id)
	if err != nil {
		return false
	}
	if f.nameFilter != nil && !f.nameFilter.Matches(name) {
		return false
	}

	iter := f.opts.SortedTagIteratorFn(tags)
	defer iter.Close()

	currIdx := 0
	for iter.Next() && currIdx < len(f.tagFilters) {
		name, value := iter.Current()

		comparison := bytes.Compare(name, f.tagFilters[currIdx].name)
		if comparison < 0 {
			continue
		}

		if comparison > 0 {
			if f.op == Conjunction {
				// For AND, if the current filter tag doesn't exist, bail immediately.
				return false
			}

			// Iterate tagFilters for the OR case.
			currIdx++
			for currIdx < len(f.tagFilters) && bytes.Compare(name, f.tagFilters[currIdx].name) > 0 {
				currIdx++
			}

			if currIdx == len(f.tagFilters) {
				// Past all tagFilters.
				return false
			}

			if bytes.Compare(name, f.tagFilters[currIdx].name) < 0 {
				continue
			}
		}

		match := f.tagFilters[currIdx].valueFilter.Matches(value)
		if match && f.op == Disjunction {
			return true
		}

		if !match && f.op == Conjunction {
			return false
		}

		currIdx++
	}

	if iter.Err() != nil || f.op == Disjunction {
		return false
	}

	return currIdx == len(f.tagFilters)
}
