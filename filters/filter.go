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
	"errors"
	"fmt"
)

var (
	errInvalidFilterPattern = errors.New("invalid filter pattern defined")

	_matchAllFilter               Filter      = matchAllFilter{}
	_matchNoneFilter              Filter      = matchNoneFilter{}
	_timestampFilter              Filter      = timestampFilter{}
	_uuidFilter                   Filter      = uuidFilter{}
	_singleAnyCharFilterForwards  chainFilter = &singleAnyCharFilter{backwards: false}
	_singleAnyCharFilterBackwards chainFilter = &singleAnyCharFilter{backwards: true}
)

// LogicalOp is a logical operator.
type LogicalOp string

// chainSegment is the part of the pattern that the chain represents.
type chainSegment int

// A list of supported logical operators.
const (
	// Conjunction is logical AND.
	Conjunction LogicalOp = "&&"
	// Disjunction is logical OR.
	Disjunction LogicalOp = "||"

	middle chainSegment = iota
	start
	end

	wildcardChar         = '*'
	negationChar         = '!'
	singleAnyChar        = '?'
	singleRangeStartChar = '['
	singleRangeEndChar   = ']'
	rangeChar            = '-'
	multiRangeStartChar  = '{'
	multiRangeEndChar    = '}'
	invalidNestedChars   = "?[{"
)

var (
	multiRangeSplit = []byte(",")
)

// Filter matches a string against certain conditions. All Filters must be
// goroutine safe.
type Filter interface {
	fmt.Stringer

	// Matches returns true if the conditions are met.
	Matches(val []byte) bool
}

// NewFilter supports startsWith, endsWith, contains and a single wildcard
// along with negation and glob matching support.
// NOTE: Currently only supports ASCII matching and has zero compatibility
// with UTF8 so you should make sure all matches are done against ASCII only.
func NewFilter(pattern []byte) (Filter, error) {
	// TODO(martinm): Provide more detailed error messages.
	if len(pattern) == 0 {
		return newEqualityFilter(pattern), nil
	}

	if pattern[0] != negationChar {
		return newWildcardFilter(pattern)
	}

	if len(pattern) == 1 {
		// Only negation symbol.
		return nil, errInvalidFilterPattern
	}

	filter, err := newWildcardFilter(pattern[1:])
	if err != nil {
		return nil, err
	}

	return newNegationFilter(filter), nil
}

// newWildcardFilter creates a filter that segments the pattern based
// on wildcards, creating a rangeFilter for each segment.
func newWildcardFilter(pattern []byte) (Filter, error) {
	wIdx := bytes.IndexRune(pattern, wildcardChar)

	if wIdx == -1 {
		// No wildcards.
		return newRangeFilter(pattern, false, middle)
	}

	if len(pattern) == 1 {
		// Whole thing is wildcard.
		return NewMatchAllFilter(), nil
	}

	if wIdx == len(pattern)-1 {
		// Single wildcard at end.
		return newRangeFilter(pattern[:len(pattern)-1], false, start)
	}

	secondWIdx := bytes.IndexRune(pattern[wIdx+1:], wildcardChar)
	if secondWIdx == -1 {
		if wIdx == 0 {
			// Single wildcard at start.
			return newRangeFilter(pattern[1:], true, end)
		}

		// Single wildcard in the middle.
		first, err := newRangeFilter(pattern[:wIdx], false, start)
		if err != nil {
			return nil, err
		}

		second, err := newRangeFilter(pattern[wIdx+1:], true, end)
		if err != nil {
			return nil, err
		}

		return NewMultiFilter([]Filter{first, second}, Conjunction), nil
	}

	if wIdx == 0 && secondWIdx == len(pattern)-2 && len(pattern) > 2 {
		// Wildcard at beginning and end.
		return newContainsFilter(pattern[1 : len(pattern)-1])
	}

	return nil, errInvalidFilterPattern
}

// newRangeFilter creates a filter that checks for ranges (? or [] or {}) and segments
// the pattern into a multiple chain filters based on ranges found.
func newRangeFilter(pattern []byte, backwards bool, seg chainSegment) (Filter, error) {
	var filters []chainFilter
	eqIdx := -1
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == singleRangeStartChar {
			// Found '[', create an equality filter for the chars before this one if any
			// and use vals before next ']' as input for a singleRangeFilter.
			if eqIdx != -1 {
				filters = append(filters, newEqualityChainFilter(pattern[eqIdx:i], backwards))
				eqIdx = -1
			}

			endIdx := bytes.IndexRune(pattern[i+1:], singleRangeEndChar)
			if endIdx == -1 {
				return nil, errInvalidFilterPattern
			}

			f, err := newSingleRangeFilter(pattern[i+1:i+1+endIdx], backwards)
			if err != nil {
				return nil, errInvalidFilterPattern
			}

			filters = append(filters, f)
			i += endIdx + 1
		} else if pattern[i] == multiRangeStartChar {
			// Found '{', create equality filter for chars before this if any and then
			// use vals before next '}' to create multiCharRange filter.
			if eqIdx != -1 {
				filters = append(filters, newEqualityChainFilter(pattern[eqIdx:i], backwards))
				eqIdx = -1
			}

			endIdx := bytes.IndexRune(pattern[i+1:], multiRangeEndChar)
			if endIdx == -1 {
				return nil, errInvalidFilterPattern
			}

			f, err := newMultiCharSequenceFilter(pattern[i+1:i+1+endIdx], backwards)
			if err != nil {
				return nil, errInvalidFilterPattern
			}

			filters = append(filters, f)
			i += endIdx + 1
		} else if pattern[i] == singleAnyChar {
			// Found '?', create equality filter for chars before this one if any and then
			// attach singleAnyCharFilter to chain.
			if eqIdx != -1 {
				filters = append(filters, newEqualityChainFilter(pattern[eqIdx:i], backwards))
				eqIdx = -1
			}

			filters = append(filters, newSingleAnyCharFilter(backwards))
		} else if eqIdx == -1 {
			// Normal char, need to mark index to start next equality filter.
			eqIdx = i
		}
	}

	if eqIdx != -1 {
		filters = append(filters, newEqualityChainFilter(pattern[eqIdx:], backwards))
	}

	return newMultiChainFilter(filters, seg, backwards), nil
}

// equalityFilter is a filter that matches exact values.
type equalityFilter struct {
	pattern []byte
}

func newEqualityFilter(pattern []byte) Filter {
	return &equalityFilter{pattern: pattern}
}

func (f *equalityFilter) String() string {
	return "Equals(\"" + string(f.pattern) + "\")"
}

func (f *equalityFilter) Matches(val []byte) bool {
	return bytes.Equal(f.pattern, val)
}

// containsFilter is a filter that performs contains matches.
type containsFilter struct {
	pattern []byte
}

func newContainsFilter(pattern []byte) (Filter, error) {
	if bytes.ContainsAny(pattern, invalidNestedChars) {
		return nil, errInvalidFilterPattern
	}

	return &containsFilter{pattern: pattern}, nil
}

func (f *containsFilter) String() string {
	return "Contains(\"" + string(f.pattern) + "\")"
}

func (f *containsFilter) Matches(val []byte) bool {
	return bytes.Contains(val, f.pattern)
}

// negationFilter is a filter that matches the opposite of the provided filter.
type negationFilter struct {
	filter Filter
}

func newNegationFilter(filter Filter) Filter {
	return &negationFilter{filter: filter}
}

func (f *negationFilter) String() string {
	return "Not(" + f.filter.String() + ")"
}

func (f *negationFilter) Matches(val []byte) bool {
	return !f.filter.Matches(val)
}

// matchAllFilter is a filter that matches all input.
type matchAllFilter struct{}

// NewMatchAllFilter returns a filter that matches all input.
func NewMatchAllFilter() Filter                  { return _matchAllFilter }
func (f matchAllFilter) String() string          { return "All" }
func (f matchAllFilter) Matches(val []byte) bool { return true }

// matchNoneFilter is a filter that does not match any input.
type matchNoneFilter struct{}

// NewMatchNoneFilter returns a filter that does not match any input.
func NewMatchNoneFilter() Filter                  { return _matchNoneFilter }
func (f matchNoneFilter) String() string          { return "None" }
func (f matchNoneFilter) Matches(val []byte) bool { return false }

// timestampFilter is a filter that matches timestamps.
type timestampFilter struct{}

// NewTimestampFilter returns a filter that matches any input that
// contains timestamps.
func NewTimestampFilter() Filter         { return _timestampFilter }
func (f timestampFilter) String() string { return "Timestamp" }
func (f timestampFilter) Matches(val []byte) bool {
	if len(val) < 10 {
		return false
	}

	var count int
	for i := 1; i-count <= len(val)-9; i++ {
		// Only look for timestamps between 1400000000 (2014-05-12) and
		// 2000000000 (2033-05-18).
		if count == 0 {
			if val[i-1] == '1' && '4' <= val[i] && val[i] <= '9' {
				count = 2
			}
			continue
		}

		if '0' <= val[i] && val[i] <= '9' {
			count++
		} else {
			count = 0
		}

		if count == 10 {
			return true
		}
	}

	return false
}

// uuidFilter is a filter that matches UUID's.
type uuidFilter struct{}

// NewUUIDFilter returns a filter that matches any input that contains UUID's,
// which are defined as 32 consecutive hexidecimal characters, ignoring hyphens.
func NewUUIDFilter() Filter         { return _uuidFilter }
func (f uuidFilter) String() string { return "UUID" }
func (f uuidFilter) Matches(val []byte) bool {
	var (
		count int
		end   = len(val)
	)

	for i := 0; i-count <= end-32; i++ {
		// Ignore hyphens.
		if val[i] == '-' {
			continue
		}

		if !isHex(val[i]) {
			count = 0
			continue
		}

		if count++; count >= 32 {
			return true
		}
	}
	return false
}

// isHex returns a bool indicating whether a byte is a hexidecimal character.
func isHex(b byte) bool {
	return ('a' <= b && b <= 'f') || ('0' <= b && b <= '9') || ('A' <= b && b <= 'F')
}

// multiFilter chains multiple filters together with a logicalOp.
type multiFilter struct {
	filters []Filter
	op      LogicalOp
}

// NewMultiFilter returns a filter that chains multiple filters together
// using a LogicalOp.
func NewMultiFilter(filters []Filter, op LogicalOp) Filter {
	return &multiFilter{filters: filters, op: op}
}

func (f *multiFilter) String() string {
	separator := " " + string(f.op) + " "
	var buf bytes.Buffer
	numFilters := len(f.filters)
	for i := 0; i < numFilters; i++ {
		buf.WriteString(f.filters[i].String())
		if i < numFilters-1 {
			buf.WriteString(separator)
		}
	}
	return buf.String()
}

func (f *multiFilter) Matches(val []byte) bool {
	if len(f.filters) == 0 {
		return true
	}

	for _, filter := range f.filters {
		match := filter.Matches(val)
		if f.op == Conjunction && !match {
			return false
		}

		if f.op == Disjunction && match {
			return true
		}
	}

	return f.op == Conjunction
}

// chainFilter matches an input string against certain conditions
// while returning the unmatched part of the input if there is a match.
type chainFilter interface {
	fmt.Stringer

	matches(val []byte) ([]byte, bool)
}

// equalityChainFilter is a filter that performs equality string matches
// from either the front or back of the string.
type equalityChainFilter struct {
	pattern   []byte
	backwards bool
}

func newEqualityChainFilter(pattern []byte, backwards bool) chainFilter {
	return &equalityChainFilter{pattern: pattern, backwards: backwards}
}

func (f *equalityChainFilter) String() string {
	return "Equals(\"" + string(f.pattern) + "\")"
}

func (f *equalityChainFilter) matches(val []byte) ([]byte, bool) {
	if f.backwards && bytes.HasSuffix(val, f.pattern) {
		return val[:len(val)-len(f.pattern)], true
	}

	if !f.backwards && bytes.HasPrefix(val, f.pattern) {
		return val[len(f.pattern):], true
	}

	return nil, false
}

// singleAnyCharFilter is a filter that allows any one char.
type singleAnyCharFilter struct {
	backwards bool
}

func newSingleAnyCharFilter(backwards bool) chainFilter {
	if backwards {
		return _singleAnyCharFilterBackwards
	}

	return _singleAnyCharFilterForwards
}

func (f *singleAnyCharFilter) String() string { return "AnyChar" }

func (f *singleAnyCharFilter) matches(val []byte) ([]byte, bool) {
	if len(val) == 0 {
		return nil, false
	}

	if f.backwards {
		return val[:len(val)-1], true
	}

	return val[1:], true
}

// newSingleRangeFilter creates a filter that performs range matching
// on a single char.
func newSingleRangeFilter(pattern []byte, backwards bool) (chainFilter, error) {
	if len(pattern) == 0 {
		return nil, errInvalidFilterPattern
	}

	negate := false
	if pattern[0] == negationChar {
		negate = true
		pattern = pattern[1:]
	}

	if len(pattern) > 1 && pattern[1] == rangeChar {
		// If there is a '-' char at position 2, look for repeated instances
		// of a-z.
		if len(pattern)%3 != 0 {
			return nil, errInvalidFilterPattern
		}

		patterns := make([][]byte, 0, len(pattern)%3)
		for i := 0; i < len(pattern); i += 3 {
			if pattern[i+1] != rangeChar || pattern[i] > pattern[i+2] {
				return nil, errInvalidFilterPattern
			}

			patterns = append(patterns, pattern[i:i+3])
		}

		return &singleRangeFilter{patterns: patterns, backwards: backwards, negate: negate}, nil
	}

	return &singleCharSetFilter{pattern: pattern, backwards: backwards, negate: negate}, nil
}

// singleRangeFilter is a filter that performs a single character match against
// a range of chars given in a range format eg. [a-z].
type singleRangeFilter struct {
	patterns  [][]byte
	backwards bool
	negate    bool
}

func (f *singleRangeFilter) String() string {
	var negatePrefix, negateSuffix string
	if f.negate {
		negatePrefix = "Not("
		negateSuffix = ")"
	}

	return negatePrefix + "Range(\"" +
		string(bytes.Join(f.patterns, []byte(fmt.Sprintf(" %s ", Disjunction)))) +
		"\")" + negateSuffix
}

func (f *singleRangeFilter) matches(val []byte) ([]byte, bool) {
	if len(val) == 0 {
		return nil, false
	}

	match := false
	idx := 0
	remainder := val[1:]
	if f.backwards {
		idx = len(val) - 1
		remainder = val[:idx]
	}

	for _, pattern := range f.patterns {
		if val[idx] >= pattern[0] && val[idx] <= pattern[2] {
			match = true
			break
		}
	}

	if f.negate {
		match = !match
	}

	return remainder, match
}

// singleCharSetFilter is a filter that performs a single character match against
// a set of chars given explicity eg. [abcdefg].
type singleCharSetFilter struct {
	pattern   []byte
	backwards bool
	negate    bool
}

func (f *singleCharSetFilter) String() string {
	var negatePrefix, negateSuffix string
	if f.negate {
		negatePrefix = "Not("
		negateSuffix = ")"
	}

	return negatePrefix + "Range(\"" + string(f.pattern) + "\")" + negateSuffix
}

func (f *singleCharSetFilter) matches(val []byte) ([]byte, bool) {
	if len(val) == 0 {
		return nil, false
	}

	match := false
	for i := 0; i < len(f.pattern); i++ {
		if f.backwards && val[len(val)-1] == f.pattern[i] {
			match = true
			break
		}

		if !f.backwards && val[0] == f.pattern[i] {
			match = true
			break
		}
	}

	if f.negate {
		match = !match
	}

	if f.backwards {
		return val[:len(val)-1], match
	}

	return val[1:], match
}

// multiCharRangeFilter is a filter that performs matches against multiple sets of chars
// eg. {abc,defg}.
type multiCharSequenceFilter struct {
	patterns  [][]byte
	backwards bool
}

func newMultiCharSequenceFilter(patterns []byte, backwards bool) (chainFilter, error) {
	if len(patterns) == 0 {
		return nil, errInvalidFilterPattern
	}

	return &multiCharSequenceFilter{
		patterns:  bytes.Split(patterns, multiRangeSplit),
		backwards: backwards,
	}, nil
}

func (f *multiCharSequenceFilter) String() string {
	return "Range(\"" + string(bytes.Join(f.patterns, multiRangeSplit)) + "\")"
}

func (f *multiCharSequenceFilter) matches(val []byte) ([]byte, bool) {
	if len(val) == 0 {
		return nil, false
	}

	for _, pattern := range f.patterns {
		if f.backwards && bytes.HasSuffix(val, pattern) {
			return val[:len(val)-len(pattern)], true
		}

		if !f.backwards && bytes.HasPrefix(val, pattern) {
			return val[len(pattern):], true
		}
	}

	return nil, false
}

// multiChainFilter chains multiple chainFilters together with &&.
type multiChainFilter struct {
	filters   []chainFilter
	seg       chainSegment
	backwards bool
}

// newMultiChainFilter creates a new multiChainFilter from given chainFilters.
func newMultiChainFilter(filters []chainFilter, seg chainSegment, backwards bool) Filter {
	return &multiChainFilter{filters: filters, seg: seg, backwards: backwards}
}

func (f *multiChainFilter) String() string {
	separator := " then "
	var buf bytes.Buffer
	switch f.seg {
	case start:
		buf.WriteString("StartsWith(")
	case end:
		buf.WriteString("EndsWith(")
	}

	numFilters := len(f.filters)
	for i := 0; i < numFilters; i++ {
		buf.WriteString(f.filters[i].String())
		if i < numFilters-1 {
			buf.WriteString(separator)
		}
	}

	switch f.seg {
	case start, end:
		buf.WriteString(")")
	}

	return buf.String()
}

func (f *multiChainFilter) Matches(val []byte) bool {
	if len(f.filters) == 0 {
		return true
	}

	var match bool

	if f.backwards {
		for i := len(f.filters) - 1; i >= 0; i-- {
			val, match = f.filters[i].matches(val)
			if !match {
				return false
			}
		}
	} else {
		for i := 0; i < len(f.filters); i++ {
			val, match = f.filters[i].matches(val)
			if !match {
				return false
			}
		}
	}

	if f.seg == middle && len(val) != 0 {
		// chain was middle segment and some value was left over at end of chain.
		return false
	}

	return true
}
