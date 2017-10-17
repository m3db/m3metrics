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

package rules

const (
	defaultNamespaceTag = "namespace"
)

// SanitizerOptions provide a set of options for the sanitizer.
type SanitizerOptions interface {
	// SetNamespaceTag sets the namespace tag.
	SetNamespaceTag(value string) SanitizerOptions

	// NamespaceTag returns the namespace tag.
	NamespaceTag() string
}

type sanitizerOptions struct {
	namespaceTag string
}

// NewSanitizerOptions create a new set of sanitizer options.
func NewSanitizerOptions() SanitizerOptions {
	return &sanitizerOptions{
		namespaceTag: defaultNamespaceTag,
	}
}

func (o *sanitizerOptions) SetNamespaceTag(value string) SanitizerOptions {
	opts := *o
	opts.namespaceTag = value
	return &opts
}

func (o *sanitizerOptions) NamespaceTag() string {
	return o.namespaceTag
}

// Sanitizer sanitizes rules.
type Sanitizer interface {
	// SanitizeRollupRule sanitizes a rollup rule for a given namespace.
	SanitizeRollupRule(rr *RollupRuleView)
}

type sanitizer struct {
	namespaceTag string
}

// NewSanitizer creates a new sanitizer.
func NewSanitizer(opts SanitizerOptions) Sanitizer {
	return &sanitizer{
		namespaceTag: opts.NamespaceTag(),
	}
}

// SanitizeRollupRule performs the following sanitizations:
// * If the list of rollup tags do not contain the namespace tag, add the namespace tag.
func (s *sanitizer) SanitizeRollupRule(rr *RollupRuleView) {
	if rr == nil {
		return
	}
	for i, target := range rr.Targets {
		found := false
		for _, tag := range target.Tags {
			if tag == s.namespaceTag {
				found = true
				break
			}
		}
		if !found {
			rr.Targets[i].Tags = append(rr.Targets[i].Tags, s.namespaceTag)
		}
	}
}
