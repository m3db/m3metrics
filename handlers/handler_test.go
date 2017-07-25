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

package handlers

import (
	"testing"

	"github.com/m3db/m3cluster/kv/mem"

	"github.com/stretchr/testify/require"
)

const (
	testNamespaceKey  = "testKey"
	testNamespace     = "fooNs"
	testRuleSetKeyFmt = "rules/%s"
)

func testHandler() *Handler {
	opts := NewHandlerOpts(0, testNamespaceKey, testRuleSetKeyFmt)
	store := mem.NewStore()
	h := NewHandler(store, opts)
	return &h
}

func TestRuleSetKey(t *testing.T) {
	h := testHandler()
	key := h.RuleSetKey(testNamespace)
	require.Equal(t, "rules/fooNs", key)
}

func TestNewHandler(t *testing.T) {
	opts := NewHandlerOpts(0, testNamespaceKey, testRuleSetKeyFmt)
	store := mem.NewStore()
	h := NewHandler(store, opts)

	require.Equal(t, h.store, store)
	require.Equal(t, h.opts, opts)
}
