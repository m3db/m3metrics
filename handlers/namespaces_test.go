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

	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/stretchr/testify/require"
)

var (
	testNamespaces = &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "fooNs",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 2,
						Tombstoned:        false,
					},
				},
			},
			&schema.Namespace{
				Name: "barNs",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 2,
						Tombstoned:        true,
					},
				},
			},
		},
	}

	badNamespaces = &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{Name: "fooNs", Snapshots: nil},
			&schema.Namespace{Name: "fooNs", Snapshots: nil},
		},
	}
)

func TestNamespaces(t *testing.T) {
	h := testHandler()
	h.store.Set(testNamespaceKey, testNamespaces)
	nss, err := h.Namespaces()
	require.NoError(t, err)
	require.NotNil(t, nss.Namespaces)
}

func TestNamespacesError(t *testing.T) {
	h := testHandler()
	h.store.Set(testNamespaceKey, &schema.RollupRule{Uuid: "x"})
	nss, err := h.Namespaces()
	require.Error(t, err)
	require.Nil(t, nss)
}

func TestValidateNamespace(t *testing.T) {
	h := testHandler()
	h.store.Set(testNamespaceKey, testNamespaces)
	nss, err := h.Namespaces()
	require.NoError(t, err)
	err = h.ValidateNamespace(nss, "fooNs")
	require.NoError(t, err)
}

func TestValidateNamespaceDNE(t *testing.T) {
	h := testHandler()
	h.store.Set(testNamespaceKey, testNamespaces)
	nss, err := h.Namespaces()
	err = h.ValidateNamespace(nss, "blah")
	require.Error(t, err)
}

func TestValidateNamespaceTombstoned(t *testing.T) {
	h := testHandler()
	h.store.Set(testNamespaceKey, testNamespaces)
	nss, err := h.Namespaces()
	err = h.ValidateNamespace(nss, "bar")
	require.Error(t, err)
}
