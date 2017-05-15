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

package policy

import (
	"fmt"

	"github.com/tmthrgd/go-popcount"
)

const (
	// NB(jeromefroe): Since PoliciesFlag can represent up to 63 unique
	// policies, the default size of the dynamic tables should be 63 as well.
	defaultTableSize = 63
)

// Bitflag is a bitflag representation of a slice of policies. It can
// represent up to 63 unique policies or any combination thereof.
type Bitflag uint64

// EmptyBitflag is a bitflag that represents an empty slice of policies.
var EmptyBitflag Bitflag

// NewBitflag returns a new empty bitflag.
func NewBitflag() Bitflag { return EmptyBitflag }

// Set sets the nth bit of a PoliciesFlag. It panics if n > 63.
func (i Bitflag) Set(n uint) Bitflag {
	if n > 63 {
		err := fmt.Errorf(
			"tried to set %vth bit of a PoliciesFlag, only bits 0-63 can be set",
			n,
		)
		panic(err)
	}

	i = i | (1 << n)
	return i
}

// Iterator returns an iterator for a policies bitflag. It makes a copy
// of the flag so that the original bitflag can be safely mutated.
func (i Bitflag) Iterator() BitflagIterator {
	return BitflagIterator{
		flag: i,
		idx:  -1,
	}
}

// BitflagIterator is an iterator for iterating over the bits in a
// policies bitflag.
type BitflagIterator struct {
	flag Bitflag
	idx  int
}

// Next returns a boolean indicating whether the policies bitflag has any
// remaining bits set.
func (i *BitflagIterator) Next() bool {
	var set bool
	for {
		if i.flag == 0 {
			return false
		}
		i.idx++

		set = (i.flag & (1 << uint(i.idx))) > 0
		if set {
			// Unset the current bit.
			i.flag = i.flag ^ (1 << uint(i.idx))
			return true
		}
	}
}

// Index returns the index of the currently set bit in the policies bitflag.
func (i BitflagIterator) Index() int {
	return i.idx
}

// Encoder encodes policies into a bitflag.
type Encoder struct {
	dynamicTable map[Policy]uint
}

// NewEncoder returns a new Encoder.
func NewEncoder() Encoder {
	dt := make(map[Policy]uint, defaultTableSize)
	return Encoder{dt}
}

// Encode encodes a slice of polices into a bitflag. It returns a bitflag
// representation of the policies it encoded and a slice of policies that
// it could not encode because they are new. New policies are inserted into
// its table so that subsequent calls to Encode will be able to encode them
// in the returned bitflag as well. It accepts a buffer argument so that
// slices of policies can be reused between calls.
func (e *Encoder) Encode(policies, buffer []Policy) (Bitflag, []Policy) {
	var flag Bitflag
	if buffer == nil {
		buffer = make([]Policy, 0)
	}

	for _, policy := range policies {
		id, ok := e.dynamicTable[policy]
		if !ok && len(e.dynamicTable) <= defaultTableSize {
			e.dynamicTable[policy] = uint(len(e.dynamicTable))
			buffer = append(buffer, policy)
			continue
		}

		flag = flag.Set(id)
	}

	return flag, buffer
}

// Decoder decodes a bitflag representing a slice of policies.
type Decoder struct {
	dynamicTable []Policy
}

// NewDecoder returns a new decoder for policy bitflags.
func NewDecoder() Decoder {
	dt := make([]Policy, 0, defaultTableSize)
	return Decoder{dt}
}

// Decode decodes a policies bitflag, appending the associated policies
// to the slice of policies passed to it. Any policies in the slice passed
// to it are added to its table so they can be decoded in subsequent calls
// to Decode. Decode also returns a bitflag representing all the policies
// which were passed into it, including those in both the policies bitflag
// and the slice of policies.
func (d *Decoder) Decode(
	policies []Policy,
	flag Bitflag,
) (Bitflag, []Policy, error) {
	if policies == nil {
		l := popcount.Count64(uint64(flag))
		policies = make([]Policy, 0, l)
	}
	originalLen := len(policies)

	iter := flag.Iterator()
	for iter.Next() {
		idx := iter.Index()
		if idx >= len(d.dynamicTable) {
			err := fmt.Errorf(
				"encountered invalid index decoding bitflag, %v was set but the decoder only has %v entries",
				idx,
				len(d.dynamicTable),
			)
			return 0, policies, err
		}

		policies = append(policies, d.dynamicTable[idx])
	}

	for i, policy := range policies {
		if i == originalLen {
			break
		}

		if len(d.dynamicTable) < defaultTableSize {
			idx := uint(len(d.dynamicTable))
			flag = flag.Set(idx)
			d.dynamicTable = append(d.dynamicTable, policy)
		}
	}

	return flag, policies, nil
}
