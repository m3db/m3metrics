// Copyright (c) 2018 Uber Technologies, Inc.
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

// mockgen rules for generating mocks for exported interfaces (reflection mode).
//go:generate sh -c "mockgen -package=id $PACKAGE/metric/id ID | mockclean -pkg $PACKAGE/metric/id -out $GOPATH/src/$PACKAGE/metric/id/id_mock.go"
//go:generate sh -c "mockgen -package=matcher $PACKAGE/matcher Matcher | mockclean -pkg $PACKAGE/matcher -out $GOPATH/src/$PACKAGE/matcher/matcher_mock.go"
//go:generate sh -c "mockgen -package=protobuf $PACKAGE/encoding/protobuf UnaggregatedEncoder | mockclean -pkg $PACKAGE/encoding/protobuf -out $GOPATH/src/$PACKAGE/encoding/protobuf/protobuf_mock.go"
//go:generate sh -c "mockgen -package=rules $PACKAGE/rules Store| mockclean -pkg $PACKAGE/rules -out $GOPATH/src/$PACKAGE/rules/store_mock.go"

package mocks
