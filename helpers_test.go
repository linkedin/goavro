// Copyright 2015 LinkedIn Corp. Licensed under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License.  You may obtain a copy of the License
// at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.Copyright [201X] LinkedIn Corp. Licensed under the Apache
// License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.

package goavro

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

type testBuffer interface {
	io.ReadWriter
	Bytes() []byte
}

// A byte buffer for testing that fulfills io.ReadWriter, but can't be
// upcast to ByteWriter or StringWriter
type simpleBuffer struct {
	buf bytes.Buffer
}

func (sb *simpleBuffer) Write(b []byte) (n int, err error) {
	return sb.buf.Write(b)
}

func (sb *simpleBuffer) Bytes() []byte {
	return sb.buf.Bytes()
}

func (sb *simpleBuffer) Read(p []byte) (n int, err error) {
	return sb.buf.Read(p)
}

func checkError(t *testing.T, actualError error, expectedError interface{}) {
	if expectedError == nil {
		if actualError != nil {
			t.Errorf("Actual: %#v; Expected: %#v", actualError.Error(), expectedError)
		}
	} else {
		if actualError == nil {
			t.Errorf("Actual: %#v; Expected: %#v", actualError, expectedError)
		} else {
			var expected error
			switch expectedError.(type) {
			case string:
				expected = fmt.Errorf(expectedError.(string))
			case error:
				expected = expectedError.(error)
			}
			if !strings.Contains(actualError.Error(), expected.Error()) {
				t.Errorf("Actual: %#v; Expected to contain: %#v",
					actualError.Error(), expected.Error())
			}
		}
	}
}

func checkErrorFatal(t *testing.T, actualError error, expectedError interface{}) {
	if expectedError == nil {
		if actualError != nil {
			t.Fatalf("Actual: %#v; Expected: %#v", actualError.Error(), expectedError)
		}
	} else {
		if actualError == nil {
			t.Fatalf("Actual: %#v; Expected: %#v", actualError, expectedError)
		} else {
			var expected string
			switch expectedError.(type) {
			case string:
				expected = expectedError.(string)
			case error:
				expected = expectedError.(error).Error()
			}
			if !strings.Contains(actualError.Error(), expected) {
				t.Fatalf("Actual: %#v; Expected to contain: %#v",
					actualError.Error(), expected)
			}
		}
	}
}

func checkResponse(t *testing.T, bb *bytes.Buffer, n int, expectedBytes []byte) {
	expectedCount := len(expectedBytes)
	if n != expectedCount {
		t.Errorf("Actual: %#v; Expected: %#v", n, expectedCount)
	}
	if bytes.Compare(bb.Bytes(), expectedBytes) != 0 {
		t.Errorf("Actual: %#v; Expected: %#v", bb.Bytes(), expectedBytes)
	}
}
