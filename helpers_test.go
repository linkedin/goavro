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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

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
			var expected error
			switch expectedError.(type) {
			case string:
				expected = fmt.Errorf(expectedError.(string))
			case error:
				expected = expectedError.(error)
			}
			if !strings.Contains(actualError.Error(), expected.Error()) {
				t.Fatalf("Actual: %#v; Expected to contain: %#v",
					actualError.Error(), expected.Error())
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

func schemaType(t *testing.T, someJsonSchema string) string {
	var schema interface{}
	err := json.Unmarshal([]byte(someJsonSchema), &schema)
	if err != nil {
		t.Fatal(err)
	}
	switch schema.(type) {
	case map[string]interface{}:
		someMap := schema.(map[string]interface{})
		someValue, ok := someMap["type"]
		if !ok {
			t.Errorf("Actual: %#v; Expected: %#v", ok, true)
		}
		someString, ok := someValue.(string)
		if !ok {
			t.Errorf("Actual: %#v; Expected: %#v", ok, true)
		}
		return someString
	case []interface{}:
		return "union"
	default:
		t.Errorf("Actual: %#T; Expected: map[string]interface{}", schema)
		return ""
	}
}

func schemaTypeCodec(t *testing.T, someJsonSchema string) string {
	var schema interface{}
	err := json.Unmarshal([]byte(someJsonSchema), &schema)
	if err != nil {
		t.Fatal(err)
	}
	switch schema.(type) {
	case map[string]interface{}:
		someMap := schema.(map[string]interface{})
		someValue, ok := someMap["type"]
		if !ok {
			t.Errorf("Actual: %#v; Expected: %#v", ok, true)
		}
		someString, ok := someValue.(string)
		if !ok {
			t.Errorf("Actual: %#v; Expected: %#v", ok, true)
		}
		return someString
	case []interface{}:
		return "union"
	default:
		t.Errorf("Actual: %#T; Expected: map[string]interface{}", schema)
		return ""
	}
}

func schemaName(t *testing.T, someJsonSchema string) string {
	var schema interface{}
	err := json.Unmarshal([]byte(someJsonSchema), &schema)
	if err != nil {
		t.Fatal(err)
	}
	someMap, ok := schema.(map[string]interface{})
	if !ok {
		t.Errorf("Actual: %#T; Expected: map[string]interface{}", schema)
	}
	someValue, ok := someMap["name"]
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	someString, ok := someValue.(string)
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	return someString
}
