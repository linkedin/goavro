// Copyright [2017] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"bytes"
	"fmt"
	"math"
	"testing"
)

func testTextDecodeFail(t *testing.T, schema string, buf []byte, errorMessage string) {
	c, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	value, newBuffer, err := c.NativeFromTextual(buf)
	ensureError(t, err, errorMessage)
	if value != nil {
		t.Errorf("Actual: %v; Expected: %v", value, nil)
	}
	if !bytes.Equal(buf, newBuffer) {
		t.Errorf("Actual: %v; Expected: %v", newBuffer, buf)
	}
}

func testTextEncodeFail(t *testing.T, schema string, datum interface{}, errorMessage string) {
	c, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := c.TextualFromNative(nil, datum)
	ensureError(t, err, errorMessage)
	if buf != nil {
		t.Errorf("Actual: %v; Expected: %v", buf, nil)
	}
}

func testTextEncodeFailBadDatumType(t *testing.T, schema string, datum interface{}) {
	testTextEncodeFail(t, schema, datum, "received: ")
}

func testTextDecodeFailShortBuffer(t *testing.T, schema string, buf []byte) {
	testTextDecodeFail(t, schema, buf, "short buffer")
}

func testTextDecodePass(t *testing.T, schema string, datum interface{}, encoded []byte) {
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}

	decoded, remaining, err := codec.NativeFromTextual(encoded)
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}

	// remaining ought to be empty because there is nothing remaining to be
	// decoded
	if actual, expected := len(remaining), 0; actual != expected {
		t.Errorf("schema: %s; Datum: %#v; Actual: %v; Expected: %v", schema, datum, actual, expected)
	}

	var datumIsMap, datumIsNumerical, datumIsSlice, datumIsString bool
	var datumFloat float64
	var datumMap map[string]interface{}
	var datumSlice []interface{}
	var datumString string
	switch v := datum.(type) {
	case float64:
		datumFloat = v
		datumIsNumerical = true
	case float32:
		datumFloat = float64(v)
		datumIsNumerical = true
	case int:
		datumFloat = float64(v)
		datumIsNumerical = true
	case int32:
		datumFloat = float64(v)
		datumIsNumerical = true
	case int64:
		datumFloat = float64(v)
		datumIsNumerical = true
	case string:
		datumString = v
		datumIsString = true
	case []interface{}:
		datumIsSlice = true
		datumSlice = v
	case map[string]interface{}:
		datumIsMap = true
		datumMap = v
	}

	var decodedIsMap, decodedIsNumerical, decodedIsSlice, decodedIsString bool
	var decodedMap map[string]interface{}
	var decodedFloat float64
	var decodedSlice []interface{}
	var decodedString string
	switch v := decoded.(type) {
	case float64:
		decodedFloat = v
		decodedIsNumerical = true
	case float32:
		decodedFloat = float64(v)
		decodedIsNumerical = true
	case int:
		decodedFloat = float64(v)
		decodedIsNumerical = true
	case int32:
		decodedFloat = float64(v)
		decodedIsNumerical = true
	case int64:
		decodedFloat = float64(v)
		decodedIsNumerical = true
	case string:
		decodedString = v
		decodedIsString = true
	case []interface{}:
		decodedIsSlice = true
		decodedSlice = v
	case map[string]interface{}:
		decodedIsMap = true
		decodedMap = v
	}

	// NOTE: Special handling when both datum and decoded values are floating
	// point to test whether both are NaN, -Inf, or +Inf.
	if datumIsNumerical && decodedIsNumerical {
		if (math.IsNaN(datumFloat) != math.IsNaN(decodedFloat)) &&
			(math.IsInf(datumFloat, 1) != math.IsInf(decodedFloat, 1)) &&
			(math.IsInf(datumFloat, -1) != math.IsInf(decodedFloat, -1)) &&
			datumFloat != decodedFloat {
			t.Errorf("numerical comparison: schema: %s; Datum: %v; Actual: %v; Expected: %v", schema, datum, decodedFloat, datumFloat)
		}
	} else if datumIsMap && decodedIsMap {
		if actual, expected := len(decodedMap), len(datumMap); actual != expected {
			t.Fatalf("map comparison: length mismatch; Actual: %v; Expected: %v", actual, expected)
		}
		for key, datumValue := range datumMap {
			decodedValue, ok := decodedMap[key]
			if !ok {
				t.Fatalf("map comparison: decoded missing key: %q: Actual: %v; Expected: %v", key, decodedMap, datumMap)
			}
			if actual, expected := fmt.Sprintf("%v", decodedValue), fmt.Sprintf("%v", datumValue); actual != expected {
				t.Errorf("map comparison: values differ for key: %q; Actual: %v; Expected: %v", key, actual, expected)
			}
		}
	} else if datumIsSlice && decodedIsSlice {
		if actual, expected := len(decodedMap), len(datumMap); actual != expected {
			t.Fatalf("slice comparison: length mismatch; Actual: %v; Expected: %v", actual, expected)
		}
		for i, datumValue := range datumSlice {
			decodedValue := decodedSlice[i]
			if actual, expected := fmt.Sprintf("%v", decodedValue), fmt.Sprintf("%v", datumValue); actual != expected {
				t.Errorf("slice comparison: values differ for index: %d: Actual: %v; Expected: %v", i+1, actual, expected)
			}
		}
	} else if datumIsString && decodedIsString {
		if actual, expected := decodedString, datumString; actual != expected {
			t.Errorf("string comparison: Actual: %v; Expected: %v", actual, expected)
		}
	} else if actual, expected := fmt.Sprintf("%v", decoded), fmt.Sprintf("%v", datum); actual != expected {
		t.Errorf("schema: %s; Datum: %v; Actual: %s; Expected: %s", schema, datum, actual, expected)
	}
}

func testTextEncodePass(t *testing.T, schema string, datum interface{}, expected []byte) {
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatalf("Schma: %q %s", schema, err)
	}

	actual, err := codec.TextualFromNative(nil, datum)
	if err != nil {
		t.Fatalf("schema: %s; Datum: %v; %s", schema, datum, err)
	}
	if !bytes.Equal(actual, expected) {
		t.Errorf("schema: %s; Datum: %v; Actual: %+q; Expected: %+q", schema, datum, actual, expected)
	}
}

// testTextCodecPass does a bi-directional codec check, by encoding datum to
// bytes, then decoding bytes back to datum.
func testTextCodecPass(t *testing.T, schema string, datum interface{}, buf []byte) {
	testTextDecodePass(t, schema, datum, buf)
	testTextEncodePass(t, schema, datum, buf)
}
