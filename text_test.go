// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
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
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testTextDecodeFail(t *testing.T, schema string, buf []byte, errorMessage string) {
	t.Helper()
	c, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	value, newBuffer, err := c.NativeFromTextual(buf)
	ensureError(t, err, errorMessage)
	if value != nil {
		t.Errorf("GOT: %v; WANT: %v", value, nil)
	}
	if !bytes.Equal(buf, newBuffer) {
		t.Errorf("GOT: %v; WANT: %v", newBuffer, buf)
	}
}

func testTextEncodeFail(t *testing.T, schema string, datum interface{}, errorMessage string) {
	t.Helper()
	c, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := c.TextualFromNative(nil, datum)
	ensureError(t, err, errorMessage)
	if buf != nil {
		t.Errorf("GOT: %v; WANT: %v", buf, nil)
	}
}

func testTextEncodeFailBadDatumType(t *testing.T, schema string, datum interface{}) {
	t.Helper()
	testTextEncodeFail(t, schema, datum, "received: ")
}

func testTextDecodeFailShortBuffer(t *testing.T, schema string, buf []byte) {
	t.Helper()
	testTextDecodeFail(t, schema, buf, "short buffer")
}

func testTextDecodePass(t *testing.T, schema string, datum interface{}, encoded []byte) {
	t.Helper()
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}
	toNativeAndCompare(t, schema, datum, encoded, codec)
}
func testJSONDecodePass(t *testing.T, schema string, datum interface{}, encoded []byte) {
	t.Helper()
	codec, err := NewCodecFrom(schema, &codecBuilder{
		buildCodecForTypeDescribedByMap,
		buildCodecForTypeDescribedByString,
		buildCodecForTypeDescribedBySliceOneWayJSON,
	})
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}
	toNativeAndCompare(t, schema, datum, encoded, codec)
}
func testNativeToTextualJSONPass(t *testing.T, schema string, datum interface{}, encoded []byte) {
	t.Helper()
	codec, err := NewCodecFrom(schema, &codecBuilder{
		buildCodecForTypeDescribedByMap,
		buildCodecForTypeDescribedByString,
		buildCodecForTypeDescribedBySliceTwoWayJSON,
	})
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}
	toTextualAndCompare(t, schema, datum, encoded, codec)
}

func toTextualAndCompare(t *testing.T, schema string, datum interface{}, expected []byte, codec *Codec) {
	t.Helper()
	decoded, err := codec.TextualFromNative(nil, datum)
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}

	// do extra stuff to to the challenge equality of maps
	var want interface{}

	if err := json.Unmarshal(expected, &want); err != nil {
		t.Errorf("Could not unmarshal the expected data into a go struct:%#v:", string(expected))
	}

	var got interface{}

	if err := json.Unmarshal(decoded, &got); err != nil {
		t.Errorf("Could not unmarshal the received data into a go struct:%#v:", string(decoded))
	}

	if !assert.Equal(t, want, got) {
		t.Errorf("GOT: %v; WANT: %v", string(decoded), string(expected))
	}
}

func toNativeAndCompare(t *testing.T, schema string, datum interface{}, encoded []byte, codec *Codec) {
	t.Helper()
	decoded, remaining, err := codec.NativeFromTextual(encoded)
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}

	// remaining ought to be empty because there is nothing remaining to be
	// decoded
	if actual, expected := len(remaining), 0; actual != expected {
		t.Errorf("schema: %s; Datum: %#v; Actual: %v; Expected: %v", schema, datum, actual, expected)
	}

	const (
		_         = iota
		isInt     = iota
		isFloat32 = iota
		isFloat64 = iota
		isMap     = iota
		isSlice   = iota
		isString  = iota
	)

	var datumType int
	var datumInt int64
	var datumFloat32 float32
	var datumFloat64 float64
	var datumMap map[string]interface{}
	var datumSlice []interface{}
	var datumString string
	switch v := datum.(type) {
	case float64:
		datumFloat64 = v
		datumType = isFloat64
	case float32:
		datumFloat32 = v
		datumType = isFloat32
	case int:
		datumInt = int64(v)
		datumType = isInt
	case int32:
		datumInt = int64(v)
		datumType = isInt
	case int64:
		datumInt = v
		datumType = isInt
	case string:
		datumString = v
		datumType = isString
	case []interface{}:
		datumSlice = v
		datumType = isSlice
	case map[string]interface{}:
		datumMap = v
		datumType = isMap
	}

	var decodedType int
	var decodedInt int64
	var decodedFloat32 float32
	var decodedFloat64 float64
	var decodedMap map[string]interface{}
	var decodedSlice []interface{}
	var decodedString string
	switch v := decoded.(type) {
	case float64:
		decodedFloat64 = v
		decodedType = isFloat64
	case float32:
		decodedFloat32 = v
		decodedType = isFloat32
	case int:
		decodedInt = int64(v)
		decodedType = isInt
	case int32:
		decodedInt = int64(v)
		decodedType = isInt
	case int64:
		decodedInt = v
		decodedType = isInt
	case string:
		decodedString = v
		decodedType = isString
	case []interface{}:
		decodedSlice = v
		decodedType = isSlice
	case map[string]interface{}:
		decodedMap = v
		decodedType = isMap
	}

	if datumType == isInt && decodedType == isInt {
		if datumInt != decodedInt {
			t.Errorf("numerical comparison: schema: %s; Datum: %v; Actual: %v; Expected: %v", schema, datum, decodedInt, datumInt)
		}
		return
	}
	// NOTE: Special handling when both datum and decoded values are floating
	// point to test whether both are NaN, -Inf, or +Inf.
	if datumType == isFloat64 && decodedType == isFloat64 {
		if !(math.IsNaN(datumFloat64) && math.IsNaN(decodedFloat64)) &&
			!(math.IsInf(datumFloat64, 1) && math.IsInf(decodedFloat64, 1)) &&
			!(math.IsInf(datumFloat64, -1) && math.IsInf(decodedFloat64, -1)) &&
			datumFloat64 != decodedFloat64 {
			t.Errorf("numerical comparison: schema: %s; Datum: %v; Actual: %v; Expected: %v", schema, datum, decodedFloat64, datumFloat64)
		}
		return
	}
	if datumType == isFloat32 && decodedType == isFloat32 {
		a := float64(datumFloat32)
		b := float64(decodedFloat32)
		if !(math.IsNaN(a) && math.IsNaN(b)) &&
			!(math.IsInf(a, 1) && math.IsInf(b, 1)) &&
			!(math.IsInf(a, -1) && math.IsInf(b, -1)) &&
			a != b {
			t.Errorf("numerical comparison: schema: %s; Datum: %v; Actual: %v; Expected: %v", schema, datum, decodedFloat32, datumFloat32)
		}
		return
	}
	if datumType == isMap && decodedType == isMap {
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
		return
	}
	if datumType == isSlice && decodedType == isSlice {
		if actual, expected := len(decodedMap), len(datumMap); actual != expected {
			t.Fatalf("slice comparison: length mismatch; Actual: %v; Expected: %v", actual, expected)
		}
		for i, datumValue := range datumSlice {
			decodedValue := decodedSlice[i]
			if actual, expected := fmt.Sprintf("%v", decodedValue), fmt.Sprintf("%v", datumValue); actual != expected {
				t.Errorf("slice comparison: values differ for index: %d: Actual: %v; Expected: %v", i+1, actual, expected)
			}
		}
		return
	}
	if datumType == isString && decodedType == isString {
		if actual, expected := decodedString, datumString; actual != expected {
			t.Errorf("string comparison: Actual: %v; Expected: %v", actual, expected)
		}
		return
	}
	if actual, expected := fmt.Sprintf("%v", decoded), fmt.Sprintf("%v", datum); actual != expected {
		t.Errorf("schema: %s; Datum: %v; Actual: %s; Expected: %s", schema, datum, actual, expected)
	}
}

func testTextEncodePass(t *testing.T, schema string, datum interface{}, expected []byte) {
	t.Helper()
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
	t.Helper()
	testTextDecodePass(t, schema, datum, buf)
	testTextEncodePass(t, schema, datum, buf)
}
