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

var morePositiveThanMaxBlockCount, morePositiveThanMaxBlockSize, moreNegativeThanMaxBlockCount, mostNegativeBlockCount []byte

func init() {
	c, err := NewCodec("long")
	if err != nil {
		panic(err)
	}

	morePositiveThanMaxBlockCount, err = c.BinaryFromNative(nil, (MaxBlockCount + 1))
	if err != nil {
		panic(err)
	}

	morePositiveThanMaxBlockSize, err = c.BinaryFromNative(nil, (MaxBlockSize + 1))
	if err != nil {
		panic(err)
	}

	moreNegativeThanMaxBlockCount, err = c.BinaryFromNative(nil, -(MaxBlockCount + 1))
	if err != nil {
		panic(err)
	}

	mostNegativeBlockCount, err = c.BinaryFromNative(nil, math.MinInt64)
	if err != nil {
		panic(err)
	}
}

func testBinaryDecodeFail(t *testing.T, schema string, buf []byte, errorMessage string) {
	c, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	value, newBuffer, err := c.NativeFromBinary(buf)
	ensureError(t, err, errorMessage)
	if value != nil {
		t.Errorf("Actual: %v; Expected: %v", value, nil)
	}
	if !bytes.Equal(buf, newBuffer) {
		t.Errorf("Actual: %v; Expected: %v", newBuffer, buf)
	}
}

func testBinaryEncodeFail(t *testing.T, schema string, datum interface{}, errorMessage string) {
	c, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := c.BinaryFromNative(nil, datum)
	ensureError(t, err, errorMessage)
	if buf != nil {
		t.Errorf("Actual: %v; Expected: %v", buf, nil)
	}
}

func testBinaryEncodeFailBadDatumType(t *testing.T, schema string, datum interface{}) {
	testBinaryEncodeFail(t, schema, datum, "received: ")
}

func testBinaryDecodeFailShortBuffer(t *testing.T, schema string, buf []byte) {
	testBinaryDecodeFail(t, schema, buf, "short buffer")
}

func testBinaryDecodePass(t *testing.T, schema string, datum interface{}, encoded []byte) {
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	value, remaining, err := codec.NativeFromBinary(encoded)
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}

	// remaining ought to be empty because there is nothing remaining to be
	// decoded
	if actual, expected := len(remaining), 0; actual != expected {
		t.Errorf("schema: %s; Datum: %v; Actual: %#v; Expected: %#v", schema, datum, actual, expected)
	}

	// for testing purposes, to prevent big switch statement, convert each to
	// string and compare.
	if actual, expected := fmt.Sprintf("%v", value), fmt.Sprintf("%v", datum); actual != expected {
		t.Errorf("schema: %s; Datum: %v; Actual: %#v; Expected: %#v", schema, datum, actual, expected)
	}
}

func testBinaryEncodePass(t *testing.T, schema string, datum interface{}, expected []byte) {
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatalf("Schma: %q %s", schema, err)
	}

	actual, err := codec.BinaryFromNative(nil, datum)
	if err != nil {
		t.Fatalf("schema: %s; Datum: %v; %s", schema, datum, err)
	}
	if !bytes.Equal(actual, expected) {
		t.Errorf("schema: %s; Datum: %v; Actual: %#v; Expected: %#v", schema, datum, actual, expected)
	}
}

// testBinaryCodecPass does a bi-directional codec check, by encoding datum to
// bytes, then decoding bytes back to datum.
func testBinaryCodecPass(t *testing.T, schema string, datum interface{}, buf []byte) {
	testBinaryDecodePass(t, schema, datum, buf)
	testBinaryEncodePass(t, schema, datum, buf)
}
