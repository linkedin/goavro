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
	"testing"
)

func TestArraySchema(t *testing.T) {
	testSchemaValid(t, `{"type":"array","items":"bytes"}`)
	testSchemaInvalid(t, `{"type":"array","item":"int"}`, "Array ought to have items key")
	testSchemaInvalid(t, `{"type":"array","items":"integer"}`, "Array items ought to be valid Avro type")
	testSchemaInvalid(t, `{"type":"array","items":3}`, "Array items ought to be valid Avro type")
	testSchemaInvalid(t, `{"type":"array","items":int}`, "invalid character") // type name must be quoted
}

func TestArrayDecodeInitialBlockCountCannotDecode(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"array","items":"int"}`, nil, "block count")
}

func TestArrayDecodeInitialBlockCountZero(t *testing.T) {
	testBinaryDecodePass(t, `{"type":"array","items":"int"}`, []interface{}{}, []byte{0})
}

func TestArrayDecodeInitialBlockCountNegative(t *testing.T) {
	testBinaryDecodePass(t, `{"type":"array","items":"int"}`, []interface{}{3}, []byte{1, 2, 6, 0})
}

func TestArrayDecodeInitialBlockCountTooLarge(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"array","items":"int"}`, morePositiveThanMaxBlockCount, "block count")
}

func TestArrayDecodeInitialBlockCountNegativeTooLarge(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"array","items":"int"}`, append(moreNegativeThanMaxBlockCount, byte(0)), "block count")
}

func TestArrayDecodeInitialBlockCountTooNegative(t *testing.T) {
	// -(uint8(-128)) == -128
	testBinaryDecodeFail(t, `{"type":"array","items":"int"}`, append(mostNegativeBlockCount, byte(0)), "block count")
}

func TestArrayDecodeNextBlockCountCannotDecode(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"array","items":"int"}`, []byte{2, 6}, "block count")
}

func TestArrayDecodeNextBlockCountNegative(t *testing.T) {
	testBinaryDecodePass(t, `{"type":"array","items":"int"}`, []interface{}{3, 3}, []byte{2, 6, 1, 2, 6, 0})
}

func TestArrayDecodeNextBlockCountTooLarge(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"array","items":"int"}`, append([]byte{2, 6}, morePositiveThanMaxBlockCount...), "block count")
}

func TestArrayDecodeNextBlockCountNegativeTooLarge(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"array","items":"int"}`, append([]byte{2, 6}, append(moreNegativeThanMaxBlockCount, []byte{2, 6, 0}...)...), "block count")
}

func TestArrayDecodeNextBlockCountTooNegative(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"array","items":"int"}`, append([]byte{2, 6}, append(mostNegativeBlockCount, []byte{2, 6, 0}...)...), "block count")
}

func TestArrayNull(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"array","items":"null"}`, []interface{}{}, []byte{0})
	testBinaryCodecPass(t, `{"type":"array","items":"null"}`, []interface{}{nil}, []byte{2, 0})
	testBinaryCodecPass(t, `{"type":"array","items":"null"}`, []interface{}{nil, nil}, []byte{4, 0})
}

func TestArrayReceiveSliceEmptyInterface(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"array","items":"boolean"}`, []interface{}{}, []byte{0})
	testBinaryCodecPass(t, `{"type":"array","items":"boolean"}`, []interface{}{false}, []byte{2, 0, 0})
	testBinaryCodecPass(t, `{"type":"array","items":"boolean"}`, []interface{}{true}, []byte{2, 1, 0})
	testBinaryCodecPass(t, `{"type":"array","items":"boolean"}`, []interface{}{false, false}, []byte{4, 0, 0, 0})
	testBinaryCodecPass(t, `{"type":"array","items":"boolean"}`, []interface{}{true, true}, []byte{4, 1, 1, 0})
}

func TestArrayBinaryReceiveSliceInt(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"array","items":"int"}`, []int{}, []byte{0})
	testBinaryCodecPass(t, `{"type":"array","items":"int"}`, []int{1}, []byte("\x02\x02\x00"))
	testBinaryCodecPass(t, `{"type":"array","items":"int"}`, []int{1, 2}, []byte("\x04\x02\x04\x00"))
}

func TestArrayTextualReceiveSliceInt(t *testing.T) {
	testTextCodecPass(t, `{"type":"array","items":"int"}`, []int{}, []byte(`[]`))
	testTextCodecPass(t, `{"type":"array","items":"int"}`, []int{1}, []byte(`[1]`))
	testTextCodecPass(t, `{"type":"array","items":"int"}`, []int{1, 2}, []byte(`[1,2]`))
}

func TestArrayBytes(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"array","items":"bytes"}`, []interface{}(nil), []byte{0})                           // item count == 0
	testBinaryCodecPass(t, `{"type":"array","items":"bytes"}`, []interface{}{[]byte("foo")}, []byte("\x02\x06foo\x00")) // item count == 1, item 1 size == 3, foo, no more items
	testBinaryCodecPass(t, `{"type":"array","items":"bytes"}`, []interface{}{[]byte("foo"), []byte("bar")}, []byte("\x04\x06foo\x06bar\x00"))

	testBinaryCodecPass(t, `{"type":"array","items":"bytes"}`, [][]byte(nil), []byte{0})                           // item count == 0
	testBinaryCodecPass(t, `{"type":"array","items":"bytes"}`, [][]byte{[]byte("foo")}, []byte("\x02\x06foo\x00")) // item count == 1, item 1 size == 3, foo, no more items
	testBinaryCodecPass(t, `{"type":"array","items":"bytes"}`, [][]byte{[]byte("foo"), []byte("bar")}, []byte("\x04\x06foo\x06bar\x00"))
}

func TestArrayEncodeError(t *testing.T) {
	// provided slice of primitive types that are not compatible with schema
	testBinaryEncodeFailBadDatumType(t, `{"type":"array","items":"int"}`, []string{"1"})
	testBinaryEncodeFailBadDatumType(t, `{"type":"array","items":"int"}`, []string{"1", "2"})
}

func TestArrayEncodeErrorFIXME(t *testing.T) {
	// NOTE: Would be better if returns error, however, because only the size is encoded, the
	// items encoder is never invoked to detect it is the wrong slice type
	if false {
		testBinaryEncodeFailBadDatumType(t, `{"type":"array","items":"int"}`, []string{})
	} else {
		testBinaryCodecPass(t, `{"type":"array","items":"int"}`, []string{}, []byte{0})
	}
}

func TestArrayTextDecodeFail(t *testing.T) {
	schema := `{"type":"array","items":"string"}`
	testTextDecodeFail(t, schema, []byte(`   "v1"  ,  "v2"  ]  `), "expected: '['")
	testTextDecodeFail(t, schema, []byte(` [  13  ,  "v2"  ]  `), "expected initial \"")
	testTextDecodeFail(t, schema, []byte(` [  "v1  ,  "v2"  ]  `), "expected ',' or ']'")
	testTextDecodeFail(t, schema, []byte(` [  "v1"    "v2"  ]  `), "expected ',' or ']'")
	testTextDecodeFail(t, schema, []byte(` [  "v1"  ,  13  ]  `), "expected initial \"")
	testTextDecodeFail(t, schema, []byte(` [  "v1"  ,  "v2  ]  `), "expected final \"")
	testTextDecodeFail(t, schema, []byte(` [  "v1"  ,  "v2"    `), "short buffer")
}

func TestArrayTextCodecPass(t *testing.T) {
	schema := `{"type":"array","items":"string"}`
	datum := []interface{}{"⌘ ", "value2"}
	testTextEncodePass(t, schema, datum, []byte(`["\u0001\u2318 ","value2"]`))
	testTextDecodePass(t, schema, datum, []byte(` [ "\u0001\u2318 " , "value2" ]`))
	testTextCodecPass(t, schema, []interface{}{}, []byte(`[]`)) // empty array
}
