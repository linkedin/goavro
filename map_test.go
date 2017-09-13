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
	"fmt"
	"log"
	"testing"
)

func TestMapSchema(t *testing.T) {
	// NOTE: This schema also used to read and write files in OCF format
	testSchemaValid(t, `{"type":"map","values":"bytes"}`)

	testSchemaInvalid(t, `{"type":"map","value":"int"}`, "Map ought to have values key")
	testSchemaInvalid(t, `{"type":"map","values":"integer"}`, "Map values ought to be valid Avro type")
	testSchemaInvalid(t, `{"type":"map","values":3}`, "Map values ought to be valid Avro type")
	testSchemaInvalid(t, `{"type":"map","values":int}`, "invalid character") // type name must be quoted
}

func TestMapDecodeInitialBlockCountCannotDecode(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, nil, "block count")
}

func TestMapDecodeInitialBlockCountZero(t *testing.T) {
	testBinaryDecodePass(t, `{"type":"map","values":"int"}`, map[string]interface{}{}, []byte{0})
}

func TestMapDecodeInitialBlockCountNegative(t *testing.T) {
	testBinaryDecodePass(t, `{"type":"map","values":"int"}`, map[string]interface{}{"k1": 3}, []byte{1, 2, 4, 'k', '1', 6, 0})
}

func TestMapDecodeInitialBlockCountTooLarge(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, morePositiveThanMaxBlockCount, "block count")
}

func TestMapDecodeInitialBlockCountNegativeTooLarge(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, append(moreNegativeThanMaxBlockCount, byte(0)), "block count")
}

func TestMapDecodeInitialBlockCountTooNegative(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, append(mostNegativeBlockCount, byte(0)), "block count")
}

func TestMapDecodeNextBlockCountCannotDecode(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, []byte{1, 2, 4, 'k', '1', 6}, "block count")
}

func TestMapDecodeNextBlockCountNegative(t *testing.T) {
	c, err := NewCodec(`{"type":"map","values":"int"}`)
	if err != nil {
		t.Fatal(err)
	}

	decoded, _, err := c.NativeFromBinary([]byte{1, 2, 4, 'k', '1', 6, 1, 8, 4, 'k', '2', 0x1a, 0})
	if err != nil {
		t.Fatal(err)
	}

	decodedMap, ok := decoded.(map[string]interface{})
	if !ok {
		t.Fatalf("Actual: %v; Expected: %v", ok, true)
	}

	value, ok := decodedMap["k1"]
	if !ok {
		t.Errorf("Actual: %v; Expected: %v", ok, true)
	}
	if actual, expected := value.(int32), int32(3); actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}

	value, ok = decodedMap["k2"]
	if !ok {
		t.Errorf("Actual: %v; Expected: %v", ok, true)
	}
	if actual, expected := value.(int32), int32(13); actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}
}

func TestMapDecodeNextBlockCountTooLarge(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, append([]byte{1, 2, 4, 'k', '1', 6}, morePositiveThanMaxBlockCount...), "block count")
}

func TestMapDecodeNextBlockCountNegativeTooLarge(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, append(append([]byte{1, 2, 4, 'k', '1', 6}, moreNegativeThanMaxBlockCount...), 2), "block count")
}

func TestMapDecodeNextBlockCountTooNegative(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, append(append([]byte{1, 2, 4, 'k', '1', 6}, mostNegativeBlockCount...), 2), "block count")
}

func TestMapDecodeFail(t *testing.T) {
	schema := `{"type":"map","values":"boolean"}`
	testBinaryDecodeFail(t, schema, nil, "cannot decode binary map block count")           // leading block count
	testBinaryDecodeFail(t, schema, []byte("\x01"), "cannot decode binary map block size") // when block count < 0
	testBinaryDecodeFail(t, schema, []byte("\x02\x04"), "cannot decode binary map key")
	testBinaryDecodeFail(t, schema, []byte("\x02\x04"), "cannot decode binary map key")
	testBinaryDecodeFail(t, schema, []byte("\x02\x04a"), "cannot decode binary map key")
	testBinaryDecodeFail(t, schema, []byte("\x02\x04ab"), `cannot decode binary map value for key "ab"`)
	testBinaryDecodeFail(t, schema, []byte("\x02\x04ab\x02"), "boolean: expected")
	testBinaryDecodeFail(t, schema, []byte("\x02\x04ab\x01"), "cannot decode binary map block count") // trailing block count
	testBinaryDecodeFail(t, schema, []byte("\x04\x04ab\x00\x04ab\x00\x00"), "duplicate key")
}

func TestMap(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"map","values":"null"}`, map[string]interface{}{"ab": nil}, []byte("\x02\x04ab\x00"))
	testBinaryCodecPass(t, `{"type":"map","values":"boolean"}`, map[string]interface{}{"ab": true}, []byte("\x02\x04ab\x01\x00"))
}

func TestMapTextDecodeFail(t *testing.T) {
	schema := `{"type":"map","values":"string"}`
	testTextDecodeFail(t, schema, []byte(`    "string"  :  "silly"  ,   "bytes"  : "silly" } `), "expected: '{'")
	testTextDecodeFail(t, schema, []byte(`  {  16  :  "silly"  ,   "bytes"  : "silly" } `), "expected initial \"")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  ,  "silly"  ,   "bytes"  : "silly" } `), "expected: ':'")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  13  ,   "bytes"  : "silly" } `), "expected initial \"")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly" :   "bytes"  : "silly" } `), "expected ',' or '}'")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly"    "bytes"  : "silly" } `), "expected ',' or '}'")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly" ,   "bytes"  : "silly"  `), "short buffer")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly"  `), "short buffer")
	testTextDecodeFail(t, schema, []byte(`{"key1":"\u0001\u2318 ","key1":"value2"}`), "duplicate key")
}

func TestMapTextCodecPass(t *testing.T) {
	schema := `{"type":"map","values":"string"}`
	datum := map[string]interface{}{"key1": "⌘ "}
	testTextCodecPass(t, schema, make(map[string]interface{}), []byte(`{}`)) // empty map
	testTextEncodePass(t, schema, datum, []byte(`{"key1":"\u0001\u2318 "}`))
	testTextDecodePass(t, schema, datum, []byte(` { "key1" : "\u0001\u2318 " }`))
}

func TestMapBinaryReceiveSliceInt(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"map","values":"int"}`, map[string]int{}, []byte("\x00"))
	testBinaryCodecPass(t, `{"type":"map","values":"int"}`, map[string]int{"k1": 13}, []byte("\x02\x04k1\x1a\x00"))
	testBinaryEncodeFail(t, `{"type":"map","values":"int"}`, map[int]int{42: 13}, "cannot create map[string]interface{}")
}

func TestMapTextualReceiveSliceInt(t *testing.T) {
	testTextCodecPass(t, `{"type":"map","values":"int"}`, map[string]int{}, []byte(`{}`))
	testTextCodecPass(t, `{"type":"map","values":"int"}`, map[string]int{"k1": 13}, []byte(`{"k1":13}`))
	testTextEncodeFail(t, `{"type":"map","values":"int"}`, map[int]int{42: 13}, "cannot create map[string]interface{}")
}

func ExampleMap() {
	codec, err := NewCodec(`{
            "name": "r1",
            "type": "record",
            "fields": [{
                "name": "f1",
                "type": {"type":"map","values":"double"}
            }]
        }`)
	if err != nil {
		log.Fatal(err)
	}

	buf, err := codec.TextualFromNative(nil, map[string]interface{}{
		"f1": map[string]float64{
			"k1": 3.5,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(buf))
	// Output: {"f1":{"k1":3.5}}
}
