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
	"strconv"
	"testing"
)

func TestSchemaUnion(t *testing.T) {
	testSchemaInvalid(t, `[{"type":"enum","name":"e1","symbols":["alpha","bravo"]},"e1"]`, "Union item 2 ought to be unique type")
	testSchemaInvalid(t, `[{"type":"enum","name":"com.example.one","symbols":["red","green","blue"]},{"type":"enum","name":"one","namespace":"com.example","symbols":["dog","cat"]}]`, "Union item 2 ought to be unique type")
}

func TestUnion(t *testing.T) {
	testBinaryCodecPass(t, `["null"]`, Union("null", nil), []byte("\x00"))
	testBinaryCodecPass(t, `["null","int"]`, Union("null", nil), []byte("\x00"))
	testBinaryCodecPass(t, `["int","null"]`, Union("null", nil), []byte("\x02"))

	testBinaryCodecPass(t, `["null","int"]`, Union("int", 3), []byte("\x02\x06"))
	testBinaryCodecPass(t, `["null","long"]`, Union("long", 3), []byte("\x02\x06"))

	testBinaryCodecPass(t, `["int","null"]`, Union("int", 3), []byte("\x00\x06"))
	testBinaryEncodePass(t, `["int","null"]`, Union("int", 3), []byte("\x00\x06")) // can encode a bare 3

	testBinaryEncodeFail(t, `[{"type":"enum","name":"colors","symbols":["red","green","blue"]},{"type":"enum","name":"animals","symbols":["dog","cat"]}]`, Union("colors", "bravo"), "value ought to be member of symbols")
	testBinaryEncodeFail(t, `[{"type":"enum","name":"colors","symbols":["red","green","blue"]},{"type":"enum","name":"animals","symbols":["dog","cat"]}]`, Union("animals", "bravo"), "value ought to be member of symbols")
	testBinaryCodecPass(t, `[{"type":"enum","name":"colors","symbols":["red","green","blue"]},{"type":"enum","name":"animals","symbols":["dog","cat"]}]`, Union("colors", "green"), []byte{0, 2})
	testBinaryCodecPass(t, `[{"type":"enum","name":"colors","symbols":["red","green","blue"]},{"type":"enum","name":"animals","symbols":["dog","cat"]}]`, Union("animals", "cat"), []byte{2, 2})
}

func TestUnionRejectInvalidType(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, `["null","long"]`, 3)
	testBinaryEncodeFailBadDatumType(t, `["null","int","long","float"]`, float64(3.5))
	testBinaryEncodeFailBadDatumType(t, `["null","long"]`, Union("int", 3))
	testBinaryEncodeFailBadDatumType(t, `["null","int","long","float"]`, Union("double", float64(3.5)))
}

func TestUnionWillCoerceTypeIfPossible(t *testing.T) {
	testBinaryCodecPass(t, `["null","long","float","double"]`, Union("long", int32(3)), []byte("\x02\x06"))
	testBinaryCodecPass(t, `["null","int","float","double"]`, Union("int", int64(3)), []byte("\x02\x06"))
	testBinaryCodecPass(t, `["null","int","long","double"]`, Union("double", float32(3.5)), []byte("\x06\x00\x00\x00\x00\x00\x00\f@"))
	testBinaryCodecPass(t, `["null","int","long","float"]`, Union("float", float64(3.5)), []byte("\x06\x00\x00\x60\x40"))
}

func TestUnionNumericCoercionGuardsPrecision(t *testing.T) {
	testBinaryEncodeFail(t, `["null","int","long","double"]`, Union("int", float32(3.5)), "lose precision")
}

func TestUnionWithArray(t *testing.T) {
	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, Union("null", nil), []byte("\x00"))

	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, Union("array", []interface{}{}), []byte("\x02\x00"))
	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, Union("array", []interface{}{1}), []byte("\x02\x02\x02\x00"))
	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, Union("array", []interface{}{1, 2}), []byte("\x02\x04\x02\x04\x00"))

	testBinaryCodecPass(t, `[{"type": "array", "items": "string"}, "null"]`, Union("null", nil), []byte{2})
	testBinaryCodecPass(t, `[{"type": "array", "items": "string"}, "null"]`, Union("array", []string{"foo"}), []byte("\x00\x02\x06foo\x00"))
	testBinaryCodecPass(t, `[{"type": "array", "items": "string"}, "null"]`, Union("array", []string{"foo", "bar"}), []byte("\x00\x04\x06foo\x06bar\x00"))
}

func TestUnionWithMap(t *testing.T) {
	testBinaryCodecPass(t, `["null",{"type":"map","values":"string"}]`, Union("null", nil), []byte("\x00"))
	testBinaryCodecPass(t, `["string",{"type":"map","values":"string"}]`, Union("map", map[string]interface{}{"He": "Helium"}), []byte("\x02\x02\x04He\x0cHelium\x00"))
	testBinaryCodecPass(t, `["string",{"type":"array","items":"string"}]`, Union("string", "Helium"), []byte("\x00\x0cHelium"))
}

func TestUnionMapRecordFitsInRecord(t *testing.T) {
	// union value may be either map or a record
	codec, err := NewCodec(`["null",{"type":"map","values":"double"},{"type":"record","name":"com.example.record","fields":[{"name":"field1","type":"int"},{"name":"field2","type":"float"}]}]`)
	if err != nil {
		t.Fatal(err)
	}

	// the provided datum value could be encoded by either the map or the record
	// schemas above
	datum := map[string]interface{}{
		"field1": 3,
		"field2": 3.5,
	}
	datumIn := Union("com.example.record", datum)

	buf, err := codec.BinaryFromNative(nil, datumIn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, []byte{
		0x04,                   // prefer record (union item 2) over map (union item 1)
		0x06,                   // field1 == 3
		0x00, 0x00, 0x60, 0x40, // field2 == 3.5
	}) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, []byte{byte(2)})
	}

	// round trip
	datumOut, buf, err := codec.NativeFromBinary(buf)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := len(buf), 0; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}

	datumOutMap, ok := datumOut.(map[string]interface{})
	if !ok {
		t.Fatalf("Actual: %#v; Expected: %#v", ok, false)
	}
	if actual, expected := len(datumOutMap), 1; actual != expected {
		t.Fatalf("Actual: %#v; Expected: %#v", actual, expected)
	}
	datumValue, ok := datumOutMap["com.example.record"]
	if !ok {
		t.Fatalf("Actual: %#v; Expected: %#v", datumOutMap, "have `com.example.record` key")
	}
	datumValueMap, ok := datumValue.(map[string]interface{})
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	if actual, expected := len(datumValueMap), len(datum); actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	for k, v := range datum {
		if actual, expected := fmt.Sprintf("%v", datumValueMap[k]), fmt.Sprintf("%v", v); actual != expected {
			t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
		}
	}
}

func TestUnionRecordFieldWhenNull(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "r1",
  "fields": [
    {"name": "f1", "type": [{"type": "array", "items": "string"}, "null"]}
  ]
}`

	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": Union("array", []interface{}{})}, []byte("\x00\x00"))
	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": Union("array", []string{"bar"})}, []byte("\x00\x02\x06bar\x00"))
	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": Union("array", []string{})}, []byte("\x00\x00"))
	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": Union("null", nil)}, []byte("\x02"))
	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": nil}, []byte("\x02"))
}

func TestUnionText(t *testing.T) {
	testTextEncodeFail(t, `["null","int"]`, Union("null", 3), "expected")
	testTextCodecPass(t, `["null","int"]`, Union("null", nil), []byte("null"))
	testTextCodecPass(t, `["null","int"]`, Union("int", 3), []byte(`{"int":3}`))
	testTextCodecPass(t, `["null","int","string"]`, Union("string", "😂 "), []byte(`{"string":"\u0001\uD83D\uDE02 "}`))
}

func ExampleUnion() {
	codec, err := NewCodec(`["null","string","int"]`)
	if err != nil {
		fmt.Println(err)
	}
	buf, err := codec.TextualFromNative(nil, Union("string", "some string"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(buf))
	// Output: {"string":"some string"}
}

func ExampleUnion3() {
	// Imagine a record field with the following union type. I have seen this
	// sort of type in many schemas. I have been told the reasoning behind it is
	// when the writer desires to encode data to JSON that cannot be written as
	// a JSON number, then to encode it as a string and allow the reader to
	// parse the string accordingly.
	codec, err := NewCodec(`["null","double","string"]`)
	if err != nil {
		fmt.Println(err)
	}

	native, _, err := codec.NativeFromTextual([]byte(`{"string":"NaN"}`))
	if err != nil {
		fmt.Println(err)
	}

	value := math.NaN()
	if native == nil {
		fmt.Print("decoded null: ")
	} else {
		for k, v := range native.(map[string]interface{}) {
			switch k {
			case "double":
				fmt.Print("decoded double: ")
				value = v.(float64)
			case "string":
				fmt.Print("decoded string: ")
				s := v.(string)
				switch s {
				case "NaN":
					value = math.NaN()
				case "+Infinity":
					value = math.Inf(1)
				case "-Infinity":
					value = math.Inf(-1)
				default:
					var err error
					value, err = strconv.ParseFloat(s, 64)
					if err != nil {
						fmt.Println(err)
					}
				}
			}
		}
	}
	fmt.Println(value)
	// Output: decoded string: NaN
}
