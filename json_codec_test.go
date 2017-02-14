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
	"reflect"
	"testing"
)

////////////////////////////////////////
// helpers
////////////////////////////////////////

func checkCodecJSONDecoderError(t *testing.T, schema string, bits []byte, expectedError interface{}) {
	codec, err := NewJSONCodec(schema)
	checkErrorFatal(t, err, nil)
	bb := bytes.NewBuffer(bits)
	_, err = codec.Decode(bb)
	checkError(t, err, expectedError)
}

func checkCodecJSONDecoderResult(t *testing.T, schema string, bits []byte, datum interface{}) {
	codec, err := NewJSONCodec(schema)
	checkErrorFatal(t, err, nil)
	bb := bytes.NewBuffer(bits)
	decoded, err := codec.Decode(bb)
	checkErrorFatal(t, err, nil)

	if reflect.TypeOf(decoded) == reflect.TypeOf(datum) {
		switch datum.(type) {
		case []byte:
			if bytes.Compare(decoded.([]byte), datum.([]byte)) != 0 {
				t.Errorf("Actual: %#v; Expected: %#v", decoded, datum)
			}
		case Fixed:
			if actual, expected := decoded.(Fixed).Name, datum.(Fixed).Name; actual != expected {
				t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
			}
			if actual, expected := decoded.(Fixed).Value, datum.(Fixed).Value; bytes.Compare(actual, expected) != 0 {
				t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
			}
		default:
			if decoded != datum {
				t.Errorf("Actual: %v; Expected: %v", decoded, datum)
			}
		}
	} else {
		t.Errorf("Actual: %T; Expected: %T", decoded, datum)
	}
}

func checkCodecJSONEncoderError(t *testing.T, schema string, datum interface{}, expectedError interface{}) {
	bb := new(bytes.Buffer)
	codec, err := NewJSONCodec(schema)
	checkErrorFatal(t, err, nil)

	err = codec.Encode(bb, datum)
	checkErrorFatal(t, err, expectedError)
}

func checkCodecJSONEncoderResult(t *testing.T, schema string, datum interface{}, bits []byte) {
	// test against both bytes.Buffer and simpleBuffer
	test := func(t *testing.T, schema string, datum interface{}, bits []byte, wb testBuffer) {
		codec, err := NewJSONCodec(schema)
		checkErrorFatal(t, err, nil)

		err = codec.Encode(wb, datum)
		if err != nil {
			t.Errorf("Encode Error Testing %v %v Actual: %v; Expected: %v", schema, datum, err, bits)
		}

		if !bytes.Equal(wb.Bytes(), bits) {
			t.Errorf("Testing %v %v Actual: %#v; Expected: %#v", schema, datum, wb.Bytes(), bits)
		}
	}
	test(t, schema, datum, bits, new(bytes.Buffer))
	test(t, schema, datum, bits, new(simpleBuffer))
}

func checkCodecJSONRoundTrip(t *testing.T, schema string, datum interface{}) {
	// test against both bytes.Buffer and simpleBuffer
	test := func(t *testing.T, schema string, datum interface{}, wb testBuffer) {
		codec, err := NewJSONCodec(schema)
		if err != nil {
			t.Errorf("%v %v %v", schema, datum, err)
			return
		}
		err = codec.Encode(wb, datum)
		if err != nil {
			t.Errorf("%v %v %v", schema, datum, err)
			return
		}
		actual, err := codec.Decode(wb)
		if err != nil {
			t.Errorf("%v %v %v", schema, datum, err)
			return
		}
		actualJSON, err := json.Marshal(actual)
		if err != nil {
			t.Errorf("%v %v %v", schema, datum, err)
			return
		}
		expectedJSON, err := json.Marshal(datum)
		if err != nil {
			t.Errorf("%v %v %v", schema, datum, err)
			return
		}
		if !bytes.Equal(actualJSON, expectedJSON) {
			t.Errorf("Actual: %q; Expected: %q", actualJSON, expectedJSON)
		}
	}
	test(t, schema, datum, new(bytes.Buffer))
	test(t, schema, datum, new(simpleBuffer))
}

func TestJSONCodecRoundTrip(t *testing.T) {
	// null
	checkCodecJSONRoundTrip(t, `"null"`, nil)
	checkCodecJSONRoundTrip(t, `{"type":"null"}`, nil)
	// boolean
	checkCodecJSONRoundTrip(t, `"boolean"`, false)
	checkCodecJSONRoundTrip(t, `"boolean"`, true)
	// int
	checkCodecJSONRoundTrip(t, `"int"`, int32(-3))
	checkCodecJSONRoundTrip(t, `"int"`, int32(-65))
	checkCodecJSONRoundTrip(t, `"int"`, int32(0))
	checkCodecJSONRoundTrip(t, `"int"`, int32(1016))
	checkCodecJSONRoundTrip(t, `"int"`, int32(3))
	checkCodecJSONRoundTrip(t, `"int"`, int32(42))
	checkCodecJSONRoundTrip(t, `"int"`, int32(64))
	checkCodecJSONRoundTrip(t, `"int"`, int32(66052))
	checkCodecJSONRoundTrip(t, `"int"`, int32(8454660))
	checkCodecJSONRoundTrip(t, `"int"`, int32(2147483647))
	checkCodecJSONRoundTrip(t, `"int"`, int32(-2147483647))
	checkCodecJSONRoundTrip(t, `"int"`, int32(1455301406))
	// long
	checkCodecJSONRoundTrip(t, `"long"`, int64(-2147483648))
	checkCodecJSONRoundTrip(t, `"long"`, int64(-3))
	checkCodecJSONRoundTrip(t, `"long"`, int64(-65))
	checkCodecJSONRoundTrip(t, `"long"`, int64(0))
	checkCodecJSONRoundTrip(t, `"long"`, int64(1082196484))
	checkCodecJSONRoundTrip(t, `"long"`, int64(138521149956))
	checkCodecJSONRoundTrip(t, `"long"`, int64(17730707194372))
	checkCodecJSONRoundTrip(t, `"long"`, int64(2147483647))
	checkCodecJSONRoundTrip(t, `"long"`, int64(2269530520879620))
	checkCodecJSONRoundTrip(t, `"long"`, int64(3))
	checkCodecJSONRoundTrip(t, `"long"`, int64(64))

	checkCodecJSONRoundTrip(t, `"long"`, int64(-(1 << 63)))
	checkCodecJSONRoundTrip(t, `"long"`, int64((1<<63)-1))
	checkCodecJSONRoundTrip(t, `"long"`, int64(5959107741628848600))
	checkCodecJSONRoundTrip(t, `"long"`, int64(1359702038045356208))
	checkCodecJSONRoundTrip(t, `"long"`, int64(-5513458701470791632)) // https://github.com/linkedin/goavro/issues/49

	// float
	checkCodecJSONRoundTrip(t, `"float"`, float32(3.5))
	// checkCodecJSONRoundTrip(t, `"float"`, float32(math.Inf(-1)))
	// checkCodecJSONRoundTrip(t, `"float"`, float32(math.Inf(1)))
	// checkCodecJSONRoundTrip(t, `"float"`, float32(math.NaN()))
	// double
	checkCodecJSONRoundTrip(t, `"double"`, float64(3.5))
	// checkCodecJSONRoundTrip(t, `"double"`, float64(math.Inf(-1)))
	// checkCodecJSONRoundTrip(t, `"double"`, float64(math.Inf(1)))
	// checkCodecJSONRoundTrip(t, `"double"`, float64(math.NaN()))
	// bytes
	checkCodecJSONRoundTrip(t, `"bytes"`, []byte(""))
	checkCodecJSONRoundTrip(t, `"bytes"`, []byte("some bytes"))
	// string
	checkCodecJSONRoundTrip(t, `"string"`, "")
	checkCodecJSONRoundTrip(t, `"string"`, "filibuster")
}

func TestJSONCodecDecoderPrimitives(t *testing.T) {
	// null
	checkCodecJSONDecoderResult(t, `"null"`, []byte("null"), nil)
	// boolean
	checkCodecJSONDecoderError(t, `"boolean"`, []byte(""), "cannot decode boolean")
	checkCodecJSONDecoderError(t, `"boolean"`, []byte(""), "cannot decode boolean: EOF")
	checkCodecJSONDecoderResult(t, `"boolean"`, []byte("false"), false)
	checkCodecJSONDecoderResult(t, `"boolean"`, []byte("true"), true)
	// int
	checkCodecJSONDecoderError(t, `"int"`, []byte(""), "cannot decode int: EOF")
	checkCodecJSONDecoderResult(t, `"int"`, []byte("0"), int32(0))
	checkCodecJSONDecoderResult(t, `"int"`, []byte("-3"), int32(-3))
	checkCodecJSONDecoderResult(t, `"int"`, []byte("3"), int32(3))
	checkCodecJSONDecoderResult(t, `"int"`, []byte("64"), int32(64))
	checkCodecJSONDecoderResult(t, `"int"`, []byte("-65"), int32(-65))
	checkCodecJSONDecoderResult(t, `"int"`, []byte("1016"), int32(1016))
	checkCodecJSONDecoderResult(t, `"int"`, []byte("66052"), int32(66052))
	checkCodecJSONDecoderResult(t, `"int"`, []byte("8454660"), int32(8454660))
	// long
	checkCodecJSONDecoderError(t, `"long"`, []byte(""), "cannot decode long: EOF")
	checkCodecJSONDecoderResult(t, `"long"`, []byte("0"), int64(0))
	checkCodecJSONDecoderResult(t, `"long"`, []byte("-3"), int64(-3))
	checkCodecJSONDecoderResult(t, `"long"`, []byte("3"), int64(3))
	checkCodecJSONDecoderResult(t, `"long"`, []byte("64"), int64(64))
	checkCodecJSONDecoderResult(t, `"long"`, []byte("-65"), int64(-65))
	checkCodecJSONDecoderResult(t, `"long"`, []byte("2147483647"), int64(2147483647))
	checkCodecJSONDecoderResult(t, `"long"`, []byte("-2147483648"), int64(-2147483648))
	checkCodecJSONDecoderResult(t, `"long"`, []byte("1082196484"), int64(1082196484))
	checkCodecJSONDecoderResult(t, `"long"`, []byte("138521149956"), int64(138521149956))
	checkCodecJSONDecoderResult(t, `"long"`, []byte("17730707194372"), int64(17730707194372))
	checkCodecJSONDecoderResult(t, `"long"`, []byte("2269530520879620"), int64(2269530520879620))
	checkCodecJSONDecoderResult(t, `"long"`, []byte("-5513458701470791632"), int64(-5513458701470791632)) // https://github.com/linkedin/goavro/issues/49
	// float
	checkCodecJSONDecoderError(t, `"float"`, []byte(""), "cannot decode float: EOF")
	checkCodecJSONDecoderResult(t, `"float"`, []byte("3.5"), float32(3.5))
	// double
	checkCodecJSONDecoderError(t, `"double"`, []byte(""), "cannot decode double: EOF")
	checkCodecJSONDecoderResult(t, `"double"`, []byte("3.5"), float64(3.5))
	// bytes
	checkCodecJSONDecoderError(t, `"bytes"`, []byte(""), "cannot decode bytes: EOF")
	checkCodecJSONDecoderResult(t, `"bytes"`, []byte("\"\""), []byte(""))
	checkCodecJSONDecoderResult(t, `"bytes"`, []byte("\"some bytes\""), []byte("some bytes"))
	// string
	checkCodecJSONDecoderError(t, `"string"`, []byte(""), "cannot decode string: EOF")
	checkCodecJSONDecoderResult(t, `"string"`, []byte("\"\""), "")
	checkCodecJSONDecoderResult(t, `"string"`, []byte("\"some string\""), "some string")
}

func TestCodecJSONEncoderPrimitives(t *testing.T) {
	// null
	checkCodecJSONEncoderResult(t, `"null"`, nil, []byte("null"))
	checkCodecJSONEncoderResult(t, `{"type":"null"}`, nil, []byte("null"))
	// boolean
	checkCodecJSONEncoderResult(t, `"boolean"`, false, []byte("false"))
	checkCodecJSONEncoderResult(t, `"boolean"`, true, []byte("true"))
	// int
	checkCodecJSONEncoderResult(t, `"int"`, int32(-53), []byte("-53"))
	checkCodecJSONEncoderResult(t, `"int"`, int32(-33), []byte("-33"))
	checkCodecJSONEncoderResult(t, `"int"`, int32(-3), []byte("-3"))
	checkCodecJSONEncoderResult(t, `"int"`, int32(-65), []byte("-65"))
	checkCodecJSONEncoderResult(t, `"int"`, int32(0), []byte("0"))
	checkCodecJSONEncoderResult(t, `"int"`, int32(1016), []byte("1016"))
	checkCodecJSONEncoderResult(t, `"int"`, int32(3), []byte("3"))
	checkCodecJSONEncoderResult(t, `"int"`, int32(42), []byte("42"))
	checkCodecJSONEncoderResult(t, `"int"`, int32(64), []byte("64"))
	checkCodecJSONEncoderResult(t, `"int"`, int32(66052), []byte("66052"))
	checkCodecJSONEncoderResult(t, `"int"`, int32(8454660), []byte("8454660"))
	checkCodecJSONEncoderResult(t, `"int"`, int32(2147483647), []byte("2147483647"))
	checkCodecJSONEncoderResult(t, `"int"`, int32(-2147483647), []byte("-2147483647"))
	// long
	checkCodecJSONEncoderResult(t, `"long"`, int64(-2147483648), []byte("-2147483648"))
	checkCodecJSONEncoderResult(t, `"long"`, int64(-3), []byte("-3"))
	checkCodecJSONEncoderResult(t, `"long"`, int64(-65), []byte("-65"))
	checkCodecJSONEncoderResult(t, `"long"`, int64(0), []byte("0"))
	checkCodecJSONEncoderResult(t, `"long"`, int64(1082196484), []byte("1082196484"))
	checkCodecJSONEncoderResult(t, `"long"`, int64(138521149956), []byte("138521149956"))
	checkCodecJSONEncoderResult(t, `"long"`, int64(17730707194372), []byte("17730707194372"))
	checkCodecJSONEncoderResult(t, `"long"`, int64(2147483647), []byte("2147483647"))
	checkCodecJSONEncoderResult(t, `"long"`, int64(2269530520879620), []byte("2269530520879620"))
	checkCodecJSONEncoderResult(t, `"long"`, int64(3), []byte("3"))
	checkCodecJSONEncoderResult(t, `"long"`, int64(64), []byte("64"))
	checkCodecJSONEncoderResult(t, `"long"`, int64(-5513458701470791632), []byte("-5513458701470791632")) // https://github.com/linkedin/goavro/issues/49
	// float
	checkCodecJSONEncoderResult(t, `"float"`, float32(3.5), []byte("3.5"))
	// double
	checkCodecJSONEncoderResult(t, `"double"`, float64(3.5), []byte("3.5"))
	// bytes
	checkCodecJSONEncoderResult(t, `"bytes"`, []byte(""), []byte("\"\""))
	checkCodecJSONEncoderResult(t, `"bytes"`, []byte("some bytes"), []byte("\"some bytes\""))
	// string
	checkCodecJSONEncoderResult(t, `"string"`, "", []byte("\"\""))
	checkCodecJSONEncoderResult(t, `"string"`, "filibuster", []byte("\"filibuster\""))
}

func TestCodecJSONUnionPrimitives(t *testing.T) {
	// null
	checkCodecJSONEncoderResult(t, `["null"]`, nil, []byte("null"))
	checkCodecJSONEncoderResult(t, `[{"type":"null"}]`, nil, []byte("null"))
	// boolean
	checkCodecJSONEncoderResult(t, `["null","boolean"]`, nil, []byte("null"))
	checkCodecJSONEncoderResult(t, `["null","boolean"]`, false, []byte("{\"boolean\":false}"))
	checkCodecJSONEncoderResult(t, `["null","boolean"]`, true, []byte("{\"boolean\":true}"))
	// int
	checkCodecJSONEncoderResult(t, `["null","int"]`, nil, []byte("null"))
	checkCodecJSONEncoderResult(t, `["boolean","int"]`, true, []byte("{\"boolean\":true}"))
	checkCodecJSONEncoderResult(t, `["boolean","int"]`, int32(3), []byte("{\"int\":3}"))
	checkCodecJSONEncoderResult(t, `["int",{"type":"boolean"}]`, int32(42), []byte("{\"int\":42}"))
	// long
	checkCodecJSONEncoderResult(t, `["boolean","long"]`, int64(3), []byte("{\"long\":3}"))
	// float
	checkCodecJSONEncoderResult(t, `["int","float"]`, float32(3.5), []byte("{\"float\":3.5}"))
	// double
	checkCodecJSONEncoderResult(t, `["float","double"]`, float64(3.5), []byte("{\"double\":3.5}"))
	// bytes
	checkCodecJSONEncoderResult(t, `["int","bytes"]`, []byte("foobar"), []byte("{\"bytes\":\"foobar\"}"))
	// string
	checkCodecJSONEncoderResult(t, `["string","float"]`, "filibuster", []byte("{\"string\":\"filibuster\"}"))
}

func TestCodecJSONDecoderUnion(t *testing.T) {
	checkCodecJSONDecoderResult(t, `["string","float"]`, []byte("{\"string\":\"filibuster\"}"), "filibuster")
	checkCodecJSONDecoderResult(t, `["string","int"]`, []byte("{\"int\":13}"), int32(13))
}

func TestCodecJSONEncoderUnionArray(t *testing.T) {
	checkCodecJSONEncoderResult(t, `[{"type":"array","items":"int"},"string"]`, "filibuster", []byte("{\"string\":\"filibuster\"}"))

	var someArray []interface{}
	someArray = append(someArray, int32(3))
	someArray = append(someArray, int32(13))
	checkCodecJSONEncoderResult(t, `[{"type":"array","items":"int"},"string"]`, someArray, []byte("{\"array\":[3,13]}"))
}

func TestCodecJSONEncoderUnionEnum(t *testing.T) {
	checkCodecJSONEncoderResult(t, `["null",{"type":"enum","name":"color_enum","symbols":["red","blue","green"]}]`, nil, []byte("null"))
	checkCodecJSONEncoderResult(t, `["null",{"type":"enum","name":"color_enum","symbols":["red","blue","green"]}]`, Enum{"color_enum", "blue"}, []byte("{\"color_enum\":\"blue\"}"))
	checkCodecJSONEncoderError(t, `["null",{"type":"enum","name":"color_enum","symbols":["red","blue","green"]}]`, Enum{"color_enum", "purple"}, "symbol not defined: purple")
}

func TestCodecJSONEncoderUnionMap(t *testing.T) {
	someMap := make(map[string]interface{})
	someMap["superhero"] = "Batman"
	checkCodecJSONEncoderResult(t, `["null",{"type":"map","values":"string"}]`, someMap, []byte("{\"map\":{\"superhero\":\"Batman\"}}"))
	checkCodecJSONRoundTrip(t, `["null",{"type":"map","values":"string"}]`, someMap)
}

func TestCodecJSONEncoderUnionEmptyMap(t *testing.T) {
	someMap := make(map[string]interface{})
	checkCodecJSONEncoderResult(t, `["null",{"type":"map","values":"double"}]`, someMap, []byte("{\"map\":{}}"))
	checkCodecJSONRoundTrip(t, `["null",{"type":"map","values":"double"}]`, someMap)
}

func TestCodecJSONEncoderUnionRecord(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"record1","fields":[{"type":"int","name":"field1"},{"type":"string","name":"field2"}]}`

	someRecord, err := NewRecord(RecordSchema(recordSchemaJSON))
	checkErrorFatal(t, err, nil)

	someRecord.Set("field1", int32(13))
	someRecord.Set("field2", "Superman")

	bits := []byte("{\"record1\":{\"field1\":13,\"field2\":\"Superman\"}}")
	checkCodecJSONEncoderResult(t, `["null",`+recordSchemaJSON+`]`, someRecord, bits)
}

func TestCodecJSONDecoderEnum(t *testing.T) {
	schema := `{"type":"enum","name":"cards","symbols":["HEARTS","DIAMONDS","SPADES","CLUBS"]}`
	checkCodecJSONDecoderError(t, schema, []byte("\x01"), "cannot decode enum (cards)")
	checkCodecJSONDecoderResult(t, schema, []byte("\"SPADES\""), Enum{"cards", "SPADES"})
}

func TestCodecJSONEncoderEnum(t *testing.T) {
	schema := `{"type":"enum","name":"cards","symbols":["HEARTS","DIAMONDS","SPADES","CLUBS"]}`
	checkCodecJSONEncoderResult(t, schema, Enum{"cards", "SPADES"}, []byte("\"SPADES\""))
	checkCodecJSONEncoderError(t, schema, Enum{"cards", "PINEAPPLE"}, "symbol not defined")
	checkCodecJSONEncoderError(t, schema, []byte("\x01"), "cannot encode enum (cards): expected: Enum or string; received: []uint8")
	checkCodecJSONEncoderError(t, schema, "some symbol not in schema", "cannot encode enum (cards): symbol not defined: some symbol not in schema")
}

func TestCodecJSONFixed(t *testing.T) {
	schema := `{"type":"fixed","name":"fixed1","size":5}`
	checkCodecDecoderError(t, schema, []byte(""), "EOF")
	checkCodecDecoderError(t, schema, []byte("hap"), "buffer underrun")
	checkCodecEncoderError(t, schema, "happy day", "expected: Fixed; received: string")
	checkCodecEncoderError(t, schema, Fixed{Name: "fixed1", Value: []byte("day")}, "expected: 5 bytes; received: 3")
	checkCodecEncoderError(t, schema, Fixed{Name: "fixed1", Value: []byte("happy day")}, "expected: 5 bytes; received: 9")
	checkCodecEncoderResult(t, schema, Fixed{Name: "fixed1", Value: []byte("happy")}, []byte("happy"))
}

func TestCodecFixedJSONDecoder(t *testing.T) {
	schema := `
{
    "name": "messageId",
    "type": {
        "type": "fixed",
        "size": 16,
        "name": "UUID",
        "namespace": "com.example"
    },
    "doc": "A unique identifier for the message"
}`
	bits := []byte{0x12, 0x7f, 0xe9, 0xc0, 0x3b, 0x59, 0x41, 0xf5, 0x93, 0x6d, 0x77, 0x75, 0xeb, 0x84, 0xb3, 0xc7}
	expected := Fixed{Name: "com.example.UUID", Value: bits}
	checkCodecDecoderResult(t, schema, bits, expected)
}

func TestCodecJSONNamedTypes(t *testing.T) {
	schema := `{"name":"guid","type":["null",{"type":"fixed","name":"fixed_16","size":16}],"doc":"event unique id"}`
	// The 0x2 byte is an avro encoded int(1), which refers to the index of the
	// `fixed_16` type in the schema's union array.
	checkCodecJSONEncoderResult(t, schema, Fixed{Name: "fixed_16", Value: []byte("0123456789abcdef")},
		[]byte("{\"fixed_16\":\"0123456789abcdef\"}"))
}

func TestCodecJSONEncoderArrayChecksSchema(t *testing.T) {
	_, err := NewJSONCodec(`{"type":"array"}`)
	checkErrorFatal(t, err, "ought to have items key")

	_, err = NewJSONCodec(`{"type":"array","items":"flubber"}`)
	checkErrorFatal(t, err, "unknown type name")

	checkCodecJSONEncoderError(t, `{"type":"array","items":"long"}`, int64(5), "expected: []interface{}; received: int64")
}

func TestCodecJSONDecoderArrayEOF(t *testing.T) {
	schema := `{"type":"array","items":"string"}`
	checkCodecJSONDecoderError(t, schema, []byte(""), "cannot decode array")
}

func TestCodecJSONDecoderArrayEmpty(t *testing.T) {
	schema := `{"type":"array","items":"string"}`
	codec, err := NewJSONCodec(schema)
	checkErrorFatal(t, err, nil)

	bb := bytes.NewBuffer([]byte("[]"))
	actual, err := codec.Decode(bb)
	checkError(t, err, nil)

	someArray, ok := actual.([]interface{})
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	if len(someArray) != 0 {
		t.Errorf("Actual: %#v; Expected: %#v", len(someArray), 0)
	}
}

func TestCodecJSONDecoderArray(t *testing.T) {
	schema := `{"type":"array","items":"int"}`
	codec, err := NewJSONCodec(schema)
	checkErrorFatal(t, err, nil)

	bb := bytes.NewBuffer([]byte("[3,27]"))
	actual, err := codec.Decode(bb)
	checkError(t, err, nil)

	someArray, ok := actual.([]interface{})
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	expected := []int32{3, 27}
	if len(someArray) != len(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", len(someArray), len(expected))
	}
	if len(someArray) != len(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", len(someArray), len(expected))
	}
	for i, v := range someArray {
		val, ok := v.(int32)
		if !ok {
			t.Errorf("Actual: %#v; Expected: %#v", ok, true)
		}
		if val != expected[i] {
			t.Errorf("Actual: %#v; Expected: %#v", val, expected[i])
		}
	}
}

func TestCodecJSONDecoderArrayOfRecords(t *testing.T) {
	schema := `
{
  "type": "array",
  "items": {
    "type": "record",
    "name": "someRecord",
    "fields": [
      {
        "name": "someString",
        "type": "string"
      },
      {
        "name": "someInt",
        "type": "int"
      }
    ]
  }
}
`
	codec, err := NewJSONCodec(schema)
	checkErrorFatal(t, err, nil)

	encoded := []byte("[{\"someString\":\"Hello\",\"someInt\":13},{\"someString\":\"World\",\"someInt\":42}]")
	bb := bytes.NewBuffer(encoded)
	actual, err := codec.Decode(bb)
	checkError(t, err, nil)

	someArray, ok := actual.([]interface{})
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	if len(someArray) != 2 {
		t.Errorf("Actual: %#v; Expected: %#v", len(someArray), 2)
	}
	// first element
	actualString, err := someArray[0].(*Record).Get("someString")
	checkError(t, err, nil)
	expectedString := "Hello"
	if actualString != expectedString {
		t.Errorf("Actual: %#v; Expected: %#v", actualString, expectedString)
	}
	actualInt, err := someArray[0].(*Record).Get("someInt")
	checkError(t, err, nil)
	expectedInt := int32(13)
	if actualInt != expectedInt {
		t.Errorf("Actual: %#v; Expected: %#v", actualInt, expectedInt)
	}
	// second element
	actualString, err = someArray[1].(*Record).Get("someString")
	checkError(t, err, nil)
	expectedString = "World"
	if actualString != expectedString {
		t.Errorf("Actual: %#v; Expected: %#v", actualString, expectedString)
	}
	actualInt, err = someArray[1].(*Record).Get("someInt")
	checkError(t, err, nil)
	expectedInt = int32(42)
	if actualInt != expectedInt {
		t.Errorf("Actual: %#v; Expected: %#v", actualInt, expectedInt)
	}
}

func TestCodecJSONDecoderArrayMultipleBlocks(t *testing.T) {
	schema := `{"type":"array","items":"int"}`
	codec, err := NewJSONCodec(schema)
	checkErrorFatal(t, err, nil)

	bb := bytes.NewBuffer([]byte("[3,4,5,27,6]"))
	actual, err := codec.Decode(bb)
	checkError(t, err, nil)

	someArray, ok := actual.([]interface{})
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	expected := []int32{3, 4, 5, 27, 6}
	if len(someArray) != len(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", len(someArray), len(expected))
	}
	for i, v := range someArray {
		val, ok := v.(int32)
		if !ok {
			t.Errorf("Actual: %#v; Expected: %#v", ok, true)
		}
		if val != expected[i] {
			t.Errorf("Actual: %#v; Expected: %#v", val, expected[i])
		}
	}
}

func TestCodecJSONEncoderArray(t *testing.T) {
	schema := `{"type":"array","items":{"type":"long"}}`

	var datum []interface{}
	datum = append(datum, int64(-1))
	datum = append(datum, int64(-2))
	datum = append(datum, int64(-3))
	datum = append(datum, int64(-4))
	datum = append(datum, int64(-5))
	datum = append(datum, int64(-6))
	datum = append(datum, int64(0))
	datum = append(datum, int64(1))
	datum = append(datum, int64(2))
	datum = append(datum, int64(3))
	datum = append(datum, int64(4))
	datum = append(datum, int64(5))
	datum = append(datum, int64(6))

	bits := []byte("[-1,-2,-3,-4,-5,-6,0,1,2,3,4,5,6]")
	checkCodecJSONEncoderResult(t, schema, datum, bits)
}

func TestCodecJSONMapChecksSchema(t *testing.T) {
	_, err := NewJSONCodec(`{"type":"map"}`)
	checkErrorFatal(t, err, "ought to have values key")

	_, err = NewJSONCodec(`{"type":"map","values":"flubber"}`)
	checkErrorFatal(t, err, "unknown type name")

	checkCodecJSONEncoderError(t, `{"type":"map","values":"long"}`, int64(5), "expected: map[string]interface{}; received: int64")
	checkCodecJSONEncoderError(t, `{"type":"map","values":"string"}`, 3, "expected: map[string]interface{}; received: int")
}

func TestCodecJSONDecoderMapEOF(t *testing.T) {
	schema := `{"type":"map","values":"string"}`
	checkCodecJSONDecoderError(t, schema, []byte(""), "cannot decode map (map): EOF")
}

func TestCodecJSONDecoderMapZeroBlocks(t *testing.T) {
	codec, err := NewJSONCodec(`{"type":"map","values":"string"}`)
	checkErrorFatal(t, err, nil)

	bb := bytes.NewBuffer([]byte("{}"))
	actual, err := codec.Decode(bb)
	checkErrorFatal(t, err, nil)

	someMap, ok := actual.(map[string]interface{})
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	if len(someMap) != 0 {
		t.Errorf(`received: %v; Expected: %v`, len(someMap), 0)
	}
}

func TestCodecJSONDecoderMapReturnsExpectedMap(t *testing.T) {
	codec, err := NewJSONCodec(`{"type":"map","values":"string"}`)
	checkErrorFatal(t, err, nil)

	bb := bytes.NewBuffer([]byte("{\"foo\":\"BAR\"}"))
	actual, err := codec.Decode(bb)
	checkErrorFatal(t, err, nil)

	someMap, ok := actual.(map[string]interface{})
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	if len(someMap) != 1 {
		t.Errorf(`received: %v; Expected: %v`, len(someMap), 1)
	}
	datum, ok := someMap["foo"]
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	someString, ok := datum.(string)
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	if someString != "BAR" {
		t.Errorf("Actual: %#v; Expected: %#v", someString, "BAR")
	}
}

func TestCodecJSONEncoderMapChecksValueTypeDuringWrite(t *testing.T) {
	schema := `{"type":"map","values":"string"}`
	datum := make(map[string]interface{})
	datum["name"] = 13
	checkCodecJSONEncoderError(t, schema, datum, "expected: string; received: int")
}

func TestCodecJSONEncoderRecord(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"comments","namespace":"com.example","fields":[{"name":"username","type":"string","doc":"Name of user"},{"name":"comment","type":"string","doc":"The content of the user's message"},{"name":"timestamp","type":"long","doc":"Unix epoch time in milliseconds"}],"doc:":"A basic schema for storing blog comments"}`
	someRecord, err := NewRecord(RecordSchema(recordSchemaJSON))
	checkErrorFatal(t, err, nil)

	someRecord.Set("username", "Aquaman")
	someRecord.Set("comment", "The Atlantic is oddly cold this morning!")
	someRecord.Set("timestamp", int64(1082196484))

	bits := []byte("{\"username\":\"Aquaman\",\"comment\":\"The Atlantic is oddly cold this morning!\",\"timestamp\":1082196484}")
	checkCodecJSONEncoderResult(t, recordSchemaJSON, someRecord, bits)
}

func TestCodecJSONEncoderRecordWithFieldDefaultNull(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"Foo","fields":[{"name":"field1","type":"int"},{"name":"field2","type":["null","string"],"default":null}]}`
	someRecord, err := NewRecord(RecordSchema(recordSchemaJSON))
	checkErrorFatal(t, err, nil)

	someRecord.Set("field1", int32(42))
	bits := []byte("{\"field1\":42,\"field2\":null}")
	checkCodecJSONEncoderResult(t, recordSchemaJSON, someRecord, bits)
}

func TestCodecJSONEncoderRecordWithFieldDefaultBoolean(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"Foo","fields":[{"name":"field1","type":"int"},{"name":"field2","type":"boolean","default":true}]}`
	someRecord, err := NewRecord(RecordSchema(recordSchemaJSON))
	checkErrorFatal(t, err, nil)

	someRecord.Set("field1", int32(64))

	bits := []byte("{\"field1\":64,\"field2\":true}")
	checkCodecJSONEncoderResult(t, recordSchemaJSON, someRecord, bits)
}

func TestCodecJSONEncoderRecordWithFieldDefaultInt(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"Foo","fields":[{"name":"field1","type":"int","default":3}]}`
	someRecord, err := NewRecord(RecordSchema(recordSchemaJSON))
	checkErrorFatal(t, err, nil)

	bits := []byte("{\"field1\":3}")
	checkCodecJSONEncoderResult(t, recordSchemaJSON, someRecord, bits)
}

func TestCodecJSONEncoderRecordWithFieldDefaultLong(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"Foo","fields":[{"name":"field1","type":"long","default":3}]}`
	someRecord, err := NewRecord(RecordSchema(recordSchemaJSON))
	checkErrorFatal(t, err, nil)

	bits := []byte("{\"field1\":3}")
	checkCodecJSONEncoderResult(t, recordSchemaJSON, someRecord, bits)
}

func TestCodecJSONEncoderRecordWithFieldDefaultFloat(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"Foo","fields":[{"name":"field1","type":"float","default":3.5}]}`
	someRecord, err := NewRecord(RecordSchema(recordSchemaJSON))
	checkErrorFatal(t, err, nil)

	bits := []byte("{\"field1\":3.5}")
	checkCodecJSONEncoderResult(t, recordSchemaJSON, someRecord, bits)
}

func TestCodecJSONEncoderRecordWithFieldDefaultDouble(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"Foo","fields":[{"name":"field1","type":"double","default":3.5}]}`
	someRecord, err := NewRecord(RecordSchema(recordSchemaJSON))
	checkErrorFatal(t, err, nil)

	bits := []byte("{\"field1\":3.5}")
	checkCodecJSONEncoderResult(t, recordSchemaJSON, someRecord, bits)
}

func TestCodecJSONEncoderRecordWithFieldDefaultBytes(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"Foo","fields":[{"name":"field1","type":"int"},{"name":"field2","type":"bytes","default":"happy"}]}`
	someRecord, err := NewRecord(RecordSchema(recordSchemaJSON))
	checkErrorFatal(t, err, nil)

	someRecord.Set("field1", int32(64))

	bits := []byte("{\"field1\":64,\"field2\":\"happy\"}")
	checkCodecJSONEncoderResult(t, recordSchemaJSON, someRecord, bits)
}

func TestCodecJSONEncoderRecordWithFieldDefaultString(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"Foo","fields":[{"name":"field1","type":"int"},{"name":"field2","type":"string","default":"happy"}]}`
	someRecord, err := NewRecord(RecordSchema(recordSchemaJSON))
	checkErrorFatal(t, err, nil)

	someRecord.Set("field1", int32(64))

	bits := []byte("{\"field1\":64,\"field2\":\"happy\"}")
	checkCodecJSONEncoderResult(t, recordSchemaJSON, someRecord, bits)
}
