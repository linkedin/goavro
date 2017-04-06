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
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"io/ioutil"
	"math"
	"reflect"
	"testing"
)

////////////////////////////////////////
// helpers
////////////////////////////////////////

func checkCodecDecoderError(t *testing.T, schema string, bits []byte, expectedError interface{}) {
	codec, err := NewCodec(schema)
	checkErrorFatal(t, err, nil)
	bb := bytes.NewBuffer(bits)
	_, err = codec.Decode(bb)
	checkError(t, err, expectedError)
}

func checkCodecDecoderResult(t *testing.T, schema string, bits []byte, datum interface{}) {
	codec, err := NewCodec(schema)
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

func checkCodecEncoderError(t *testing.T, schema string, datum interface{}, expectedError interface{}) {
	bb := new(bytes.Buffer)
	codec, err := NewCodec(schema)
	checkErrorFatal(t, err, nil)

	err = codec.Encode(bb, datum)
	checkErrorFatal(t, err, expectedError)
}

func checkCodecEncoderResult(t *testing.T, schema string, datum interface{}, bits []byte) {
	// test against both bytes.Buffer and simpleBuffer
	test := func(t *testing.T, schema string, datum interface{}, bits []byte, wb testBuffer) {
		codec, err := NewCodec(schema)
		checkErrorFatal(t, err, nil)

		err = codec.Encode(wb, datum)
		if err != nil {
			t.Errorf("Actual: %v; Expected: %v", err, nil)
		}
		if !bytes.Equal(wb.Bytes(), bits) {
			t.Errorf("Actual: %#v; Expected: %#v", wb.Bytes(), bits)
		}
	}
	test(t, schema, datum, bits, new(bytes.Buffer))
	test(t, schema, datum, bits, new(simpleBuffer))
}

func checkCodecRoundTrip(t *testing.T, schema string, datum interface{}) {
	// test against both bytes.Buffer and simpleBuffer
	test := func(t *testing.T, schema string, datum interface{}, wb testBuffer) {
		codec, err := NewCodec(schema)
		if err != nil {
			t.Errorf("%v", err)
			return
		}
		err = codec.Encode(wb, datum)
		if err != nil {
			t.Errorf("%v", err)
			return
		}
		actual, err := codec.Decode(wb)
		if err != nil {
			t.Errorf("%v", err)
			return
		}
		actualJSON, err := json.Marshal(actual)
		if err != nil {
			t.Errorf("%v", err)
			return
		}
		expectedJSON, err := json.Marshal(datum)
		if err != nil {
			t.Errorf("%v", err)
			return
		}
		if !bytes.Equal(actualJSON, expectedJSON) {
			t.Errorf("Actual: %q; Expected: %q", actualJSON, expectedJSON)
		}
	}
	test(t, schema, datum, new(bytes.Buffer))
	test(t, schema, datum, new(simpleBuffer))
}

func checkCodecRoundTripLong(t *testing.T, number int64) {
	schema := `"long"`
	checkCodecRoundTrip(t, schema, number)

	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	// 1. encode number using goavro, then ensure result decodes using binary.Varint
	{
		buf := new(bytes.Buffer)

		err = codec.Encode(buf, number)
		if err != nil {
			t.Fatal(err)
		}
		decoded, c := binary.Varint(buf.Bytes())
		if c != buf.Len() {
			t.Errorf("Actual: %#v; Expected: %#v", c, buf.Len())
		}
		if number != decoded {
			t.Errorf("Actual: %#v; Expected: %#v", decoded, number)
		}
	}

	// 2. encode number using varint, then ensure result decodes using goavro
	{
		buf := make([]byte, 100)
		_ = binary.PutVarint(buf, number) // ignore count here because we compare both byte slices below

		decoded, err := codec.Decode(bytes.NewReader(buf))
		if err != nil {
			t.Fatal(err)
		}

		if decoded != number {
			t.Errorf("Actual: %#v; Expected: %#v", decoded, number)
		}
	}
}

////////////////////////////////////////

func TestCodecRoundTrip(t *testing.T) {
	// null
	checkCodecRoundTrip(t, `"null"`, nil)
	checkCodecRoundTrip(t, `{"type":"null"}`, nil)
	// boolean
	checkCodecRoundTrip(t, `"boolean"`, false)
	checkCodecRoundTrip(t, `"boolean"`, true)
	// int
	checkCodecRoundTrip(t, `"int"`, int32(-3))
	checkCodecRoundTrip(t, `"int"`, int32(-65))
	checkCodecRoundTrip(t, `"int"`, int32(0))
	checkCodecRoundTrip(t, `"int"`, int32(1016))
	checkCodecRoundTrip(t, `"int"`, int32(3))
	checkCodecRoundTrip(t, `"int"`, int32(42))
	checkCodecRoundTrip(t, `"int"`, int32(64))
	checkCodecRoundTrip(t, `"int"`, int32(66052))
	checkCodecRoundTrip(t, `"int"`, int32(8454660))
	checkCodecRoundTrip(t, `"int"`, int32(2147483647))
	checkCodecRoundTrip(t, `"int"`, int32(-2147483647))
	checkCodecRoundTrip(t, `"int"`, int32(1455301406))
	// long
	checkCodecRoundTripLong(t, int64(-2147483648))
	checkCodecRoundTripLong(t, int64(-3))
	checkCodecRoundTripLong(t, int64(-65))
	checkCodecRoundTripLong(t, int64(0))
	checkCodecRoundTripLong(t, int64(1082196484))
	checkCodecRoundTripLong(t, int64(138521149956))
	checkCodecRoundTripLong(t, int64(17730707194372))
	checkCodecRoundTripLong(t, int64(2147483647))
	checkCodecRoundTripLong(t, int64(2269530520879620))
	checkCodecRoundTripLong(t, int64(3))
	checkCodecRoundTripLong(t, int64(64))

	checkCodecRoundTripLong(t, int64(-(1 << 63)))
	checkCodecRoundTripLong(t, int64((1<<63)-1))
	checkCodecRoundTripLong(t, int64(5959107741628848600))
	checkCodecRoundTripLong(t, int64(1359702038045356208))
	checkCodecRoundTripLong(t, int64(-5513458701470791632)) // https://github.com/linkedin/goavro/issues/49

	// float
	checkCodecRoundTrip(t, `"float"`, float32(3.5))
	// checkCodecRoundTrip(t, `"float"`, float32(math.Inf(-1)))
	// checkCodecRoundTrip(t, `"float"`, float32(math.Inf(1)))
	// checkCodecRoundTrip(t, `"float"`, float32(math.NaN()))
	// double
	checkCodecRoundTrip(t, `"double"`, float64(3.5))
	// checkCodecRoundTrip(t, `"double"`, float64(math.Inf(-1)))
	// checkCodecRoundTrip(t, `"double"`, float64(math.Inf(1)))
	// checkCodecRoundTrip(t, `"double"`, float64(math.NaN()))
	// bytes
	checkCodecRoundTrip(t, `"bytes"`, []byte(""))
	checkCodecRoundTrip(t, `"bytes"`, []byte("some bytes"))
	// string
	checkCodecRoundTrip(t, `"string"`, "")
	checkCodecRoundTrip(t, `"string"`, "filibuster")
}

func TestCodecDecoderPrimitives(t *testing.T) {
	// null
	checkCodecDecoderResult(t, `"null"`, []byte("\x01"), nil)
	// boolean
	checkCodecDecoderError(t, `"boolean"`, []byte("\x02"), "cannot decode boolean")
	checkCodecDecoderError(t, `"boolean"`, []byte(""), "cannot decode boolean: EOF")
	checkCodecDecoderResult(t, `"boolean"`, []byte("\x00"), false)
	checkCodecDecoderResult(t, `"boolean"`, []byte("\x01"), true)
	// int
	checkCodecDecoderError(t, `"int"`, []byte(""), "cannot decode int: EOF")
	checkCodecDecoderResult(t, `"int"`, []byte("\x00"), int32(0))
	checkCodecDecoderResult(t, `"int"`, []byte("\x05"), int32(-3))
	checkCodecDecoderResult(t, `"int"`, []byte("\x06"), int32(3))
	checkCodecDecoderResult(t, `"int"`, []byte("\x80\x01"), int32(64))
	checkCodecDecoderResult(t, `"int"`, []byte("\x81\x01"), int32(-65))
	checkCodecDecoderResult(t, `"int"`, []byte("\xf0\x0f"), int32(1016))
	checkCodecDecoderResult(t, `"int"`, []byte("\x88\x88\x08"), int32(66052))
	checkCodecDecoderResult(t, `"int"`, []byte("\x88\x88\x88\x08"), int32(8454660))
	// long
	checkCodecDecoderError(t, `"long"`, []byte(""), "cannot decode long: EOF")
	checkCodecDecoderResult(t, `"long"`, []byte("\x00"), int64(0))
	checkCodecDecoderResult(t, `"long"`, []byte("\x05"), int64(-3))
	checkCodecDecoderResult(t, `"long"`, []byte("\x06"), int64(3))
	checkCodecDecoderResult(t, `"long"`, []byte("\x80\x01"), int64(64))
	checkCodecDecoderResult(t, `"long"`, []byte("\x81\x01"), int64(-65))
	checkCodecDecoderResult(t, `"long"`, []byte("\xfe\xff\xff\xff\x0f"), int64(2147483647))
	checkCodecDecoderResult(t, `"long"`, []byte("\xff\xff\xff\xff\x0f"), int64(-2147483648))
	checkCodecDecoderResult(t, `"long"`, []byte("\x88\x88\x88\x88\x08"), int64(1082196484))
	checkCodecDecoderResult(t, `"long"`, []byte("\x88\x88\x88\x88\x88\x08"), int64(138521149956))
	checkCodecDecoderResult(t, `"long"`, []byte("\x88\x88\x88\x88\x88\x88\x08"), int64(17730707194372))
	checkCodecDecoderResult(t, `"long"`, []byte("\x88\x88\x88\x88\x88\x88\x88\x08"), int64(2269530520879620))
	checkCodecDecoderResult(t, `"long"`, []byte("\x9f\xdf\x9f\x8f\xc7\xde\xde\x83\x99\x01"), int64(-5513458701470791632)) // https://github.com/linkedin/goavro/issues/49
	// float
	checkCodecDecoderError(t, `"float"`, []byte(""), "cannot decode float: EOF")
	checkCodecDecoderResult(t, `"float"`, []byte("\x00\x00\x60\x40"), float32(3.5))
	checkCodecDecoderResult(t, `"float"`, []byte("\x00\x00\x80\u007f"), float32(math.Inf(1)))
	checkCodecDecoderResult(t, `"float"`, []byte("\x00\x00\x80\xff"), float32(math.Inf(-1)))
	// double
	checkCodecDecoderError(t, `"double"`, []byte(""), "cannot decode double: EOF")
	checkCodecDecoderResult(t, `"double"`, []byte("\x00\x00\x00\x00\x00\x00\f@"), float64(3.5))
	checkCodecDecoderResult(t, `"double"`, []byte("\x00\x00\x00\x00\x00\x00\xf0\u007f"), float64(math.Inf(1)))
	checkCodecDecoderResult(t, `"double"`, []byte("\x00\x00\x00\x00\x00\x00\xf0\xff"), float64(math.Inf(-1)))
	// bytes
	checkCodecDecoderError(t, `"bytes"`, []byte(""), "cannot decode bytes: cannot decode long: EOF")
	checkCodecDecoderError(t, `"bytes"`, []byte("\x01"), "cannot decode bytes: negative length: -1")
	checkCodecDecoderError(t, `"bytes"`, []byte("\x02"), "cannot decode bytes: EOF")
	checkCodecDecoderResult(t, `"bytes"`, []byte("\x00"), []byte(""))
	checkCodecDecoderResult(t, `"bytes"`, []byte("\x14some bytes"), []byte("some bytes"))
	// string
	checkCodecDecoderError(t, `"string"`, []byte(""), "cannot decode string: cannot decode long: EOF")
	checkCodecDecoderError(t, `"string"`, []byte("\x01"), "cannot decode string: negative length: -1")
	checkCodecDecoderError(t, `"string"`, []byte("\x02"), "cannot decode string: EOF")
	checkCodecDecoderResult(t, `"string"`, []byte("\x00"), "")
	checkCodecDecoderResult(t, `"string"`, []byte("\x16some string"), "some string")
}

func TestCodecDecoderFloatNaN(t *testing.T) {
	codec, err := NewCodec(`"float"`)
	checkErrorFatal(t, err, nil)

	// NOTE: NaN never equals NaN (math is fun)
	bits := []byte("\x00\x00\xc0\u007f")
	bb := bytes.NewBuffer(bits)
	actual, err := codec.Decode(bb)
	checkErrorFatal(t, err, nil)

	someFloat, ok := actual.(float32)
	if !ok {
		t.Fatalf("Actual: %#v; Expected: %#v", ok, true)
	}
	if !math.IsNaN(float64(someFloat)) {
		expected := math.NaN()
		t.Errorf("Actual: %T(%#v); Expected: %T(%#v)", actual, actual, expected, expected)
	}
}

func TestCodecDecoderDoubleNaN(t *testing.T) {
	codec, err := NewCodec(`"double"`)
	checkErrorFatal(t, err, nil)

	// NOTE: NaN never equals NaN (math is fun)
	bits := []byte("\x01\x00\x00\x00\x00\x00\xf8\u007f")
	bb := bytes.NewBuffer(bits)
	actual, err := codec.Decode(bb)
	checkErrorFatal(t, err, nil)

	someFloat, ok := actual.(float64)
	if !ok {
		t.Fatalf("Actual: %#v; Expected: %#v", ok, true)
	}
	if !math.IsNaN(float64(someFloat)) {
		expected := math.NaN()
		t.Errorf("Actual: %T(%#v); Expected: %T(%#v)", actual, actual, expected, expected)
	}
}

func TestCodecEncoderPrimitives(t *testing.T) {
	// null
	checkCodecEncoderResult(t, `"null"`, nil, []byte(""))
	checkCodecEncoderResult(t, `{"type":"null"}`, nil, []byte(""))
	// boolean
	checkCodecEncoderResult(t, `"boolean"`, false, []byte("\x00"))
	checkCodecEncoderResult(t, `"boolean"`, true, []byte("\x01"))
	// int
	checkCodecEncoderResult(t, `"int"`, int32(-53), []byte("\x69"))
	checkCodecEncoderResult(t, `"int"`, int32(-33), []byte("\x41"))
	checkCodecEncoderResult(t, `"int"`, int32(-3), []byte("\x05"))
	checkCodecEncoderResult(t, `"int"`, int32(-65), []byte("\x81\x01"))
	checkCodecEncoderResult(t, `"int"`, int32(0), []byte("\x00"))
	checkCodecEncoderResult(t, `"int"`, int32(1016), []byte("\xf0\x0f"))
	checkCodecEncoderResult(t, `"int"`, int32(3), []byte("\x06"))
	checkCodecEncoderResult(t, `"int"`, int32(42), []byte("\x54"))
	checkCodecEncoderResult(t, `"int"`, int32(64), []byte("\x80\x01"))
	checkCodecEncoderResult(t, `"int"`, int32(66052), []byte("\x88\x88\x08"))
	checkCodecEncoderResult(t, `"int"`, int32(8454660), []byte("\x88\x88\x88\x08"))
	checkCodecEncoderResult(t, `"int"`, int32(2147483647), []byte("\xfe\xff\xff\xff\x0f"))
	checkCodecEncoderResult(t, `"int"`, int32(-2147483647), []byte("\xfd\xff\xff\xff\x0f"))
	// long
	checkCodecEncoderResult(t, `"long"`, int64(-2147483648), []byte("\xff\xff\xff\xff\x0f"))
	checkCodecEncoderResult(t, `"long"`, int64(-3), []byte("\x05"))
	checkCodecEncoderResult(t, `"long"`, int64(-65), []byte("\x81\x01"))
	checkCodecEncoderResult(t, `"long"`, int64(0), []byte("\x00"))
	checkCodecEncoderResult(t, `"long"`, int64(1082196484), []byte("\x88\x88\x88\x88\x08"))
	checkCodecEncoderResult(t, `"long"`, int64(138521149956), []byte("\x88\x88\x88\x88\x88\x08"))
	checkCodecEncoderResult(t, `"long"`, int64(17730707194372), []byte("\x88\x88\x88\x88\x88\x88\x08"))
	checkCodecEncoderResult(t, `"long"`, int64(2147483647), []byte("\xfe\xff\xff\xff\x0f"))
	checkCodecEncoderResult(t, `"long"`, int64(2269530520879620), []byte("\x88\x88\x88\x88\x88\x88\x88\x08"))
	checkCodecEncoderResult(t, `"long"`, int64(3), []byte("\x06"))
	checkCodecEncoderResult(t, `"long"`, int64(64), []byte("\x80\x01"))
	checkCodecEncoderResult(t, `"long"`, int64(-5513458701470791632), []byte("\x9f\xdf\x9f\x8f\xc7\xde\xde\x83\x99\x01")) // https://github.com/linkedin/goavro/issues/49
	// float
	checkCodecEncoderResult(t, `"float"`, float32(3.5), []byte("\x00\x00\x60\x40"))
	checkCodecEncoderResult(t, `"float"`, float32(math.Inf(-1)), []byte("\x00\x00\x80\xff"))
	checkCodecEncoderResult(t, `"float"`, float32(math.Inf(1)), []byte("\x00\x00\x80\u007f"))
	checkCodecEncoderResult(t, `"float"`, float32(math.NaN()), []byte("\x00\x00\xc0\u007f"))
	// double
	checkCodecEncoderResult(t, `"double"`, float64(3.5), []byte("\x00\x00\x00\x00\x00\x00\f@"))
	checkCodecEncoderResult(t, `"double"`, float64(math.Inf(-1)), []byte("\x00\x00\x00\x00\x00\x00\xf0\xff"))
	checkCodecEncoderResult(t, `"double"`, float64(math.Inf(1)), []byte("\x00\x00\x00\x00\x00\x00\xf0\u007f"))
	checkCodecEncoderResult(t, `"double"`, float64(math.NaN()), []byte("\x01\x00\x00\x00\x00\x00\xf8\u007f"))
	// bytes
	checkCodecEncoderResult(t, `"bytes"`, []byte(""), []byte("\x00"))
	checkCodecEncoderResult(t, `"bytes"`, []byte("some bytes"), []byte("\x14some bytes"))
	// string
	checkCodecEncoderResult(t, `"string"`, "", []byte("\x00"))
	checkCodecEncoderResult(t, `"string"`, "filibuster", []byte("\x14filibuster"))
}

func TestCodecUnionChecksSchema(t *testing.T) {
	var err error
	_, err = NewCodec(`[]`)
	checkErrorFatal(t, err, "ought have at least one member")
	_, err = NewCodec(`["null","flubber"]`)
	checkErrorFatal(t, err, "member ought to be decodable")
}

func TestCodecUnionPrimitives(t *testing.T) {
	// null
	checkCodecEncoderResult(t, `["null"]`, nil, []byte("\x00"))
	checkCodecEncoderResult(t, `[{"type":"null"}]`, nil, []byte("\x00"))
	// boolean
	checkCodecEncoderResult(t, `["null","boolean"]`, nil, []byte("\x00"))
	checkCodecEncoderResult(t, `["null","boolean"]`, false, []byte("\x02\x00"))
	checkCodecEncoderResult(t, `["null","boolean"]`, true, []byte("\x02\x01"))
	checkCodecEncoderResult(t, `["boolean","null"]`, true, []byte("\x00\x01"))
	// int
	checkCodecEncoderResult(t, `["null","int"]`, nil, []byte("\x00"))
	checkCodecEncoderResult(t, `["int","null"]`, nil, []byte("\x02"))
	checkCodecEncoderResult(t, `{"default": 1016, "name": "testField", "type": ["null", "int"]}`, int32(1016), []byte("\x02\xf0\x0f"))
	checkCodecEncoderResult(t, `{"default": 1016, "name": "testField", "type": ["null", "int"]}`, nil, []byte("\x00"))
	checkCodecEncoderResult(t, `{"default": 1016, "name": "testField", "type": ["int", "null"]}`, int32(1016), []byte("\x00\xf0\x0f"))
	checkCodecEncoderResult(t, `{"default": 1016, "name": "testField", "type": ["int", "null"]}`, nil, []byte("\x02"))
	checkCodecEncoderResult(t, `["boolean","int"]`, true, []byte("\x00\x01"))
	checkCodecEncoderResult(t, `["boolean","int"]`, int32(3), []byte("\x02\x06"))
	checkCodecEncoderResult(t, `["int",{"type":"boolean"}]`, int32(42), []byte("\x00\x54"))
	// long
	checkCodecEncoderResult(t, `["boolean","long"]`, int64(3), []byte("\x02\x06"))
	// float
	checkCodecEncoderResult(t, `["int","float"]`, float32(3.5), []byte("\x02\x00\x00\x60\x40"))
	// double
	checkCodecEncoderResult(t, `["float","double"]`, float64(3.5), []byte("\x02\x00\x00\x00\x00\x00\x00\f@"))
	// bytes
	checkCodecEncoderResult(t, `["int","bytes"]`, []byte("foobar"), []byte("\x02\x0cfoobar"))
	// string
	checkCodecEncoderResult(t, `["string","float"]`, "filibuster", []byte("\x00\x14filibuster"))
	checkCodecEncoderResult(t, `["string","null"]`, "filibuster", []byte("\x00\x14filibuster"))
	checkCodecEncoderResult(t, `["string","null"]`, nil, []byte("\x02"))
}

func TestCodecDecoderUnion(t *testing.T) {
	checkCodecDecoderResult(t, `["string","float"]`, []byte("\x00\x14filibuster"), "filibuster")
	checkCodecDecoderResult(t, `["string","int"]`, []byte("\x02\x1a"), int32(13))
}

func TestCodecEncoderUnionArray(t *testing.T) {
	checkCodecEncoderResult(t, `[{"type":"array","items":"int"},"string"]`, "filibuster", []byte("\x02\x14filibuster"))

	var someArray []interface{}
	someArray = append(someArray, int32(3))
	someArray = append(someArray, int32(13))
	checkCodecEncoderResult(t, `[{"type":"array","items":"int"},"string"]`, someArray, []byte("\x00\x04\x06\x1a\x00"))
}

func TestCodecEncoderUnionEnum(t *testing.T) {
	checkCodecEncoderResult(t, `["null",{"type":"enum","name":"color_enum","symbols":["red","blue","green"]}]`, nil, []byte("\x00"))
	checkCodecEncoderResult(t, `["null",{"type":"enum","name":"color_enum","symbols":["red","blue","green"]}]`, Enum{"color_enum", "blue"}, []byte("\x02\x02"))
	checkCodecEncoderError(t, `["null",{"type":"enum","name":"color_enum","symbols":["red","blue","green"]}]`, Enum{"color_enum", "purple"}, "symbol not defined: purple")
}

func TestCodecEncoderUnionMap(t *testing.T) {
	someMap := make(map[string]interface{})
	someMap["superhero"] = "Batman"
	checkCodecEncoderResult(t, `["null",{"type":"map","values":"string"}]`, someMap, []byte("\x02\x02\x12superhero\x0cBatman\x00"))
	checkCodecRoundTrip(t, `["null",{"type":"map","values":"string"}]`, someMap)
}

func TestCodecEncoderUnionEmptyMap(t *testing.T) {
	someMap := make(map[string]interface{})
	checkCodecEncoderResult(t, `["null",{"type":"map","values":"double"}]`, someMap, []byte("\x02\x00"))
	checkCodecRoundTrip(t, `["null",{"type":"map","values":"double"}]`, someMap)
}

func TestCodecDecoderUnionErrorYieldsName(t *testing.T) {
	schema := `
{
  "default": null,
  "doc": "some union schema with a name",
  "name": "flubber",
  "type": [
    "null",
    "string"
  ]
}
`
	bits := []byte("\x04")
	checkCodecDecoderError(t, schema, bits, "cannot encode union (union): index must be between 0 and 1; read index: 2")
}

func TestCodecEncoderUnionRecord(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"record1","fields":[{"type":"int","name":"field1"},{"type":"string","name":"field2"}]}`

	someRecord, err := NewRecord(RecordSchema(recordSchemaJSON))
	checkErrorFatal(t, err, nil)

	someRecord.Set("field1", int32(13))
	someRecord.Set("field2", "Superman")

	bits := []byte("\x02\x1a\x10Superman")
	checkCodecEncoderResult(t, `["null",`+recordSchemaJSON+`]`, someRecord, bits)
}

func TestCodecEncoderEnumChecksSchema(t *testing.T) {
	var err error

	_, err = NewCodec(`{"type":"enum"}`)
	checkError(t, err, "ought to have name key")

	_, err = NewCodec(`{"type":"enum","name":5}`)
	checkError(t, err, "name ought to be non-empty string")

	_, err = NewCodec(`{"type":"enum","name":"enum1"}`)
	checkError(t, err, "ought to have symbols key")

	_, err = NewCodec(`{"type":"enum","name":"enum1","symbols":5}`)
	checkError(t, err, "symbols ought to be non-empty array")

	_, err = NewCodec(`{"type":"enum","name":"enum1","symbols":[]}`)
	checkError(t, err, "symbols ought to be non-empty array")

	_, err = NewCodec(`{"type":"enum","name":"enum1","symbols":[5]}`)
	checkError(t, err, "symbols array member ought to be string")
}

func TestCodecDecoderEnum(t *testing.T) {
	schema := `{"type":"enum","name":"cards","symbols":["HEARTS","DIAMONDS","SPADES","CLUBS"]}`
	checkCodecDecoderError(t, schema, []byte("\x01"), "index must be between 0 and 3")
	checkCodecDecoderError(t, schema, []byte("\x08"), "index must be between 0 and 3")
	checkCodecDecoderResult(t, schema, []byte("\x04"), Enum{"cards", "SPADES"})
}

func TestCodecEncoderEnum(t *testing.T) {
	t.Skip("TODO")
	schema := `{"type":"enum","name":"cards","symbols":["HEARTS","DIAMONDS","SPADES","CLUBS"]}`
	checkCodecEncoderResult(t, schema, Enum{"cards", "SPADES"}, []byte("\x04"))
	checkCodecEncoderError(t, schema, Enum{"cards", "PINEAPPLE"}, "symbol not defined")
	checkCodecEncoderError(t, schema, []byte("\x01"), "expected: Enum or string; received: []uint8")
	checkCodecEncoderError(t, schema, "some symbol not in schema", "symbol not defined")
}

func TestCodecFixedChecksSchema(t *testing.T) {
	var err error

	_, err = NewCodec(`{"type":"fixed","size":5}`)
	checkError(t, err, "ought to have name key")

	_, err = NewCodec(`{"type":"fixed","name":5,"size":5}`)
	checkError(t, err, "name ought to be non-empty string")

	_, err = NewCodec(`{"type":"fixed","name":"fixed1"}`)
	checkError(t, err, "ought to have size key")

	_, err = NewCodec(`{"type":"fixed","name":"fixed1","size":"5"}`)
	checkError(t, err, "size ought to be number")
}

func TestCodecFixed(t *testing.T) {
	schema := `{"type":"fixed","name":"fixed1","size":5}`
	checkCodecDecoderError(t, schema, []byte(""), "EOF")
	checkCodecDecoderError(t, schema, []byte("hap"), "buffer underrun")
	checkCodecEncoderError(t, schema, "happy day", "expected: Fixed; received: string")
	checkCodecEncoderError(t, schema, Fixed{Name: "fixed1", Value: []byte("day")}, "expected: 5 bytes; received: 3")
	checkCodecEncoderError(t, schema, Fixed{Name: "fixed1", Value: []byte("happy day")}, "expected: 5 bytes; received: 9")
	checkCodecEncoderResult(t, schema, Fixed{Name: "fixed1", Value: []byte("happy")}, []byte("happy"))
}

func TestCodecFixedDecoder(t *testing.T) {
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

func TestCodecNamedTypesCheckSchema(t *testing.T) {
	schema := `{"name":"guid","type":{"type":"fixed","name":"fixed_16","size":16},"doc":"event unique id"}`
	var err error
	_, err = NewCodec(schema)
	checkError(t, err, nil)
}

func TestCodecNamedTypes(t *testing.T) {
	schema := `{"name":"guid","type":["null",{"type":"fixed","name":"fixed_16","size":16}],"doc":"event unique id"}`
	// The 0x2 byte is an avro encoded int(1), which refers to the index of the
	// `fixed_16` type in the schema's union array.
	checkCodecEncoderResult(t, schema, Fixed{Name: "fixed_16", Value: []byte("0123456789abcdef")}, append([]byte{0x2}, []byte("0123456789abcdef")...))
}

func TestCodecReferToNamedTypes(t *testing.T) {
	schema := `{"type":"record","name":"record1","fields":[{"name":"guid","type":{"type":"fixed","name":"fixed_16","size":16},"doc":"event unique id"},{"name":"treeId","type":"fixed_16","doc":"call tree uuid"}]}`
	_, err := NewCodec(schema)
	checkError(t, err, nil)
}

func TestCodecRecordFieldDefaultValueNamedType(t *testing.T) {
	schemaJSON := `{"type":"record","name":"record1","fields":[{"type":"fixed","name":"fixed_16","size":16},{"type":"fixed_16","name":"another","default":3}]}`
	_, err := NewCodec(schemaJSON)
	checkError(t, err, nil)
}

func TestCodecRecordFieldChecksDefaultType(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"record1","fields":[{"type":"int","name":"field1","default":true},{"type":"string","name":"field2"}]}`
	_, err := NewCodec(recordSchemaJSON)
	checkError(t, err, "expected: int32; received: bool")
}

func TestCodecEncoderArrayChecksSchema(t *testing.T) {
	_, err := NewCodec(`{"type":"array"}`)
	checkErrorFatal(t, err, "ought to have items key")

	_, err = NewCodec(`{"type":"array","items":"flubber"}`)
	checkErrorFatal(t, err, "unknown type name")

	checkCodecEncoderError(t, `{"type":"array","items":"long"}`, int64(5), "expected: []interface{}; received: int64")
}

func TestCodecDecoderArrayEOF(t *testing.T) {
	schema := `{"type":"array","items":"string"}`
	checkCodecDecoderError(t, schema, []byte(""), "cannot decode long: EOF")
}

func TestCodecDecoderArrayEmpty(t *testing.T) {
	schema := `{"type":"array","items":"string"}`
	codec, err := NewCodec(schema)
	checkErrorFatal(t, err, nil)

	bb := bytes.NewBuffer([]byte{0})
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

func TestCodecDecoderArray(t *testing.T) {
	schema := `{"type":"array","items":"int"}`
	codec, err := NewCodec(schema)
	checkErrorFatal(t, err, nil)

	bb := bytes.NewBuffer([]byte("\x04\x06\x36\x00"))
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

func TestCodecDecoderArrayOfRecords(t *testing.T) {
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
	codec, err := NewCodec(schema)
	checkErrorFatal(t, err, nil)

	encoded := []byte("\x04\x0aHello\x1a\x0aWorld\x54\x00")
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

func TestCodecDecoderArrayMultipleBlocks(t *testing.T) {
	schema := `{"type":"array","items":"int"}`
	codec, err := NewCodec(schema)
	checkErrorFatal(t, err, nil)

	bb := bytes.NewBuffer([]byte("\x06\x06\x08\x0a\x03\x04\x36\x0c\x00"))
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

func TestCodecEncoderArray(t *testing.T) {
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

	bits := []byte{
		20,
		1, 3, 5, 7, 9, 11, 0, 2, 4, 6,
		6,
		8, 10, 12,
		0,
	}

	checkCodecEncoderResult(t, schema, datum, bits)
}

func TestCodecMapChecksSchema(t *testing.T) {
	_, err := NewCodec(`{"type":"map"}`)
	checkErrorFatal(t, err, "ought to have values key")

	_, err = NewCodec(`{"type":"map","values":"flubber"}`)
	checkErrorFatal(t, err, "unknown type name")

	checkCodecEncoderError(t, `{"type":"map","values":"long"}`, int64(5), "expected: map[string]interface{}; received: int64")
	checkCodecEncoderError(t, `{"type":"map","values":"string"}`, 3, "expected: map[string]interface{}; received: int")
}

func TestCodecDecoderMapEOF(t *testing.T) {
	schema := `{"type":"map","values":"string"}`
	checkCodecDecoderError(t, schema, []byte(""), "cannot decode long: EOF")
}

func TestCodecDecoderMapZeroBlocks(t *testing.T) {
	codec, err := NewCodec(`{"type":"map","values":"string"}`)
	checkErrorFatal(t, err, nil)

	bb := bytes.NewBuffer([]byte("\x00"))
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

func TestCodecDecoderMapReturnsExpectedMap(t *testing.T) {
	codec, err := NewCodec(`{"type":"map","values":"string"}`)
	checkErrorFatal(t, err, nil)

	bb := bytes.NewBuffer([]byte("\x01\x04\x06\x66\x6f\x6f\x06\x42\x41\x52\x00"))
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

func TestCodecEncoderMapChecksValueTypeDuringWrite(t *testing.T) {
	schema := `{"type":"map","values":"string"}`
	datum := make(map[string]interface{})
	datum["name"] = 13
	checkCodecEncoderError(t, schema, datum, "expected: string; received: int")
}

func TestCodecEncoderMapMetadataSchema(t *testing.T) {
	md := make(map[string]interface{})
	md["avro.codec"] = []byte("null")
	md["avro.schema"] = []byte(`"int"`)

	// NOTE: because key value pair ordering is indeterminate,
	// there are two valid possibilities for the encoded map:
	option1 := []byte("\x04\x14avro.codec\x08null\x16avro.schema\x0a\x22int\x22\x00")
	option2 := []byte("\x04\x16avro.schema\x0a\x22int\x22\x14avro.codec\x08null\x00")

	bb := new(bytes.Buffer)
	err := metadataCodec.Encode(bb, md)
	checkErrorFatal(t, err, nil)
	actual := bb.Bytes()
	if (bytes.Compare(actual, option1) != 0) && (bytes.Compare(actual, option2) != 0) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, option1)
	}
}

func TestCodecRecordChecksSchema(t *testing.T) {
	var err error

	_, err = NewCodec(`{"type":"record","fields":[{"name":"age","type":"int"},{"name":"status","type":"string"}]}`)
	checkError(t, err, "ought to have name key")

	_, err = NewCodec(`{"type":"record","name":5,"fields":[{"name":"age","type":"int"},{"name":"status","type":"string"}]}`)
	checkError(t, err, "name ought to be non-empty string")

	_, err = NewCodec(`{"type":"record","name":"Foo"}`)
	checkError(t, err, "record requires one or more fields")

	_, err = NewCodec(`{"type":"record","name":"Foo","fields":5}`)
	checkError(t, err, "fields ought to be non-empty array")

	_, err = NewCodec(`{"type":"record","name":"Foo","fields":[]}`)
	checkError(t, err, "fields ought to be non-empty array")

	_, err = NewCodec(`{"type":"record","name":"Foo","fields":["foo"]}`)
	checkError(t, err, "schema expected")

	_, err = NewCodec(`{"type":"record","name":"Foo","fields":[{"type":"int"}]}`)
	checkError(t, err, "ought to have name key")

	_, err = NewCodec(`{"type":"record","name":"Foo","fields":[{"name":"field1","type":5}]}`)
	checkError(t, err, "type ought to be")

	_, err = NewCodec(`{"type":"record","name":"Foo","fields":[{"type":"int"}]}`)
	checkError(t, err, "ought to have name key")

	_, err = NewCodec(`{"type":"record","name":"Foo","fields":[{"type":"int","name":5}]}`)
	checkError(t, err, "name ought to be non-empty string")
}

func TestCodecDecoderRecord(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"Foo","fields":[{"name":"age","type":"int"},{"name":"status","type":"string"}]}`

	codec, err := NewCodec(recordSchemaJSON)
	checkErrorFatal(t, err, nil)

	bits := []byte("\x80\x01\x0ahappy")
	bb := bytes.NewBuffer(bits)

	actual, err := codec.Decode(bb)
	checkErrorFatal(t, err, nil)

	decoded, ok := actual.(*Record)
	if !ok {
		t.Fatalf("Actual: %T; Expected: Record", actual)
	}

	if decoded.Name != "Foo" {
		t.Errorf("Actual: %#v; Expected: %#v", decoded.Name, "Foo")
	}
	if decoded.Fields[0].Datum != int32(64) {
		t.Errorf("Actual: %#v; Expected: %#v", decoded.Fields[0].Datum, int32(64))
	}
	if decoded.Fields[1].Datum != "happy" {
		t.Errorf("Actual: %#v; Expected: %#v", decoded.Fields[1].Datum, "happy")
	}
}

func TestCodecEncoderRecord(t *testing.T) {
	recordSchemaJSON := `{"type":"record","name":"comments","namespace":"com.example","fields":[{"name":"username","type":"string","doc":"Name of user"},{"name":"comment","type":"string","doc":"The content of the user's message"},{"name":"timestamp","type":"long","doc":"Unix epoch time in milliseconds"}],"doc:":"A basic schema for storing blog comments"}`
	someRecord, err := NewRecord(RecordSchema(recordSchemaJSON))
	checkErrorFatal(t, err, nil)

	someRecord.Set("username", "Aquaman")
	someRecord.Set("comment", "The Atlantic is oddly cold this morning!")
	someRecord.Set("timestamp", int64(1082196484))

	bits := []byte("\x0eAquamanPThe Atlantic is oddly cold this morning!\x88\x88\x88\x88\x08")
	checkCodecEncoderResult(t, recordSchemaJSON, someRecord, bits)
}

////////////////////////////////////////

func TestBufferedEncoder(t *testing.T) {
	bits, err := bufferedEncoder(`"string"`, "filibuster")
	if err != nil {
		t.Fatal(err)
	}
	expected := []byte("\x14filibuster")
	if bytes.Compare(bits, expected) != 0 {
		t.Errorf("Actual: %#v; Expected: %#v", bits, expected)
	}
}

func bufferedEncoder(someSchemaJSON string, datum interface{}) (bits []byte, err error) {
	bb := new(bytes.Buffer)
	defer func() {
		bits = bb.Bytes()
	}()

	var codec Codec
	codec, err = NewCodec(someSchemaJSON)
	if err != nil {
		return
	}
	err = encodeWithBufferedWriter(codec, bb, datum)
	return
}

func encodeWithBufferedWriter(c Codec, w io.Writer, datum interface{}) error {
	bw := bufio.NewWriter(w)
	err := c.Encode(bw, datum)
	if err != nil {
		return err
	}
	return bw.Flush()
}

////////////////////////////////////////

type slowStartReader struct {
	buf []byte
	max int
}

func newSlowStartReader(ior io.Reader) *slowStartReader {
	ssr := &slowStartReader{max: 1}
	ssr.buf, _ = ioutil.ReadAll(ior)
	return ssr
}

func (ssr *slowStartReader) Read(buf []byte) (int, error) {
	size := ssr.max
	if size > len(buf) {
		size = len(buf)
	}
	n := copy(buf[:size], ssr.buf)
	ssr.buf = ssr.buf[n:]
	ssr.max++
	return n, nil
}

func TestDecoderBytesPartialReads(t *testing.T) {
	text := []byte("1234567890abcdef")

	bb := bytes.NewBuffer([]byte{})
	if err := longEncoder(bb, int64(16)); err != nil {
		t.Fatalf("Actual: %#v; Expected: %#v", err, nil)
	}
	n, err := bb.Write([]byte(text))
	if want := 16; n < want {
		t.Fatalf("Actual: %#v; Expected: %#v", n, want)
	}
	if err != nil {
		t.Fatalf("Actual: %#v; Expected: %#v", err, nil)
	}

	result, err := bytesDecoder(newSlowStartReader(bb))
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	if equal := bytes.Equal(text, result.([]byte)); !equal {
		t.Errorf("Actual: %#v; Expected: %#v", equal, true)
	}
}

func TestDecoderStringPartialReads(t *testing.T) {
	text := "1234567890abcdef"

	bb := bytes.NewBufferString("")
	if err := longEncoder(bb, int64(16)); err != nil {
		t.Fatalf("Actual: %#v; Expected: %#v", err, nil)
	}
	n, err := bb.Write([]byte(text))
	if want := 16; n < want {
		t.Fatalf("Actual: %#v; Expected: %#v", n, want)
	}
	if err != nil {
		t.Fatalf("Actual: %#v; Expected: %#v", err, nil)
	}

	result, err := stringDecoder(newSlowStartReader(bb))
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	if want := text; want != result {
		t.Errorf("Actual: %#v; Expected: %#v", result, want)
	}
}
