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
	"fmt"
	"math/big"
	"testing"
)

func TestRecordName(t *testing.T) {
	testSchemaInvalid(t, `{"type":"record","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`, "Record ought to have valid name: schema ought to have name key")
	testSchemaInvalid(t, `{"type":"record","name":3}`, "Record ought to have valid name: schema name ought to be non-empty string")
	testSchemaInvalid(t, `{"type":"record","name":""}`, "Record ought to have valid name: schema name ought to be non-empty string")
	testSchemaInvalid(t, `{"type":"record","name":"&foo","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`, "Record ought to have valid name: schema name ought to start with")
	testSchemaInvalid(t, `{"type":"record","name":"foo&","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`, "Record ought to have valid name: schema name ought to have second and remaining")
}

func TestRecordFields(t *testing.T) {
	testSchemaInvalid(t, `{"type":"record","name":"r1"}`, `Record "r1" ought to have fields key`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":3}`, `Record "r1" fields ought to be non-nil array`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":null}`, `Record "r1" fields ought to be non-nil array`)
}

func TestRecordFieldInvalid(t *testing.T) {
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[3]}`, `Record "r1" field 1 ought to be valid Avro named type`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[""]}`, `Record "r1" field 1 ought to be valid Avro named type`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[{}]}`, `Record "r1" field 1 ought to be valid Avro named type`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[{"type":"int"}]}`, `Record "r1" field 1 ought to have valid name`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[{"name":"f1"}]}`, `Record "r1" field 1 ought to be valid Avro named type`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[{"name":"f1","type":"integer"}]}`, `Record "r1" field 1 ought to be valid Avro named type`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[{"name":"f1","type":"int"},{"name":"f1","type":"long"}]}`, `Record "r1" field 2 ought to have unique name`)
}

func TestSchemaRecord(t *testing.T) {
	testSchemaValid(t, `{
  "name": "person",
  "type": "record",
  "fields": [
    {
      "name": "height",
      "type": "long"
    },
    {
      "name": "weight",
      "type": "long"
    },
    {
      "name": "name",
      "type": "string"
    }
  ]
}`)
}

func TestSchemaRecordFieldWithDefaults(t *testing.T) {
	testSchemaValid(t, `{
  "name": "person",
  "type": "record",
  "fields": [
    {
      "name": "height",
      "type": "long"
    },
    {
      "name": "weight",
      "type": "long"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "hacker",
      "type": "boolean",
      "default": false
    }
  ]
}`)
}

func TestRecordDecodedEmptyBuffer(t *testing.T) {
	testBinaryDecodeFailShortBuffer(t, `{"type":"record","name":"foo","fields":[{"name":"field1","type":"int"}]}`, nil)
}

func TestRecordFieldTypeHasPrimitiveName(t *testing.T) {
	codec, err := NewCodec(`{
  "type": "record",
  "name": "r1",
  "namespace": "com.example",
  "fields": [
    {
      "name": "f1",
      "type": "string"
    },
    {
      "name": "f2",
      "type": {
        "type": "int"
      }
    }
  ]
}`)
	ensureError(t, err)

	datumIn := map[string]interface{}{
		"f1": "thirteen",
		"f2": 13,
	}

	buf, err := codec.BinaryFromNative(nil, datumIn)
	ensureError(t, err)
	if expected := []byte{
		0x10, // field1 size = 8
		't', 'h', 'i', 'r', 't', 'e', 'e', 'n',
		0x1a, // field2 == 13
	}; !bytes.Equal(buf, expected) {
		t.Errorf("GOT: %#v; WANT: %#v", buf, expected)
	}

	// round trip
	datumOut, buf, err := codec.NativeFromBinary(buf)
	ensureError(t, err)
	if actual, expected := len(buf), 0; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
	datumOutMap, ok := datumOut.(map[string]interface{})
	if !ok {
		t.Errorf("GOT: %#v; WANT: %#v", ok, true)
	}
	if actual, expected := len(datumOutMap), len(datumIn); actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
	for k, v := range datumIn {
		if actual, expected := fmt.Sprintf("%v", datumOutMap[k]), fmt.Sprintf("%v", v); actual != expected {
			t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
		}
	}
}

func TestSchemaRecordRecursive(t *testing.T) {
	testSchemaValid(t, `{
  "type": "record",
  "name": "recursive",
  "fields": [
    {
      "name": "label",
      "type": "string"
    },
    {
      "name": "children",
      "type": {
        "type": "array",
        "items": "recursive"
      }
    }
  ]
}`)
}

func TestSchemaNamespaceRecursive(t *testing.T) {
	testSchemaValid(t, `{
  "type": "record",
  "name": "Container",
  "namespace": "namespace1",
  "fields": [
    {
      "name": "contained",
      "type": {
        "type": "record",
        "name": "MutuallyRecursive",
        "fields": [
          {
            "name": "label",
            "type": "string"
          },
          {
            "name": "children",
            "type": {
              "type": "array",
              "items": {
                "type": "record",
                "name": "MutuallyRecursive",
                "namespace": "namespace2",
                "fields": [
                  {
                    "name": "value",
                    "type": "int"
                  },
                  {
                    "name": "children",
                    "type": {
                      "type": "array",
                      "items": "namespace1.MutuallyRecursive"
                    }
                  },
                  {
                    "name": "morechildren",
                    "type": {
                      "type": "array",
                      "items": "MutuallyRecursive"
                    }
                  }
                ]
              }
            }
          },
          {
            "name": "anotherchild",
            "type": "namespace2.MutuallyRecursive"
          }
        ]
      }
    }
  ]
}`)
}

func TestSchemaRecordNamespaceComposite(t *testing.T) {
	testSchemaValid(t, `{
  "type": "record",
  "namespace": "x",
  "name": "Y",
  "fields": [
    {
      "name": "e",
      "type": {
        "type": "record",
        "name": "Z",
        "fields": [
          {
            "name": "f",
            "type": "x.Z"
          }
        ]
      }
    }
  ]
}`)
}

func TestSchemaRecordNamespaceFullName(t *testing.T) {
	testSchemaValid(t, `{
  "type": "record",
  "name": "x.Y",
  "fields": [
    {
      "name": "e",
      "type": {
        "type": "record",
        "name": "Z",
        "fields": [
          {
            "name": "f",
            "type": "x.Y"
          },
          {
            "name": "g",
            "type": "x.Z"
          }
        ]
      }
    }
  ]
}`)
}

func TestSchemaRecordNamespaceEnum(t *testing.T) {
	testSchemaValid(t, `{"type": "record", "name": "org.apache.avro.tests.Hello", "fields": [
  {"name": "f1", "type": {"type": "enum", "name": "MyEnum", "symbols": ["Foo", "Bar", "Baz"]}},
  {"name": "f2", "type": "org.apache.avro.tests.MyEnum"},
  {"name": "f3", "type": "MyEnum"},
  {"name": "f4", "type": {"type": "enum", "name": "other.namespace.OtherEnum", "symbols": ["one", "two", "three"]}},
  {"name": "f5", "type": "other.namespace.OtherEnum"},
  {"name": "f6", "type": {"type": "enum", "name": "ThirdEnum", "namespace": "some.other", "symbols": ["Alice", "Bob"]}},
  {"name": "f7", "type": "some.other.ThirdEnum"}
]}`)
}

func TestSchemaRecordNamespaceFixed(t *testing.T) {
	testSchemaValid(t, `{"type": "record", "name": "org.apache.avro.tests.Hello", "fields": [
  {"name": "f1", "type": {"type": "fixed", "name": "MyFixed", "size": 16}},
  {"name": "f2", "type": "org.apache.avro.tests.MyFixed"},
  {"name": "f3", "type": "MyFixed"},
  {"name": "f4", "type": {"type": "fixed", "name": "other.namespace.OtherFixed", "size": 18}},
  {"name": "f5", "type": "other.namespace.OtherFixed"},
  {"name": "f6", "type": {"type": "fixed", "name": "ThirdFixed", "namespace": "some.other", "size": 20}},
  {"name": "f7", "type": "some.other.ThirdFixed"}
]}`)
}

func TestRecordNamespace(t *testing.T) {
	c, err := NewCodec(`{
  "type": "record",
  "name": "org.foo.Y",
  "fields": [
    {
      "name": "X",
      "type": {
        "type": "fixed",
        "size": 4,
        "name": "fixed_4"
      }
    },
    {
      "name": "Z",
      "type": {
        "type": "fixed_4"
      }
    }
  ]
}`)
	ensureError(t, err)

	datumIn := map[string]interface{}{
		"X": []byte("abcd"),
		"Z": []byte("efgh"),
	}

	buf, err := c.BinaryFromNative(nil, datumIn)
	ensureError(t, err)
	if expected := []byte("abcdefgh"); !bytes.Equal(buf, expected) {
		t.Errorf("GOT: %#v; WANT: %#v", buf, expected)
	}

	// round trip
	datumOut, buf, err := c.NativeFromBinary(buf)
	ensureError(t, err)
	if actual, expected := len(buf), 0; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
	datumOutMap, ok := datumOut.(map[string]interface{})
	if !ok {
		t.Errorf("GOT: %#v; WANT: %#v", ok, true)
	}
	if actual, expected := len(datumOutMap), len(datumIn); actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
	for k, v := range datumIn {
		if actual, expected := fmt.Sprintf("%s", datumOutMap[k]), fmt.Sprintf("%s", v); actual != expected {
			t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
		}
	}
}

func TestRecordEncodeFail(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "r1",
  "fields": [
    {"name": "f1", "type": "string"},
    {"name": "f2", "type": "string"}
  ]
}`

	testBinaryEncodeFail(t, schema, map[string]interface{}{"f1": "foo"}, `field "f2": schema does not specify default value and no value provided`)
	testBinaryEncodeFail(t, schema, map[string]interface{}{"f1": "foo", "f2": 13}, `field "f2": value does not match its schema`)
}

func TestRecordTextDecodeFail(t *testing.T) {
	schema := `{"name":"r1","type":"record","fields":[{"name":"string","type":"string"},{"name":"bytes","type":"bytes"}]}`
	testTextDecodeFail(t, schema, []byte(`    "string"  :  "silly"  ,   "bytes"  : "silly" } `), "expected: '{'")
	testTextDecodeFail(t, schema, []byte(`  {  16  :  "silly"  ,   "bytes"  : "silly" } `), "expected initial \"")
	testTextDecodeFail(t, schema, []byte(`  {  "badName"  :  "silly"  ,   "bytes"  : "silly" } `), "cannot determine codec")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  ,  "silly"  ,   "bytes"  : "silly" } `), "expected: ':'")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  13  ,   "bytes"  : "silly" } `), "expected initial \"")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly" :   "bytes"  : "silly" } `), "expected ',' or '}'")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly" ,   "bytes"  : "silly"  `), "short buffer")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly"  `), "short buffer")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly" } `), "only found 1 of 2 fields")
}

func TestRecordTextCodecPass(t *testing.T) {
	silly := "⌘ "
	testTextEncodePass(t, `{"name":"r1","type":"record","fields":[{"name":"string","type":"string"}]}`, map[string]interface{}{"string": silly}, []byte(`{"string":"\u0001\u2318 "}`))
	testTextEncodePass(t, `{"name":"r1","type":"record","fields":[{"name":"bytes","type":"bytes"}]}`, map[string]interface{}{"bytes": []byte(silly)}, []byte(`{"bytes":"\u0001\u00E2\u008C\u0098 "}`))
	testTextDecodePass(t, `{"name":"r1","type":"record","fields":[{"name":"string","type":"string"},{"name":"bytes","type":"bytes"}]}`, map[string]interface{}{"string": silly, "bytes": []byte(silly)}, []byte(` { "string" : "\u0001\u2318 " , "bytes" : "\u0001\u00E2\u008C\u0098 " }`))
}

func TestRecordFieldDefaultValue(t *testing.T) {
	testSchemaValid(t, `{"type":"record","name":"r1","fields":[{"name":"f1","type":"int","default":13}]}`)
	testSchemaValid(t, `{"type":"record","name":"r1","fields":[{"name":"f1","type":"string","default":"foo"}]}`)
	testSchemaInvalid(t,
		`{"type":"record","name":"r1","fields":[{"name":"f1","type":"int","default":"foo"}]}`,
		"default value ought to have a number type")
}

func TestRecordFieldUnionDefaultValue(t *testing.T) {
	o := DefaultCodecOption()
	testSchemaValidWithOption(t, `{"type":"record","name":"r1","fields":[{"name":"f1","type":["int","null"],"default":13}]}`, o)
	testSchemaValidWithOption(t, `{"type":"record","name":"r1","fields":[{"name":"f1","type":["null","int"],"default":null}]}`, o)
	testSchemaValidWithOption(t, `{"type":"record","name":"r1","fields":[{"name":"f1","type":["null","int"],"default":"null"}]}`, o)
	o.EnableStringNull = false
	testSchemaInvalidWithOption(t, `{"type":"record","name":"r1","fields":[{"name":"f1","type":["null","int"],"default":"null"}]}`,
		"default value ought to encode using field schema", o)
	o.EnableStringNull = true
	testSchemaValidWithOption(t, `{"type":"record","name":"r1","fields":[{"name":"f1","type":["null","int"],"default":"null"}]}`, o)
}

func TestRecordFieldUnionInvalidDefaultValue(t *testing.T) {
	testSchemaInvalid(t,
		`{"type":"record","name":"r1","fields":[{"name":"f1","type":["null","int"],"default":13}]}`,
		"default value ought to encode using field schema")
	testSchemaInvalid(t,
		`{"type":"record","name":"r1","fields":[{"name":"f1","type":["int","null"],"default":null}]}`,
		"default value ought to encode using field schema")
}

func TestRecordRecursiveRoundTrip(t *testing.T) {
	codec, err := NewCodec(`
{
  "type": "record",
  "name": "LongList",
  "fields" : [
    {"name": "next", "type": ["null", "LongList"], "default": null}
  ]
}
`)
	ensureError(t, err)

	// NOTE: May omit fields when using default value
	initial := `{"next":{"LongList":{}}}`

	// NOTE: Textual encoding will show all fields, even those with values that
	// match their default values
	final := `{"next":{"LongList":{"next":null}}}`

	// Convert textual Avro data (in Avro JSON format) to native Go form
	datum, _, err := codec.NativeFromTextual([]byte(initial))
	ensureError(t, err)

	// Convert native Go form to binary Avro data
	buf, err := codec.BinaryFromNative(nil, datum)
	ensureError(t, err)

	// Convert binary Avro data back to native Go form
	datum, _, err = codec.NativeFromBinary(buf)
	ensureError(t, err)

	// Convert native Go form to textual Avro data
	buf, err = codec.TextualFromNative(nil, datum)
	ensureError(t, err)
	if actual, expected := string(buf), final; actual != expected {
		t.Fatalf("GOT: %v; WANT: %v", actual, expected)
	}
}

func ExampleCodec_NativeFromTextual_roundTrip() {
	codec, err := NewCodec(`
{
  "type": "record",
  "name": "LongList",
  "fields" : [
	{"name": "next", "type": ["null", "LongList"], "default": null}
  ]
}
`)
	if err != nil {
		fmt.Println(err)
	}

	// NOTE: May omit fields when using default value
	textual := []byte(`{"next":{"LongList":{"next":{"LongList":{}}}}}`)

	// Convert textual Avro data (in Avro JSON format) to native Go form
	native, _, err := codec.NativeFromTextual(textual)
	if err != nil {
		fmt.Println(err)
	}

	// Convert native Go form to binary Avro data
	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		fmt.Println(err)
	}

	// Convert binary Avro data back to native Go form
	native, _, err = codec.NativeFromBinary(binary)
	if err != nil {
		fmt.Println(err)
	}

	// Convert native Go form to textual Avro data
	textual, err = codec.TextualFromNative(nil, native)
	if err != nil {
		fmt.Println(err)
	}

	// NOTE: Textual encoding will show all fields, even those with values that
	// match their default values
	fmt.Println(string(textual))
	// Output: {"next":{"LongList":{"next":{"LongList":{"next":null}}}}}
}

func ExampleCodec_BinaryFromNative_avro() {
	codec, err := NewCodec(`
{
  "type": "record",
  "name": "LongList",
  "fields" : [
    {"name": "next", "type": ["null", "LongList"], "default": null}
  ]
}
`)
	if err != nil {
		fmt.Println(err)
	}

	// Convert native Go form to binary Avro data
	binary, err := codec.BinaryFromNative(nil, map[string]interface{}{
		"next": map[string]interface{}{
			"LongList": map[string]interface{}{
				"next": map[string]interface{}{
					"LongList": map[string]interface{}{
						// NOTE: May omit fields when using default value
					},
				},
			},
		},
	})
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("%#v", binary)
	// Output: []byte{0x2, 0x2, 0x0}
}

func ExampleCodec_NativeFromBinary_avro() {
	codec, err := NewCodec(`
{
  "type": "record",
  "name": "LongList",
  "fields" : [
    {"name": "next", "type": ["null", "LongList"], "default": null}
  ]
}
`)
	if err != nil {
		fmt.Println(err)
	}

	// Convert native Go form to binary Avro data
	binary := []byte{0x2, 0x2, 0x0}

	native, _, err := codec.NativeFromBinary(binary)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("%v", native)
	// Output: map[next:map[LongList:map[next:map[LongList:map[next:<nil>]]]]]
}

func ExampleCodec_NativeFromTextual_avro() {
	codec, err := NewCodec(`
{
  "type": "record",
  "name": "LongList",
  "fields" : [
    {"name": "next", "type": ["null", "LongList"], "default": null}
  ]
}
`)
	if err != nil {
		fmt.Println(err)
	}

	// Convert native Go form to text Avro data
	text := []byte(`{"next":{"LongList":{"next":{"LongList":{"next":null}}}}}`)

	native, _, err := codec.NativeFromTextual(text)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("%v", native)
	// Output: map[next:map[LongList:map[next:map[LongList:map[next:<nil>]]]]]
}

func ExampleCodec_TextualFromNative_avro() {
	codec, err := NewCodec(`
{
  "type": "record",
  "name": "LongList",
  "fields" : [
    {"name": "next", "type": ["null", "LongList"], "default": null}
  ]
}
`)
	if err != nil {
		fmt.Println(err)
	}

	// Convert native Go form to text Avro data
	text, err := codec.TextualFromNative(nil, map[string]interface{}{
		"next": map[string]interface{}{
			"LongList": map[string]interface{}{
				"next": map[string]interface{}{
					"LongList": map[string]interface{}{
						// NOTE: May omit fields when using default value
					},
				},
			},
		},
	})
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("%s", text)
	// Output: {"next":{"LongList":{"next":{"LongList":{"next":null}}}}}
}

func TestRecordFieldFixedDefaultValue(t *testing.T) {
	testSchemaValid(t, `{"type": "record", "name": "r1", "fields":[{"name": "f1", "type": {"type": "fixed", "name": "someFixed", "size": 1}, "default": "\u0000"}]}`)
}

func TestRecordFieldDecimalDefaultValue(t *testing.T) {
	testSchemaValid(t, `{"type": "record", "name": "r1", "fields":[{"name": "f1", "type": {"type": "bytes", "scale": 2, "precision":10, "logicalType":"deicmal"}, "default": "d"}]}`)
}

func TestRecordFieldDefaultValueTypes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		codec, err := NewCodec(`{"type": "record", "name": "r1", "fields":[{"name": "someBoolean", "type": "boolean", "default": true},{"name": "someBytes", "type": "bytes", "default": "0"},{"name": "someDouble", "type": "double", "default": 0},{"name": "someFloat", "type": "float", "default": 0},{"name": "someInt", "type": "int", "default": 0},{"name": "someLong", "type": "long", "default": 0},{"name": "someString", "type": "string", "default": "0"}, {"name":"someTimestamp", "type":"long", "logicalType":"timestamp-millis","default":0}, {"name": "someDecimal", "type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2, "default":"\u0000"}]}`)
		ensureError(t, err)

		r1, _, err := codec.NativeFromTextual([]byte("{}"))
		ensureError(t, err)

		r1m := r1.(map[string]interface{})

		someBoolean := r1m["someBoolean"]
		if _, ok := someBoolean.(bool); !ok {
			t.Errorf("GOT: %T; WANT: []byte", someBoolean)
		}

		someBytes := r1m["someBytes"]
		if _, ok := someBytes.([]byte); !ok {
			t.Errorf("GOT: %T; WANT: []byte", someBytes)
		}

		someDouble := r1m["someDouble"]
		if _, ok := someDouble.(float64); !ok {
			t.Errorf("GOT: %T; WANT: float64", someDouble)
		}

		someFloat := r1m["someFloat"]
		if _, ok := someFloat.(float32); !ok {
			t.Errorf("GOT: %T; WANT: float32", someFloat)
		}

		someInt := r1m["someInt"]
		if _, ok := someInt.(int32); !ok {
			t.Errorf("GOT: %T; WANT: int32", someInt)
		}

		someLong := r1m["someLong"]
		if _, ok := someLong.(int64); !ok {
			t.Errorf("GOT: %T; WANT: int64", someLong)
		}

		someString := r1m["someString"]
		if _, ok := someString.(string); !ok {
			t.Errorf("GOT: %T; WANT: string", someString)
		}
		someTimestamp := r1m["someTimestamp"]
		if _, ok := someTimestamp.(float64); !ok {
			t.Errorf("GOT: %T; WANT: float64", someTimestamp)
		}
		someDecimal := r1m["someDecimal"]
		if _, ok := someDecimal.(*big.Rat); !ok {
			t.Errorf("GOT: %T; WANT: *big.Rat", someDecimal)
		}
	})

	t.Run("provided default is wrong type", func(t *testing.T) {
		t.Run("long", func(t *testing.T) {
			_, err := NewCodec(`{"type": "record", "name": "r1", "fields":[{"name": "someLong", "type": "long", "default": "0"},{"name": "someInt", "type": "int", "default": 0},{"name": "someFloat", "type": "float", "default": 0},{"name": "someDouble", "type": "double", "default": 0}]}`)
			ensureError(t, err, "default value ought to have a number type")
		})
		t.Run("int", func(t *testing.T) {
			_, err := NewCodec(`{"type": "record", "name": "r1", "fields":[{"name": "someLong", "type": "long", "default": 0},{"name": "someInt", "type": "int", "default": "0"},{"name": "someFloat", "type": "float", "default": 0},{"name": "someDouble", "type": "double", "default": 0}]}`)
			ensureError(t, err, "default value ought to have a number type")
		})
		t.Run("float", func(t *testing.T) {
			_, err := NewCodec(`{"type": "record", "name": "r1", "fields":[{"name": "someLong", "type": "long", "default": 0},{"name": "someInt", "type": "int", "default": 0},{"name": "someFloat", "type": "float", "default": "0"},{"name": "someDouble", "type": "double", "default": 0}]}`)
			ensureError(t, err, "default value ought to have a float type")
		})
		t.Run("double", func(t *testing.T) {
			_, err := NewCodec(`{"type": "record", "name": "r1", "fields":[{"name": "someLong", "type": "long", "default": 0},{"name": "someInt", "type": "int", "default": 0},{"name": "someFloat", "type": "float", "default": 0},{"name": "someDouble", "type": "double", "default": "0"}]}`)
			ensureError(t, err, "default value ought to have a double type")
		})
	})

	t.Run("union of int and long", func(t *testing.T) {
		t.Skip("FIXME: should encode default value as int64 rather than float64")

		codec, err := NewCodec(`{"type":"record","name":"r1","fields":[{"name":"f1","type":["int","long"],"default":13}]}`)
		ensureError(t, err)

		r1, _, err := codec.NativeFromTextual([]byte("{}"))
		ensureError(t, err)

		r1m := r1.(map[string]interface{})

		someUnion := r1m["f1"]
		someMap, ok := someUnion.(map[string]interface{})
		if !ok {
			t.Fatalf("GOT: %T; WANT: map[string]interface{}", someUnion)
		}
		if got, want := len(someMap), 1; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		t.Logf("someMap: %#v", someMap)
		for k, v := range someMap {
			// The "int" type is the first type option of the union.
			if got, want := k, "int"; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			switch tv := v.(type) {
			case int64:
				if got, want := tv, int64(13); got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			default:
				t.Errorf("GOT: %T; WANT: int64", v)
			}
		}
	})
}
