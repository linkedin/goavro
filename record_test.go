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
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":3}`, `Record "r1" fields ought to be non-empty array`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[]}`, `Record "r1" fields ought to be non-empty array`)
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
	if err != nil {
		t.Fatal(err)
	}

	datumIn := map[string]interface{}{
		"f1": "thirteen",
		"f2": 13,
	}

	buf, err := codec.BinaryFromNative(nil, datumIn)
	if err != nil {
		t.Fatal(err)
	}
	if expected := []byte{
		0x10, // field1 size = 8
		't', 'h', 'i', 'r', 't', 'e', 'e', 'n',
		0x1a, // field2 == 13
	}; !bytes.Equal(buf, expected) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, expected)
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
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	if actual, expected := len(datumOutMap), len(datumIn); actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	for k, v := range datumIn {
		if actual, expected := fmt.Sprintf("%v", datumOutMap[k]), fmt.Sprintf("%v", v); actual != expected {
			t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
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
	if err != nil {
		t.Fatal(err)
	}

	datumIn := map[string]interface{}{
		"X": []byte("abcd"),
		"Z": []byte("efgh"),
	}

	buf, err := c.BinaryFromNative(nil, datumIn)
	if err != nil {
		t.Fatal(err)
	}
	if expected := []byte("abcdefgh"); !bytes.Equal(buf, expected) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, expected)
	}

	// round trip
	datumOut, buf, err := c.NativeFromBinary(buf)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := len(buf), 0; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	datumOutMap, ok := datumOut.(map[string]interface{})
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	if actual, expected := len(datumOutMap), len(datumIn); actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	for k, v := range datumIn {
		if actual, expected := fmt.Sprintf("%s", datumOutMap[k]), fmt.Sprintf("%s", v); actual != expected {
			t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
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
		"default value ought to encode using field schema")
}

func TestRecordFieldUnionDefaultValue(t *testing.T) {
	testSchemaValid(t, `{"type":"record","name":"r1","fields":[{"name":"f1","type":["int","null"],"default":13}]}`)
	testSchemaValid(t, `{"type":"record","name":"r1","fields":[{"name":"f1","type":["null","int"],"default":null}]}`)
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
	if err != nil {
		t.Fatal(err)
	}

	// NOTE: May omit fields when using default value
	initial := `{"next":{"LongList":{}}}`

	// NOTE: Textual encoding will show all fields, even those with values that
	// match their default values
	final := `{"next":{"LongList":{"next":null}}}`

	// Convert textual Avro data (in Avro JSON format) to native Go form
	datum, _, err := codec.NativeFromTextual([]byte(initial))
	if err != nil {
		t.Fatal(err)
	}

	// Convert native Go form to binary Avro data
	buf, err := codec.BinaryFromNative(nil, datum)
	if err != nil {
		t.Fatal(err)
	}

	// Convert binary Avro data back to native Go form
	datum, _, err = codec.NativeFromBinary(buf)
	if err != nil {
		t.Fatal(err)
	}

	// Convert native Go form to textual Avro data
	buf, err = codec.TextualFromNative(nil, datum)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := string(buf), final; actual != expected {
		t.Fatalf("Actual: %v; Expected: %v", actual, expected)
	}
}

func ExampleRecordRecursiveRoundTrip() {
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

func ExampleBinaryFromNative() {
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

func ExampleNativeFromBinary() {
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

func ExampleNativeFromTextual() {
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

func ExampleTextualFromNative() {
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
