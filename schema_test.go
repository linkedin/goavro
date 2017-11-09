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
	"testing"
)

func testSchemaPrimativeCodec(t *testing.T, primitiveTypeName string) {
	if _, err := NewCodec(primitiveTypeName); err != nil {
		t.Errorf("Bare primitive type: Schema: %q; Actual: %#v; Expected: %#v", primitiveTypeName, err, nil)
	}
	quoted := `"` + primitiveTypeName + `"`
	if _, err := NewCodec(quoted); err != nil {
		t.Errorf("Bare primitive type: Schema: %q; Actual: %#v; Expected: %#v", quoted, err, nil)
	}
	full := fmt.Sprintf(`{"type":"%s"}`, primitiveTypeName)
	if _, err := NewCodec(full); err != nil {
		t.Errorf("Full primitive type: Schema: %q; Actual: %#v; Expected: %#v", full, err, nil)
	}
	extra := fmt.Sprintf(`{"type":"%s","ignoredKey":"ignoredValue"}`, primitiveTypeName)
	if _, err := NewCodec(extra); err != nil {
		t.Errorf("Full primitive type with extra attributes: Schema: %q; Actual: %#v; Expected: %#v", extra, err, nil)
	}
}

func testSchemaInvalid(t *testing.T, schema, errorMessage string) {
	_, err := NewCodec(schema)
	ensureError(t, err, errorMessage)
}

func testSchemaValid(t *testing.T, schema string) {
	_, err := NewCodec(schema)
	if err != nil {
		t.Errorf("Actual: %v; Expected: %v", err, nil)
	}
}

func TestSchemaFailInvalidType(t *testing.T) {
	testSchemaInvalid(t, `{"type":"flubber"}`, "unknown type name")
}

func TestCodecSchema(t *testing.T) {
	codec, err := NewCodec(` {  "type" : "string" } `)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := codec.Schema(), `{"type":"string"}`; actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}
}

func TestSchemaWeather(t *testing.T) {
	testSchemaValid(t, `
{"type": "record", "name": "test.Weather",
 "doc": "A weather reading.",
 "fields": [
     {"name": "station", "type": "string", "order": "ignore"},
     {"name": "time", "type": "long"},
     {"name": "temp", "type": "int"}
 ]
}
`)
}

func TestSchemaFooBarSpecificRecord(t *testing.T) {
	testSchemaValid(t, `
{
    "type": "record",
    "name": "FooBarSpecificRecord",
    "namespace": "org.apache.avro",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "nicknames", "type":
            {"type": "array", "items": "string"}},
        {"name": "relatedids", "type": 
            {"type": "array", "items": "int"}},
        {"name": "typeEnum", "type": 
            ["null", { 
                    "type": "enum",
                    "name": "TypeEnum",
                    "namespace": "org.apache.avro",
                    "symbols" : ["a","b", "c"]
                }],
            "default": null
        }
    ]
}
`)
}

func TestSchemaInterop(t *testing.T) {
	testSchemaValid(t, `
{"type": "record", "name":"Interop", "namespace": "org.apache.avro",
  "fields": [
      {"name": "intField", "type": "int"},
      {"name": "longField", "type": "long"},
      {"name": "stringField", "type": "string"},
      {"name": "boolField", "type": "boolean"},
      {"name": "floatField", "type": "float"},
      {"name": "doubleField", "type": "double"},
      {"name": "bytesField", "type": "bytes"},
      {"name": "nullField", "type": "null"},
      {"name": "arrayField", "type": {"type": "array", "items": "double"}},
      {"name": "mapField", "type":
       {"type": "map", "values":
        {"type": "record", "name": "Foo",
         "fields": [{"name": "label", "type": "string"}]}}},
      {"name": "unionField", "type":
       ["boolean", "double", {"type": "array", "items": "bytes"}]},
      {"name": "enumField", "type":
       {"type": "enum", "name": "Kind", "symbols": ["A","B","C"]}},
      {"name": "fixedField", "type":
       {"type": "fixed", "name": "MD5", "size": 16}},
      {"name": "recordField", "type":
       {"type": "record", "name": "Node",
        "fields": [
            {"name": "label", "type": "string"},
            {"name": "children", "type": {"type": "array", "items": "Node"}}]}}
  ]
}
`)
}

func TestSchemaFixedNameCanBeUsedLater(t *testing.T) {
	schema := `{"type":"record","name":"record1","fields":[
                   {"name":"field1","type":{"type":"fixed","name":"fixed_4","size":4}},
                   {"name":"field2","type":"fixed_4"}]}`

	datum := map[string]interface{}{
		"field1": []byte("abcd"),
		"field2": []byte("efgh"),
	}

	testBinaryEncodePass(t, schema, datum, []byte("abcdefgh"))
}

// func ExampleCodecSchema() {
// 	schema := `{"type":"map","values":{"type":"enum","name":"foo","symbols":["alpha","bravo"]}}`
// 	codec, err := NewCodec(schema)
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	fmt.Println(codec.Schema())
// 	// Output: {"type":"map","values":{"name":"foo","type":"enum","symbols":["alpha","bravo"]}}
// }

func TestMapValueTypeEnum(t *testing.T) {
	schema := `{"type":"map","values":{"type":"enum","name":"foo","symbols":["alpha","bravo"]}}`

	datum := map[string]interface{}{"someKey": "bravo"}

	expected := []byte{
		0x2, // blockCount = 1 pair
		0xe, // key size = 7
		's', 'o', 'm', 'e', 'K', 'e', 'y',
		0x2, // value = index 1 ("bravo")
		0,   // blockCount = 0 pairs
	}

	testBinaryCodecPass(t, schema, datum, expected)
}

func TestMapValueTypeRecord(t *testing.T) {
	schema := `{"type":"map","values":{"type":"record","name":"foo","fields":[{"name":"field1","type":"string"},{"name":"field2","type":"int"}]}}`

	datum := map[string]interface{}{
		"map-key": map[string]interface{}{
			"field1": "unlucky",
			"field2": 13,
		},
	}

	expected := []byte{
		0x2,                               // blockCount = 1 key-value pair in top level map
		0xe,                               // first key size = 7
		'm', 'a', 'p', '-', 'k', 'e', 'y', // first key = "map-key"
		// this key's value is a record, which is encoded by concatenated its field values
		0x0e, // field one string size = 7
		'u', 'n', 'l', 'u', 'c', 'k', 'y',
		0x1a, // 13
		0,    // map has no more blocks
	}

	// cannot decode because order of map key enumeration random, and records
	// are returned as a Go map
	testBinaryEncodePass(t, schema, datum, expected)
}

func TestDefaultValueOughtToEncodeUsingFieldSchemaOK(t *testing.T) {

	testSchemaValid(t, `
	{
	  "namespace": "universe.of.things",
	  "type": "record",
	  "name": "Thing",
	  "fields": [
		{
		  "name": "attributes",
		  "type": [
			"null",
			{
			  "type": "array",
			  "items": {
				"namespace": "universe.of.things",
				"type": "record",
				"name": "attribute",
				"fields": [
				  {
					"name": "name",
					"type": "string"
				  },
				  {
					"name": "value",
					"type": "string"
				  }
				]
			  }
			}
		  ],
		  "default": "null"
		}
	  ]
	}`)

}
