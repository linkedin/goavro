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
	"fmt"
	"testing"
)

func TestRecordRequiresSchema(t *testing.T) {
	_, err := NewRecord()
	checkErrorFatal(t, err, "cannot create Record: no schema defined")
}

func TestRecordFieldNames(t *testing.T) {
	someJsonSchema := `{"type":"record","name":"org.foo.Y","fields":[{"type":"int","name":"X"},{"type":"string","name":"W"}]}`
	someRecord, err := NewRecord(RecordSchemaJson(someJsonSchema))
	checkErrorFatal(t, err, nil)
	if someRecord.Name != "org.foo.Y" {
		t.Errorf("Actual: %#v; Expected: %#v", someRecord.Name, "org.foo.Y")
	}
	if someRecord.Fields[0].Name != "org.foo.X" {
		t.Errorf("Actual: %#v; Expected: %#v", someRecord.Fields[0].Name, "org.foo.X")
	}
	if someRecord.Fields[1].Name != "org.foo.W" {
		t.Errorf("Actual: %#v; Expected: %#v", someRecord.Fields[1].Name, "org.foo.W")
	}
}

func TestRecordFieldBailsWithoutName(t *testing.T) {
	schema := make(map[string]interface{})

	schema["type"] = "int"
	_, err := newRecordField(schema)
	checkError(t, err, "ought to have name key")

	schema["name"] = 5
	_, err = newRecordField(schema)
	checkError(t, err, "name ought to be non-empty string")

	schema["name"] = ""
	_, err = newRecordField(schema)
	checkError(t, err, "name ought to be non-empty string")
}

func TestRecordFieldChecksSchema(t *testing.T) {
	var err error
	schema := make(map[string]interface{})

	schema["name"] = ""
	_, err = newRecordField(schema)
	checkError(t, err, "name ought to be non-empty string")

	schema["name"] = "someRecordField"
	_, err = newRecordField(schema)
	checkError(t, err, fmt.Errorf("ought to have type key"))
}

func TestRecordField(t *testing.T) {
	schema := make(map[string]interface{})
	schema["name"] = "someRecordField"
	schema["type"] = "int"
	schema["doc"] = "contans some integer"
	someRecordField, err := newRecordField(schema)
	checkError(t, err, nil)
	if someRecordField.Name != "someRecordField" {
		t.Errorf("Actual: %#v; Expected: %#v", someRecordField.Name, "someRecordField")
	}
}

func TestRecordBailsWithoutName(t *testing.T) {
	recordFields := make([]*recordField, 0)
	{
		schema := make(map[string]interface{})
		schema["name"] = "someRecordField"
		schema["type"] = "int"
		schema["doc"] = "contans some integer"
		someRecordField, err := newRecordField(schema)
		checkErrorFatal(t, err, nil)
		recordFields = append(recordFields, someRecordField)
	}

	schema := make(map[string]interface{})
	schema["fields"] = recordFields

	schema["name"] = 5
	_, err := NewRecord(RecordSchema(schema))
	checkErrorFatal(t, err, "ought to be non-empty string")

	schema["name"] = ""
	_, err = NewRecord(RecordSchema(schema))
	checkError(t, err, "ought to be non-empty string")
}

func TestRecordBailsWithoutFields(t *testing.T) {
	schema := make(map[string]interface{})

	schema["name"] = "someRecord"
	_, err := NewRecord(RecordSchema(schema))
	checkError(t, err, fmt.Errorf("record requires fields"))

	schema["fields"] = 5
	_, err = NewRecord(RecordSchema(schema))
	checkError(t, err, fmt.Errorf("record fields ought to be non-empty array"))

	schema["fields"] = make([]interface{}, 0)
	_, err = NewRecord(RecordSchema(schema))
	checkError(t, err, fmt.Errorf("record fields ought to be non-empty array"))

	fields := make([]interface{}, 0)
	fields = append(fields, "int")
	schema["fields"] = fields
	_, err = NewRecord(RecordSchema(schema))
	checkError(t, err, fmt.Errorf("expected: map[string]interface{}; actual: string"))
}

func TestRecordFieldUnion(t *testing.T) {
	someJsonSchema := `{"type":"record","name":"Foo","fields":[{"type":["null","string"],"name":"field1"}]}`
	_, err := NewRecord(RecordSchemaJson(someJsonSchema))
	checkError(t, err, nil)
}

func TestRecordGetFieldSchema(t *testing.T) {
	outerSchema := `
{
  "type": "record",
  "name": "TestRecord",
  "fields": [
    {
      "name": "value",
      "type": "int"
    },
    {
      "name": "rec",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TestRecord2",
          "fields": [
            {
              "name": "stringValue",
              "type": "string"
            },
            {
              "name": "intValue",
              "type": "int"
            }
          ]
        }
      }
    }
  ]
}
`
	outerRecord, err := NewRecord(RecordSchemaJson(outerSchema))
	checkErrorFatal(t, err, nil)
	// make sure it bails when no such schema
	schema, err := outerRecord.GetFieldSchema("no-such-field")
	checkError(t, err, "no such field: no-such-field")
	// get the inner schema
	schema, err = outerRecord.GetFieldSchema("rec")
	checkErrorFatal(t, err, nil)
	_, ok := schema.(map[string]interface{})
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
}
