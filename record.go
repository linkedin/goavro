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
	"encoding/json"
	"fmt"
	"strings"
)

// Record is an abstract data type used to hold data corresponding to
// an Avro record. Wherever an Avro schema specifies a record, this
// library's Decode method will return a Record initialized to the
// record's values read from the io.Reader. Likewise, when using
// Encode to convert data to an Avro record, it is necessary to create
// and send a Record instance to the Encoder method.
type Record struct {
	Name      string
	Fields    []*recordField
	aliases   []string
	doc       string
	n         *name
	ens       string
	schemaMap map[string]interface{}
}

// Get returns the datum of the specified Record field.
func (r Record) Get(fieldName string) (interface{}, error) {
	// qualify fieldName searches based on record namespace
	fn, err := newName(nameName(fieldName), nameNamespace(r.n.ns))
	if err != nil {
		return nil, fmt.Errorf("error getting field name: %s", fieldName)
	}

	for _, field := range r.Fields {
		if field.Name == fn.n {
			return field.Datum, nil
		}
	}
	return nil, fmt.Errorf("no such field: %s", fieldName)
}

// GetFieldSchema returns the schema of the specified Record field.
func (r Record) GetFieldSchema(fieldName string) (interface{}, error) {
	// qualify fieldName searches based on record namespace
	fn, err := newName(nameName(fieldName), nameNamespace(r.n.ns))
	if err != nil {
		return nil, fmt.Errorf("error getting field name: %s", fieldName)
	}

	for _, field := range r.Fields {
		if field.Name == fn.n {
			return field.schema, nil
		}
	}
	return nil, fmt.Errorf("no such field: %s", fieldName)
}

// Set updates the datum of the specified Record field.
func (r Record) Set(fieldName string, value interface{}) error {
	// qualify fieldName searches based on record namespace
	fn, err := newName(nameName(fieldName), nameNamespace(r.n.ns))
	if err != nil {
		return fmt.Errorf("error getting field name: %s", fieldName)
	}

	for _, field := range r.Fields {
		if field.Name == fn.n {
			field.Datum = value
			return nil
		}
	}
	return fmt.Errorf("no such field: %s", fieldName)
}

// String returns a string representation of the Record.
func (r Record) String() string {
	fields := make([]string, len(r.Fields))
	for idx, f := range r.Fields {
		fields[idx] = fmt.Sprintf("%v", f)
	}
	return fmt.Sprintf("{%s: [%v]}", r.Name, strings.Join(fields, ", "))
}

// NewRecord will create a Record instance corresponding to the
// specified schema.
//
//    func recordExample(codec goavro.Codec, w io.Writer, recordSchema string) error {
//         // To encode a Record, you need to instantiate a Record instance
//         // that adheres to the schema the Encoder expect.
//         someRecord, err := goavro.NewRecord(goavro.RecordSchema(recordSchema))
//         if err != nil {
//             return err
//         }
//         // Once you have a Record, you can set the values of the various fields.
//         someRecord.Set("username", "Aquaman")
//         someRecord.Set("comment", "The Atlantic is oddly cold this morning!")
//         // Feel free to fully qualify the field name if you'd like
//         someRecord.Set("com.example.timestamp", int64(1082196484))
//
//         // Once the fields of the Record have the correct data, you can encode it
//         err = codec.Encode(w, someRecord)
//         return err
//     }
func NewRecord(setters ...RecordSetter) (*Record, error) {
	record := &Record{n: &name{}}
	for _, setter := range setters {
		err := setter(record)
		if err != nil {
			return nil, err
		}
	}
	if record.schemaMap == nil {
		return nil, newCodecBuildError("record", "no schema defined")
	}
	var err error
	record.n, err = newName(nameSchema(record.schemaMap), nameEnclosingNamespace(record.ens))
	if err != nil {
		return nil, newCodecBuildError("record", err)
	}
	record.Name = record.n.n
	ns := record.n.namespace()

	val, ok := record.schemaMap["fields"]
	if !ok {
		return nil, newCodecBuildError("record", "record requires one or more fields")
	}
	fields, ok := val.([]interface{})
	if !ok || len(fields) == 0 {
		return nil, newCodecBuildError("record", "record fields ought to be non-empty array")
	}

	record.Fields = make([]*recordField, len(fields))
	for i, field := range fields {
		rf, err := newRecordField(field, recordFieldEnclosingNamespace(ns))
		if err != nil {
			return nil, newCodecBuildError("record", err)
		}
		record.Fields[i] = rf
	}

	// fields optional to the avro spec

	if val, ok = record.schemaMap["doc"]; ok {
		record.doc, ok = val.(string)
		if !ok {
			return nil, newCodecBuildError("record", "doc ought to be string")
		}
	}
	if val, ok = record.schemaMap["aliases"]; ok {
		record.aliases, ok = val.([]string)
		if !ok {
			return nil, newCodecBuildError("record", "aliases ought to be array of strings")
		}
	}
	record.schemaMap = nil
	return record, nil
}

// RecordSetter functions are those those which are used to
// instantiate a new Record.
type RecordSetter func(*Record) error

// recordSchemaRaw specifies the schema of the record to create. Schema
// must be `map[string]interface{}`.
func recordSchemaRaw(schema interface{}) RecordSetter {
	return func(r *Record) error {
		var ok bool
		r.schemaMap, ok = schema.(map[string]interface{})
		if !ok {
			return newCodecBuildError("record", "expected: map[string]interface{}; received: %T", schema)
		}
		return nil
	}
}

// RecordSchema specifies the schema of the record to
// create. Schema must be a JSON string.
func RecordSchema(recordSchemaJSON string) RecordSetter {
	return func(r *Record) error {
		var schema interface{}
		err := json.Unmarshal([]byte(recordSchemaJSON), &schema)
		if err != nil {
			return newCodecBuildError("record", err)
		}
		var ok bool
		r.schemaMap, ok = schema.(map[string]interface{})
		if !ok {
			return newCodecBuildError("record", "expected: map[string]interface{}; received: %T", schema)
		}
		return nil
	}
}

// RecordEnclosingNamespace specifies the enclosing namespace of the
// record to create. For instance, if the enclosing namespace is
// `com.example`, and the record name is `Foo`, then the full record
// name will be `com.example.Foo`.
func RecordEnclosingNamespace(someNamespace string) RecordSetter {
	return func(r *Record) error {
		r.ens = someNamespace
		return nil
	}
}

////////////////////////////////////////

type recordField struct {
	Name       string
	Datum      interface{}
	doc        string
	defval     interface{}
	hasDefault bool
	order      string
	aliases    []string
	schema     interface{}
	ens        string
}

func (rf recordField) String() string {
	return fmt.Sprintf("%s: %v", rf.Name, rf.Datum)
}

type recordFieldSetter func(*recordField) error

func recordFieldEnclosingNamespace(someNamespace string) recordFieldSetter {
	return func(rf *recordField) error {
		rf.ens = someNamespace
		return nil
	}
}

func newRecordField(schema interface{}, setters ...recordFieldSetter) (*recordField, error) {
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, newCodecBuildError("record field", "schema expected: map[string]interface{}; received: %T", schema)
	}

	rf := &recordField{}
	for _, setter := range setters {
		err := setter(rf)
		if err != nil {
			return nil, newCodecBuildError("record field", err)
		}
	}

	n, err := newName(nameSchema(schemaMap), nameEnclosingNamespace(rf.ens))
	if err != nil {
		return nil, newCodecBuildError("record field", err)
	}
	rf.Name = n.n

	typeName, ok := schemaMap["type"]
	if !ok {
		return nil, newCodecBuildError("record field", "ought to have type key")
	}
	rf.schema = schema

	// fields optional to the avro spec

	val, ok := schemaMap["default"]
	if ok {
		rf.hasDefault = true
		switch typeName.(type) {
		case string:
			switch typeName {
			case "int":
				dv, ok := val.(float64)
				if !ok {
					return nil, newCodecBuildError("record field", "default value type mismatch: %s; expected: %s; received: %T", rf.Name, "int32", val)
				}
				rf.defval = int32(dv)
			case "long":
				dv, ok := val.(float64)
				if !ok {
					return nil, newCodecBuildError("record field", "default value type mismatch: %s; expected: %s; received: %T", rf.Name, "int64", val)
				}
				rf.defval = int64(dv)
			case "float":
				dv, ok := val.(float64)
				if !ok {
					return nil, newCodecBuildError("record field", "default value type mismatch: %s; expected: %s; received: %T", rf.Name, "float32", val)
				}
				rf.defval = float32(dv)
			case "bytes":
				dv, ok := val.(string)
				if !ok {
					return nil, newCodecBuildError("record field", "default value type mismatch: %s; expected: %s; received: %T", rf.Name, "string", val)
				}
				rf.defval = []byte(dv)
			default:
				rf.defval = val
			}
		default:
			rf.defval = val
		}
	}

	if val, ok = schemaMap["doc"]; ok {
		rf.doc, ok = val.(string)
		if !ok {
			return nil, newCodecBuildError("record field", "record field doc ought to be string")
		}
	}

	if val, ok = schemaMap["order"]; ok {
		rf.order, ok = val.(string)
		if !ok {
			return nil, newCodecBuildError("record field", "record field order ought to be string")
		}
		switch rf.order {
		case "ascending", "descending", "ignore":
			// ok
		default:
			return nil, newCodecBuildError("record field", "record field order ought to bescending, descending, or ignore")
		}
	}

	if val, ok = schemaMap["aliases"]; ok {
		rf.aliases, ok = val.([]string)
		if !ok {
			return nil, newCodecBuildError("record field", "record field aliases ought to be array of strings")
		}
	}

	return rf, nil
}
