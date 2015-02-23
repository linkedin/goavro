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
	"strings"
)

// Record is an abstract data type used to hold data corresponding to
// an Avro record. Wherever an Avro schema specifies a record, this
// library's Decode method will return a Record initialized to the
// record's values read from the io.Reader. Likewise, when using
// Encode to convert data to an Avro record, it is necessary to create
// and send a Record instance to the Encoder method.
type Record struct {
	Name    string
	Fields  []*recordField
	aliases []string
	doc     string
	n       *name
	ens     string
}

// String returns a string representation of the Record.
func (r Record) String() string {
	fields := make([]string, len(r.Fields))
	for idx, f := range r.Fields {
		fields[idx] = fmt.Sprintf("%v", f)
	}
	return fmt.Sprintf("{%s: [%v]}", r.Name, strings.Join(fields, ", "))
}

// NewRecord will create a Record corresponding to the specified
// schema.
func NewRecord(schema interface{}, setters ...RecordSetter) (*Record, error) {
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("cannot create Record: expected: map[string]interface{}; actual: %T", schema)
	}

	newRecord := &Record{n: &name{}}
	for _, setter := range setters {
		err := setter(newRecord)
		if err != nil {
			return nil, err
		}
	}

	var err error
	newRecord.n, err = newName(nameSchema(schemaMap), nameEnclosingNamespace(newRecord.ens))
	if err != nil {
		return nil, fmt.Errorf("cannot create Record: %v", err)
	}
	newRecord.Name = newRecord.n.n
	ns := newRecord.n.namespace()

	val, ok := schemaMap["fields"]
	if !ok {
		return nil, fmt.Errorf("cannot create Record: record requires fields")
	}
	fields, ok := val.([]interface{})
	if !ok || len(fields) == 0 {
		return nil, fmt.Errorf("cannot create Record: record fields ought to be non-empty array")
	}

	newRecord.Fields = make([]*recordField, len(fields))
	for i, field := range fields {
		rf, err := newRecordField(field, recordFieldEnclosingNamespace(ns))
		if err != nil {
			return nil, fmt.Errorf("cannot create Record: %v", err)
		}
		newRecord.Fields[i] = rf
	}

	// fields optional to the avro spec

	if val, ok = schemaMap["doc"]; ok {
		newRecord.doc, ok = val.(string)
		if !ok {
			return nil, fmt.Errorf("record doc ought to be string")
		}
	}
	if val, ok = schemaMap["aliases"]; ok {
		newRecord.aliases, ok = val.([]string)
		if !ok {
			return nil, fmt.Errorf("record aliases ought to be array of strings")
		}
	}

	return newRecord, nil
}

// RecordSetter functions are those those which are used to
// instantiate a new Record.
type RecordSetter func(*Record) error

func recordEnclosingNamespace(someNamespace string) RecordSetter {
	return func(r *Record) error {
		r.ens = someNamespace
		return nil
	}
}

////////////////////////////////////////

type recordField struct {
	Name    string
	Datum   interface{}
	doc     string
	defval  interface{}
	order   string
	aliases []string
	schema  interface{}
	ens     string
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
	cannotCreate := makeErrorReporter("cannot create record field: ")

	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, cannotCreate("schema expected: map[string]interface{}; actual: %T", schema)
	}

	rf := &recordField{}
	for _, setter := range setters {
		err := setter(rf)
		if err != nil {
			return nil, cannotCreate("%v", err)
		}
	}

	n, err := newName(nameSchema(schemaMap), nameEnclosingNamespace(rf.ens))
	if err != nil {
		return nil, cannotCreate("%v", err)
	}
	rf.Name = n.n

	val, ok := schemaMap["type"]
	if !ok {
		return nil, cannotCreate("ought to have type key")
	}
	rf.schema = schema

	// fields optional to the avro spec

	if val, ok = schemaMap["default"]; ok {
		rf.defval = val
	}

	if val, ok = schemaMap["doc"]; ok {
		rf.doc, ok = val.(string)
		if !ok {
			return nil, cannotCreate("record field doc ought to be string")
		}
	}

	if val, ok = schemaMap["order"]; ok {
		rf.order, ok = val.(string)
		if !ok {
			return nil, cannotCreate("record field order ought to be string")
		}
		switch rf.order {
		case "ascending", "descending", "ignore":
			// ok
		default:
			return nil, cannotCreate("record field order ought to bescending, descending, or ignore")
		}
	}

	if val, ok = schemaMap["aliases"]; ok {
		rf.aliases, ok = val.([]string)
		if !ok {
			return nil, cannotCreate("record field aliases ought to be array of strings")
		}
	}

	return rf, nil
}
