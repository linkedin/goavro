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

package main

import (
	"bytes"
	"fmt"
	"github.com/linkedin/goavro"
)

var (
	outerSchema, innerSchema string
)

func init() {
	innerSchema = `
{
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
`
	outerSchema = fmt.Sprintf(`
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
        "items": %s
      }
    }
  ]
}
`, innerSchema)
}

func makeOuterRecord() (*goavro.Record, error) {
	innerRecords := make([]interface{}, 0)
	innerRecord, err := goavro.NewRecord(goavro.RecordSchemaJson(innerSchema))
	if err != nil {
		return nil, err
	}
	innerRecord.Set("stringValue", "Hello")
	innerRecord.Set("intValue", int32(1))
	innerRecords = append(innerRecords, innerRecord)

	innerRecord, err = goavro.NewRecord(goavro.RecordSchemaJson(innerSchema))
	if err != nil {
		return nil, err
	}
	innerRecord.Set("stringValue", "World")
	innerRecord.Set("intValue", int32(2))
	innerRecords = append(innerRecords, innerRecord)

	outerRecord, err := goavro.NewRecord(goavro.RecordSchemaJson(outerSchema))
	if err != nil {
		return nil, err
	}
	outerRecord.Set("value", int32(3))
	outerRecord.Set("rec", innerRecords)
	return outerRecord, nil
}

func main() {
	// make a codec
	c, err := goavro.NewCodec(outerSchema)
	if err != nil {
		panic(fmt.Errorf("cannot create codec: %v", err))
	}
	// make a record
	originalRecord, err := makeOuterRecord()
	if err != nil {
		panic(fmt.Errorf("cannot make outer record: %v", err))
	}
	// encode record
	bb := new(bytes.Buffer)
	err = c.Encode(bb, originalRecord)
	if err != nil {
		panic(fmt.Errorf("cannot encode record: %v", err))
	}
	// decode bytes
	decoded, err := c.Decode(bytes.NewReader(bb.Bytes()))
	if err != nil {
		panic(fmt.Errorf("cannot decode record: %v", err))
	}
	decodedRecord, ok := decoded.(*goavro.Record)
	if !ok {
		panic("expected *goavro.Record")
	}
	decodedValue, err := decodedRecord.Get("value")
	if err != nil {
		panic(err)
	}
	if decodedValue != int32(3) {
		fmt.Printf("Actual: %#v; Expected: %#v\n", decodedValue, int32(3))
	}
	fmt.Printf("Read a value: %d\n", decodedValue)
	rec, err := decodedRecord.Get("rec")
	if err != nil {
		panic(err)
	}
	decodedArray := rec.([]interface{})
	if len(decodedArray) != 2 {
		fmt.Printf("Actual: %#v; Expected: %#v\n", len(decodedArray), 2)
	}
	for index, decodedSubRecord := range decodedArray {
		r := decodedSubRecord.(*goavro.Record)
		sv, err := r.Get("stringValue")
		if err != nil {
			panic(err)
		}
		iv, err := r.Get("intValue")
		if err != nil {
			panic(err)
		}
		fmt.Printf("Read a subrecord %d string value: %s\n", index, sv)
		fmt.Printf("Read a subrecord %d int value: %d\n", index, iv)
	}
}
