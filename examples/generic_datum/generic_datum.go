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
	"log"
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

func main() {
	innerRecords := make([]interface{}, 0)
	// make first inner record
	innerRecord, err := goavro.NewRecord(goavro.RecordSchema(innerSchema))
	if err != nil {
		log.Fatalf("cannot create innerRecord: %v", err)
	}
	if err = innerRecord.Set("stringValue", "Hello"); err != nil {
		log.Fatal(err)
	}
	if err = innerRecord.Set("intValue", int32(1)); err != nil {
		log.Fatal(err)
	}
	innerRecords = append(innerRecords, innerRecord)
	// make another inner record
	innerRecord, _ = goavro.NewRecord(goavro.RecordSchema(innerSchema))
	innerRecord.Set("stringValue", "World")
	innerRecord.Set("intValue", int32(2))
	innerRecords = append(innerRecords, innerRecord)
	// make outer record
	outerRecord, err := goavro.NewRecord(goavro.RecordSchema(outerSchema))
	if err != nil {
		log.Fatalf("cannot create outerRecord: %v", err)
	}
	outerRecord.Set("value", int32(3))
	outerRecord.Set("rec", innerRecords)
	// make a codec
	c, err := goavro.NewCodec(outerSchema)
	if err != nil {
		log.Fatal(err)
	}
	// encode outerRecord to io.Writer (here, a bytes.Buffer)
	bb := new(bytes.Buffer)
	err = c.Encode(bb, outerRecord)
	if err != nil {
		log.Fatal(err)
	}
	// decode bytes
	decoded, err := c.Decode(bytes.NewReader(bb.Bytes()))
	if err != nil {
		log.Fatal(err)
	}
	decodedRecord, ok := decoded.(*goavro.Record)
	if !ok {
		log.Fatalf("expected *goavro.Record; received: %T", decoded)
	}
	decodedValue, err := decodedRecord.Get("value")
	if err != nil {
		log.Fatal(err)
	}
	if decodedValue != int32(3) {
		log.Printf("Actual: %#v; Expected: %#v\n", decodedValue, int32(3))
	}
	fmt.Printf("Read a value: %d\n", decodedValue)
	rec, err := decodedRecord.Get("rec")
	if err != nil {
		log.Fatal(err)
	}
	decodedArray := rec.([]interface{})
	if len(decodedArray) != 2 {
		log.Fatalf("Actual: %#v; Expected: %#v\n", len(decodedArray), 2)
	}
	for index, decodedSubRecord := range decodedArray {
		r := decodedSubRecord.(*goavro.Record)
		sv, err := r.Get("stringValue")
		if err != nil {
			log.Fatal(err)
		}
		iv, err := r.Get("intValue")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Read a subrecord %d string value: %s\n", index, sv)
		fmt.Printf("Read a subrecord %d int value: %d\n", index, iv)
	}
}
