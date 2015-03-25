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
	"io"
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
	innerRecord1, err := goavro.NewRecord(goavro.RecordSchemaJson(innerSchema))
	if err != nil {
		return nil, err
	}
	innerRecord1.Fields[0].Datum = "Hello"
	innerRecord1.Fields[1].Datum = int32(1)
	innerRecords = append(innerRecords, innerRecord1)

	innerRecord2, err := goavro.NewRecord(goavro.RecordSchemaJson(innerSchema))
	if err != nil {
		return nil, err
	}
	innerRecord2.Fields[0].Datum = "World"
	innerRecord2.Fields[1].Datum = int32(2)
	innerRecords = append(innerRecords, innerRecord2)

	outerRecord, err := goavro.NewRecord(goavro.RecordSchemaJson(outerSchema))
	if err != nil {
		return nil, err
	}
	outerRecord.Fields[0].Datum = int32(3)
	outerRecord.Fields[1].Datum = innerRecords
	return outerRecord, nil
}

func encodeSomeRecord(c goavro.Codec, someRecord *goavro.Record) (*bytes.Buffer, error) {
	bb := new(bytes.Buffer)
	err := c.Encode(bb, someRecord)
	return bb, err
}

func decodeSomeRecord(c goavro.Codec, r io.Reader) (*goavro.Record, error) {
	something, err := c.Decode(r)
	if err != nil {
		return nil, err
	}
	return something.(*goavro.Record), nil
}

func main() {
	c, err := goavro.NewCodec(outerSchema)
	if err != nil {
		panic(fmt.Errorf("cannot create codec: %v", err))
	}
	originalRecord, err := makeOuterRecord()
	if err != nil {
		panic(err)
	}
	buf, err := encodeSomeRecord(c, originalRecord)
	if err != nil {
		panic(err)
	}
	decodedRecord, err := decodeSomeRecord(c, bytes.NewReader(buf.Bytes()))
	if err != nil {
		panic(err)
	}
	decodedValue := decodedRecord.Fields[0].Datum
	if decodedValue != int32(3) {
		fmt.Printf("Actual: %#v; Expected: %#v\n", decodedValue, int32(3))
	}
	fmt.Printf("Read a value: %d\n", decodedValue)
	decodedArray := decodedRecord.Fields[1].Datum.([]interface{})
	if len(decodedArray) != 2 {
		fmt.Printf("Actual: %#v; Expected: %#v\n", len(decodedArray), 2)
	}
	for index, decodedSubRecord := range decodedArray {
		r := decodedSubRecord.(*goavro.Record)
		fmt.Printf("Read a subrecord %d string value: %s\n", index, r.Fields[0].Datum)
		fmt.Printf("Read a subrecord %d int value: %d\n", index, r.Fields[1].Datum)
	}
}
