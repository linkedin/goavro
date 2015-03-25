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
	"github.com/linkedin/goavro"
	"log"
)

func main() {
	recordSchemaJson := `
{
  "type": "record",
  "name": "comments",
  "doc:": "A basic schema for storing blog comments",
  "namespace": "com.example",
  "fields": [
    {
      "doc": "Name of user",
      "type": "string",
      "name": "username"
    },
    {
      "doc": "The content of the user's message",
      "type": "string",
      "name": "comment"
    },
    {
      "doc": "Unix epoch time in milliseconds",
      "type": "long",
      "name": "timestamp"
    }
  ]
}
`
	someRecord, err := goavro.NewRecord(goavro.RecordSchemaJson(recordSchemaJson))
	if err != nil {
		log.Fatal(err)
	}
	someRecord.Fields[0].Datum = "Aquaman"
	someRecord.Fields[1].Datum = "The Atlantic is oddly cold this morning!"
	someRecord.Fields[2].Datum = int64(1082196484)

	codec, err := goavro.NewCodec(recordSchemaJson)
	if err != nil {
		log.Fatal(err)
	}

	bb := new(bytes.Buffer)
	if err = codec.Encode(bb, someRecord); err != nil {
		log.Fatal(err)
	}

	actual := bb.Bytes()
	expected := []byte("\x0eAquamanPThe Atlantic is oddly cold this morning!\x88\x88\x88\x88\x08")
	if bytes.Compare(actual, expected) != 0 {
		log.Printf("Actual: %#v; Expected: %#v", actual, expected)
	}
}
