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
	"log"

	"github.com/linkedin/goavro"
)

const schema = `{
  "type": "record",
  "name": "wordList",
  "namespace": "com.example",
  "doc:": "List of words",
  "fields": [
    {
      "type": {
        "items": "string",
        "type": "array"
      },
      "name": "words"
    }
  ]
}
`

var codec goavro.Codec

func init() {
	var err error
	// If you want speed, create the codec one time for each
	// schema and reuse it to create multiple Writer instances.
	codec, err = goavro.NewCodec(schema)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	// Simple array's do not need to be enclosed in goavro.NewRecord
	// but only passed in a slice of interface{}
	innerRecords := []interface{}{
		"Hello",
		"World",
	}

	// Simply add the array directly to the record
	record, err := goavro.NewRecord(goavro.RecordSchema(schema))
	if err != nil {
		log.Fatal(err)
	}
	record.Set("words", innerRecords)

	// Encode the record into a bytes.Buffer
	bb := new(bytes.Buffer)
	if err = codec.Encode(bb, record); err != nil {
		log.Fatal(err)
	}

	// Compare encoded bytes against the expected bytes.
	actual := bb.Bytes()
	expected := []byte(
		"\x04" + // array of two elements
			"\x0aHello" + // first element
			"\x0aWorld\x00") // second element
	if bytes.Compare(actual, expected) != 0 {
		log.Printf("Actual: %#v; Expected: %#v", actual, expected)
	}

	// Let's decode the blob and print the output in JSON format
	// using goavro.Record's String() method.
	decoded, err := codec.Decode(bytes.NewReader(actual))
	fmt.Println(decoded)
	// we only need to perform type assertion if we want to access inside
	result := decoded.(*goavro.Record)
	fmt.Println("Record Name:", result.Name)
	fmt.Println("Record Fields:")
	for i, field := range result.Fields {
		fmt.Println(" field", i, field.Name, ":", field.Datum)
	}
}
