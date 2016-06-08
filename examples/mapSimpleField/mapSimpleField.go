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
  "name": "wordCount",
  "namespace": "com.example",
  "doc:": "Count of words",
  "fields": [
    {
      "type": {
        "values": "int",
        "type": "map"
      },
      "name": "counts"
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
	// Map's do not need to be enclosed in goavro.NewRecord
	// but only passed in as map[string]interface{}
	// According Avro specification the key must be a string but
	// the values can be any valid Avro type.
	// Simple types do not need to be added as goavro.Record but
	// Complex types do.
	// Type Reference: https://avro.apache.org/docs/current/spec.html
	innerRecords := map[string]interface{}{
		"Hello": int32(3),
		"World": int32(66),
	}

	// Simply add the map directly to the record
	record, err := goavro.NewRecord(goavro.RecordSchema(schema))
	if err != nil {
		log.Fatal(err)
	}
	record.Set("counts", innerRecords)

	// Encode the record into a bytes.Buffer
	bb := new(bytes.Buffer)
	if err = codec.Encode(bb, record); err != nil {
		log.Fatal(err)
	}

	// We cannot compare encoded bytes against the expected bytes
	// as we do in other examples due to Go's nondeterministic map ordering

	// Let's decode the blob and print the output in JSON format
	// using goavro.Record's String() method.
	actual := bb.Bytes()
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
