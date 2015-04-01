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

const innerSchema = `
{
  "type": "record",
  "name": "user",
  "namespace": "com.example",
  "doc": "User information",
  "fields": [
    {
      "type": "string",
      "name": "account",
      "doc": "The user's account name"
    },
    {
      "type": "long",
      "name": "creationDate",
      "doc": "Unix epoch time in milliseconds"
    }
  ]
}
`

var (
	outerSchema string
	codec       goavro.Codec
)

func init() {
	outerSchema = fmt.Sprintf(`
{
  "type": "record",
  "name": "comments",
  "doc:": "A basic schema for storing blog comments",
  "namespace": "com.example",
  "fields": [
    {
      "name": "user",
      "type": %s
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
`, innerSchema)

	var err error
	// If you want speed, create the codec one time for each
	// schema and reuse it to create multiple Writer instances.
	codec, err = goavro.NewCodec(outerSchema)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	// If we want to encode data, we need to put it in an actual
	// goavro.Record instance corresponding to the schema we wish
	// to encode against.
	//
	// NewRecord will create a goavro.Record instance
	// corresponding to the specified schema.
	innerRecord, err := goavro.NewRecord(goavro.RecordSchema(innerSchema))
	if err != nil {
		log.Fatal(err)
	}
	innerRecord.Set("account", "Aquaman")
	innerRecord.Set("creationDate", int64(1082196484))

	// We create both an innerRecord and an outerRecord.
	outerRecord, err := goavro.NewRecord(goavro.RecordSchema(outerSchema))
	if err != nil {
		log.Fatal(err)
	}
	// innerRecord is a completely seperate record instance from
	// outerRecord. Once we have an innerRecord instance it can be
	// assigned to the appropriate Datum item of the outerRecord.
	outerRecord.Set("user", innerRecord)
	// Other fields are set on the outerRecord.
	outerRecord.Set("comment", "The Atlantic is oddly cold this morning!")
	outerRecord.Set("timestamp", int64(1427255074))

	// Encode the outerRecord into a bytes.Buffer
	bb := new(bytes.Buffer)
	if err = codec.Encode(bb, outerRecord); err != nil {
		log.Fatal(err)
	}
	// Compare encoded bytes against the expected bytes.
	actual := bb.Bytes()
	expected := []byte(
		"\x0eAquaman" + // account
			"\x88\x88\x88\x88\x08" + // creationDate
			"\x50" + // 50 hex == 80 dec variable length integer encoded == 40 -> string is 40 characters long
			"The Atlantic is oddly cold this morning!" + // comment
			"\xc4\xbc\x91\xd1\x0a") // timestamp
	if bytes.Compare(actual, expected) != 0 {
		log.Printf("Actual: %#v; Expected: %#v", actual, expected)
	}
	// Let's decode the blob and print the output in JSON format
	// using goavro.Record's String() method.
	decoded, err := codec.Decode(bytes.NewReader(actual))
	fmt.Println(decoded)
	// we only need to perform type assertion if we want to access inside
	record := decoded.(*goavro.Record)
	fmt.Println("Record Name:", record.Name)
	fmt.Println("Record Fields:")
	for i, field := range record.Fields {
		fmt.Println(" field", i, field.Name, ":", field.Datum)
	}
}
