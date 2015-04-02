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
	"fmt"
	"github.com/linkedin/goavro"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"
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
      "doc": "Unix epoch time"
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
      "doc": "Unix epoch time in nanoseconds",
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
	pr, pw, err := os.Pipe()
	if err != nil {
		log.Fatal(err)
	}

	go dumpWriter(pw, codec)
	dumpReader(pr)
}

func dumpWriter(w io.Writer, codec goavro.Codec) {
	fw, err := codec.NewWriter(
		goavro.BlockSize(5),             // queue up no more than 5 items
		goavro.BlockTick(3*time.Second), // but flush at least every 3 seconds
		goavro.Compression(goavro.CompressionDeflate),
		goavro.ToWriter(w))
	if err != nil {
		log.Fatal(err)
	}
	defer fw.Close()

	sigs := make(chan os.Signal)
	signal.Notify(sigs)
	defer func() {
		signal.Stop(sigs)
	}()

writeLoop:
	for {
		select {
		case <-time.After(time.Duration(rand.Intn(500)) * time.Millisecond):
			sendRecord(fw)
		case <-sigs:
			break writeLoop
		}
	}
}

func sendRecord(fw *goavro.Writer) {
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
	outerRecord.Set("timestamp", int64(time.Now().UnixNano()))
	fw.Write(outerRecord)
}

func dumpReader(r io.Reader) {
	fr, err := goavro.NewReader(goavro.FromReader(r))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := fr.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	for fr.Scan() {
		datum, err := fr.Read()
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println(datum)
	}
}
