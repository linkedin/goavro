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
	"os"
)

var (
	schema string
	codec  goavro.Codec
)

func init() {
	schema = `
{
  "type" : "record",
  "name" : "Weather",
  "namespace" : "test",
  "doc" : "A weather reading.",
  "fields" : [ {
    "name" : "station",
    "type" : "string"
  }, {
    "name" : "time",
    "type" : "long"
  }, {
    "name" : "temp",
    "type" : "int"
  } ]
}
`

	var err error
	// If you want speed, create the codec one time for each
	// schema and reuse it to create multiple Writer instances.
	codec, err = goavro.NewCodec(schema)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	switch len(os.Args) {
	case 1:
		dumpWriter(os.Stdout, codec)
	case 2:
		fh, err := os.Create(os.Args[1])
		if err != nil {
			log.Fatal(err)
		}
		dumpWriter(fh, codec)
		fh.Close()
	default:
		fmt.Fprintf(os.Stderr, "usage: %s [filename]\n", os.Args[0])
		os.Exit(2)
	}
}

func dumpWriter(w io.Writer, codec goavro.Codec) {
	fw, err := codec.NewWriter(
		// goavro.Compression(goavro.CompressionDeflate),
		goavro.Compression(goavro.CompressionSnappy),
		goavro.ToWriter(w))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := fw.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	raw := []map[string]interface{}{
		{"station": "011990-99999", "time": int64(-619524000000), "temp": int32(0)},
		{"station": "011990-99999", "time": int64(-619506000000), "temp": int32(22)},
		{"station": "011990-99999", "time": int64(-619484400000), "temp": int32(-11)},
		{"station": "012650-99999", "time": int64(-655531200000), "temp": int32(111)},
		{"station": "012650-99999", "time": int64(-655509600000), "temp": int32(78)},
	}
	for _, rec := range raw {
		record, err := goavro.NewRecord(goavro.RecordSchema(schema))
		if err != nil {
			log.Fatal(err)
		}
		for k, v := range rec {
			record.Set(k, v)
		}
		fw.Write(record)
	}
}
