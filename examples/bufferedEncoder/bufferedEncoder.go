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
	"bufio"
	"bytes"
	"github.com/linkedin/goavro"
	"io"
	"log"
)

func main() {
	bits, err := bufferedEncoder(`"string"`, "filibuster")
	if err != nil {
		log.Fatal(err)
	}
	expected := []byte("\x14filibuster")
	if bytes.Compare(bits, expected) != 0 {
		log.Fatalf("Actual: %#v; Expected: %#v", bits, expected)
	}
}

func bufferedEncoder(someSchemaJSON string, datum interface{}) (bits []byte, err error) {
	bb := new(bytes.Buffer)
	defer func() {
		bits = bb.Bytes()
	}()

	var c goavro.Codec
	c, err = goavro.NewCodec(someSchemaJSON)
	if err != nil {
		return
	}
	err = encodeWithBufferedWriter(c, bb, datum)
	return
}

func encodeWithBufferedWriter(c goavro.Codec, w io.Writer, datum interface{}) error {
	bw := bufio.NewWriter(w)
	err := c.Encode(bw, datum)
	if err != nil {
		return err
	}
	return bw.Flush()
}
