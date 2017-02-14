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

package goavro

import (
	"encoding/json"
	"fmt"
	"io"
)

func jsonEncode(w io.Writer, datum interface{}) error {
	// Use json marshal because the json encoder inserts a new line at the end.
	b, err := json.Marshal(datum)
	if err != nil {
		return err
	}
	n, err := w.Write(b)
	if n < len(b) {
		return fmt.Errorf("JSON encode error only wrote %v(%v) bytes", n, len(b))
	}
	return nil
}

func newJSONEncoder(goType string) encoderFunction {
	return func(w io.Writer, datum interface{}) error {
		return jsonEncode(w, datum)
	}
}

func nullJSONEncoder(w io.Writer, datum interface{}) error {
	return newJSONEncoder("nil")(w, datum)
}

func booleanJSONEncoder(w io.Writer, datum interface{}) error {
	return newJSONEncoder("bool")(w, datum)
}

func intJSONEncoder(w io.Writer, datum interface{}) error {
	someNumber, ok := datum.(int32)
	if !ok {
		return newEncoderError("int", "expected: int32 received %T", datum)
	}
	return newJSONEncoder("int32")(w, someNumber)
}

func longJSONEncoder(w io.Writer, datum interface{}) error {
	someNumber, ok := datum.(int64)
	if !ok {
		return newEncoderError("long", "expected: int64 received %T", datum)
	}
	return newJSONEncoder("int64")(w, someNumber)
}

func floatJSONEncoder(w io.Writer, datum interface{}) error {
	someNumber, ok := datum.(float32)
	if !ok {
		return newEncoderError("float", "expected: float32 received %T", datum)
	}
	return newJSONEncoder("float32")(w, someNumber)
}

func doubleJSONEncoder(w io.Writer, datum interface{}) error {
	someNumber, ok := datum.(float64)
	if !ok {
		return newEncoderError("float", "expected: float64 received %T", datum)
	}
	return newJSONEncoder("float64")(w, someNumber)
}

func bytesJSONEncoder(w io.Writer, datum interface{}) error {
	someBytes, ok := datum.([]byte)
	if !ok {
		return newEncoderError("bytes", "expected: []byte received %T", datum)
	}
	return newJSONEncoder("[]uint8")(w, string(someBytes))
}

func stringJSONEncoder(w io.Writer, datum interface{}) error {
	someString, ok := datum.(string)
	if !ok {
		return newEncoderError("string", "expected: string; received: %T", datum)
	}
	return newJSONEncoder("string")(w, someString)
}
