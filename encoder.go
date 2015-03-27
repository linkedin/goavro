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
	"fmt"
	"io"
	"math"
)

// ErrEncoder is returned when the encoder encounters an error.
type ErrEncoder struct {
	Message string
	Err     error
}

func (e ErrEncoder) Error() string {
	if e.Err == nil {
		return "cannot encode " + e.Message
	}
	return "cannot encode " + e.Message + ": " + e.Err.Error()
}

func newEncoderError(dataType string, a ...interface{}) *ErrEncoder {
	var err error
	var format, message string
	var ok bool
	if len(a) == 0 {
		return &ErrEncoder{dataType + ": no reason given", nil}
	}
	// if last item is error: save it
	if err, ok = a[len(a)-1].(error); ok {
		a = a[:len(a)-1] // pop it
	}
	// if items left, first ought to be format string
	if len(a) > 0 {
		if format, ok = a[0].(string); ok {
			a = a[1:] // unshift
			message = fmt.Sprintf(format, a...)
		}
	}
	if message != "" {
		message = ": " + message
	}
	return &ErrEncoder{dataType + message, err}
}

func nullEncoder(_ io.Writer, _ interface{}) error {
	return nil
}

func booleanEncoder(w io.Writer, datum interface{}) error {
	someBoolean, ok := datum.(bool)
	if !ok {
		return newEncoderError("boolean", "expected: bool; received: %T", datum)
	}
	bb := make([]byte, 1)
	if someBoolean {
		bb[0] = byte(1)
	}
	if _, err := w.Write(bb); err != nil {
		return newEncoderError("boolean", err)
	}
	return nil
}

func intEncoder(w io.Writer, datum interface{}) error {
	downShift := uint32(31)
	someInt, ok := datum.(int32)
	if !ok {
		return newEncoderError("int", "expected: int32; received: %T", datum)
	}
	encoded := int64((someInt << 1) ^ (someInt >> downShift))
	bb := make([]byte, 0)
	if encoded == 0 {
		bb = append(bb, byte(0))
	} else {
		for encoded > 0 {
			b := byte(encoded & 127)
			encoded = encoded >> 7
			if !(encoded == 0) {
				b |= 128
			}
			bb = append(bb, b)
		}
	}
	_, err := w.Write(bb)
	return err
}

func longEncoder(w io.Writer, datum interface{}) error {
	downShift := uint32(63)
	someInt, ok := datum.(int64)
	if !ok {
		return newEncoderError("long", "expected: int64; received: %T", datum)
	}
	encoded := int64((someInt << 1) ^ (someInt >> downShift))
	bb := make([]byte, 0)
	if encoded == 0 {
		bb = append(bb, byte(0))
	} else {
		for encoded > 0 {
			b := byte(encoded & 127)
			encoded = encoded >> 7
			if !(encoded == 0) {
				b |= 128
			}
			bb = append(bb, b)
		}
	}
	_, err := w.Write(bb)
	return err
}

func floatEncoder(w io.Writer, datum interface{}) error {
	someFloat, ok := datum.(float32)
	if !ok {
		return newEncoderError("float", "expected: float32; received: %T", datum)
	}
	bits := uint64(math.Float32bits(someFloat))
	const byteCount = 4
	buf := make([]byte, byteCount)
	for i := 0; i < byteCount; i++ {
		buf[i] = byte(bits & 255)
		bits = bits >> 8
	}
	_, err := w.Write(buf)
	return err
}

func doubleEncoder(w io.Writer, datum interface{}) error {
	someFloat, ok := datum.(float64)
	if !ok {
		return newEncoderError("double", "expected: float64; received: %T", datum)
	}
	bits := uint64(math.Float64bits(someFloat))
	const byteCount = 8
	buf := make([]byte, byteCount)
	for i := 0; i < byteCount; i++ {
		buf[i] = byte(bits & 255)
		bits = bits >> 8
	}
	_, err := w.Write(buf)
	return err
}

func bytesEncoder(w io.Writer, datum interface{}) error {
	someBytes, ok := datum.([]byte)
	if !ok {
		return newEncoderError("bytes", "expected: []byte; received: %T", datum)
	}
	err := longEncoder(w, int64(len(someBytes)))
	if err != nil {
		return newEncoderError("bytes", err)
	}
	_, err = w.Write(someBytes)
	return err
}

func stringEncoder(w io.Writer, datum interface{}) error {
	someString, ok := datum.(string)
	if !ok {
		return newEncoderError("string", "expected: string; received: %T", datum)
	}
	err := longEncoder(w, int64(len(someString)))
	if err != nil {
		return newEncoderError("string", err)
	}
	_, err = w.Write([]byte(someString))
	return err
}
