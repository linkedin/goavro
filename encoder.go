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

func nullEncoder(_ io.Writer, _ interface{}) error {
	return nil
}

func booleanEncoder(w io.Writer, datum interface{}) error {
	someBoolean, ok := datum.(bool)
	if !ok {
		return fmt.Errorf("expected: boolean; actual: %T", datum)
	}
	bb := make([]byte, 1)
	if someBoolean {
		bb[0] = byte(1)
	}
	_, err := w.Write(bb)
	if err != nil {
		return fmt.Errorf("cannot write boolean: %v", err)
	}
	return nil
}

func intEncoder(w io.Writer, datum interface{}) error {
	downShift := uint32(31)
	someInt, ok := datum.(int32)
	if !ok {
		return fmt.Errorf("cannot encode int: expected: int32; actual: %T", datum)
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
		return fmt.Errorf("cannot encode long: expected: int64; actual: %T", datum)
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
		return fmt.Errorf("cannot encode float: expected: float32; actual: %T", datum)
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
		return fmt.Errorf("cannot encode double: expected: float64; actual: %T", datum)
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
		return fmt.Errorf("cannot encode bytes: expected: []byte; actual: %T", datum)
	}
	err := longEncoder(w, int64(len(someBytes)))
	if err != nil {
		return fmt.Errorf("cannot encode bytes: %v", err)
	}
	_, err = w.Write(someBytes)
	return err
}

func stringEncoder(w io.Writer, datum interface{}) error {
	someString, ok := datum.(string)
	if !ok {
		return fmt.Errorf("cannot encode string: expected: string; actual: %T", datum)
	}
	err := longEncoder(w, int64(len(someString)))
	if err != nil {
		return fmt.Errorf("cannot encode string: %v", err)
	}
	_, err = w.Write([]byte(someString))
	return err
}
