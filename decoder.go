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
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// ErrDecoder is returned when the encoder encounters an error.
type ErrDecoder struct {
	Message string
	Err     error
}

func (e ErrDecoder) Error() string {
	if e.Err == nil {
		return "cannot decode " + e.Message
	} else {
		return "cannot decode " + e.Message + ": " + e.Err.Error()
	}
}

func newDecoderError(dataType string, a ...interface{}) *ErrDecoder {
	var err error
	var format, message string
	var ok bool
	if len(a) == 0 {
		return &ErrDecoder{dataType + ": no reason given", nil}
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
	return &ErrDecoder{dataType + message, err}
}

func nullDecoder(_ io.Reader) (interface{}, error) {
	return nil, nil
}

func booleanDecoder(r io.Reader) (interface{}, error) {
	bb := make([]byte, 1)
	if _, err := r.Read(bb); err != nil {
		return nil, newDecoderError("boolean", err)
	}
	var datum bool
	switch bb[0] {
	case byte(0):
		// zero value of boolean is false
	case byte(1):
		datum = true
	default:
		return nil, newDecoderError("boolean", "expected 1 or 0; received: %d", bb[0])
	}
	return datum, nil
}

func intDecoder(r io.Reader) (interface{}, error) {
	var v int
	var err error
	bb := make([]byte, 1)
	for shift := uint(0); ; shift += 7 {
		if _, err = r.Read(bb); err != nil {
			return nil, newDecoderError("int", err)
		}
		b := bb[0]
		v |= int(b&mask) << shift
		if b&flag == 0 {
			break
		}
	}
	datum := (int32(v>>1) ^ -int32(v&1))
	return datum, nil
}

func longDecoder(r io.Reader) (interface{}, error) {
	var v int
	var err error
	bb := make([]byte, 1)
	for shift := uint(0); ; shift += 7 {
		if _, err = r.Read(bb); err != nil {
			return nil, newDecoderError("long", err)
		}
		b := bb[0]
		v |= int(b&mask) << shift
		if b&flag == 0 {
			break
		}
	}
	datum := (int64(v>>1) ^ -int64(v&1))
	return datum, nil
}

func floatDecoder(r io.Reader) (interface{}, error) {
	buf := make([]byte, 4)
	if _, err := r.Read(buf); err != nil {
		return nil, newDecoderError("float", err)
	}
	bits := binary.LittleEndian.Uint32(buf)
	datum := math.Float32frombits(bits)
	return datum, nil
}

func doubleDecoder(r io.Reader) (interface{}, error) {
	buf := make([]byte, 8)
	if _, err := r.Read(buf); err != nil {
		return nil, newDecoderError("double", err)
	}
	datum := math.Float64frombits(binary.LittleEndian.Uint64(buf))
	return datum, nil
}

func bytesDecoder(r io.Reader) (interface{}, error) {
	someValue, err := longDecoder(r)
	if err != nil {
		return nil, newDecoderError("bytes", err)
	}
	size, ok := someValue.(int64)
	if !ok {
		return nil, newDecoderError("bytes", "expected int64; received: %T", someValue)
	}
	if size < 0 {
		return nil, newDecoderError("bytes", "negative length: %d", size)
	}
	buf := make([]byte, size)
	bytesRead, err := r.Read(buf)
	if err != nil {
		return nil, newDecoderError("bytes", err)
	}
	if int64(bytesRead) < size {
		return nil, newDecoderError("bytes", "buffer underrun")
	}
	return buf, nil
}

func stringDecoder(r io.Reader) (interface{}, error) {
	// NOTE: could have implemented in terms of makeBytesDecoder,
	// but prefer to not have nested error messages
	someValue, err := longDecoder(r)
	if err != nil {
		return nil, newDecoderError("string", err)
	}
	size, ok := someValue.(int64)
	if !ok {
		return nil, newDecoderError("string", "expected int64; received: %T", someValue)
	}
	if size < 0 {
		return nil, newDecoderError("string", "negative length: %d", size)
	}
	buf := make([]byte, size)
	byteCount, err := r.Read(buf)
	if err != nil {
		return nil, newDecoderError("string", err)
	}
	if int64(byteCount) < size {
		return nil, newDecoderError("string", "buffer underrun")
	}
	return string(buf), nil
}
