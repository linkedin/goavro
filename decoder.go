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

func nullDecoder(_ io.Reader) (interface{}, error) {
	return nil, nil
}

func booleanDecoder(r io.Reader) (interface{}, error) {
	bb := make([]byte, 1)
	_, err := r.Read(bb)
	if err != nil {
		return nil, fmt.Errorf("cannot decode boolean: %v", err)
	}
	var datum bool
	switch bb[0] {
	case byte(0):
		// zero value of boolean is false
	case byte(1):
		datum = true
	default:
		return nil, fmt.Errorf("cannot decode boolean: %x", bb[0])
	}
	return datum, nil
}

func intDecoder(r io.Reader) (interface{}, error) {
	var v int
	var err error
	bb := make([]byte, 1)
	for shift := uint(0); ; shift += 7 {
		_, err = r.Read(bb)
		if err != nil {
			return nil, fmt.Errorf("cannot decode int: %v", err)
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
		_, err = r.Read(bb)
		if err != nil {
			return nil, fmt.Errorf("cannot decode long: %v", err)
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
	_, err := r.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("cannot decode float: %v", err)
	}
	bits := binary.LittleEndian.Uint32(buf)
	datum := math.Float32frombits(bits)
	return datum, nil
}

func doubleDecoder(r io.Reader) (interface{}, error) {
	buf := make([]byte, 8)
	_, err := r.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("cannot decode double: %v", err)
	}
	datum := math.Float64frombits(binary.LittleEndian.Uint64(buf))
	return datum, nil
}

func bytesDecoder(r io.Reader) (interface{}, error) {
	someValue, err := longDecoder(r)
	if err != nil {
		return nil, fmt.Errorf("cannot decode bytes: %v", err)
	}
	size, ok := someValue.(int64)
	if !ok {
		return nil, fmt.Errorf("cannot decode bytes: expected int64; actual: %T", someValue)
	}
	if size < 0 {
		return nil, fmt.Errorf("cannot decode bytes: negative length: %d", size)
	}
	buf := make([]byte, size)
	bytes_read, err := r.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("cannot decode bytes: %v", err)
	}
	if int64(bytes_read) < size {
		return nil, fmt.Errorf("cannot decode bytes: buffer underrun")
	}
	return buf, nil
}

func stringDecoder(r io.Reader) (interface{}, error) {
	// NOTE: could have implemented in terms of makeBytesDecoder,
	// but prefer to not have nested error messages
	someValue, err := longDecoder(r)
	if err != nil {
		return nil, fmt.Errorf("cannot decode string: %v", err)
	}
	size, ok := someValue.(int64)
	if !ok {
		return nil, fmt.Errorf("cannot decode string: expected int64; actual: %T", someValue)
	}
	if size < 0 {
		return nil, fmt.Errorf("cannot decode string: negative length: %d", size)
	}
	buf := make([]byte, size)
	byteCount, err := r.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("cannot decode string: %v", err)
	}
	if int64(byteCount) < size {
		return nil, fmt.Errorf("cannot decode string: buffer underrun")
	}
	return string(buf), nil
}
