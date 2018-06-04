// Copyright [2017] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"fmt"
	"strconv"
)

// Fixed does not have child objects, therefore whatever namespace it defines is
// just to store its name in the symbol table.
func makeFixedCodec(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	c, err := registerNewCodec(st, schemaMap, enclosingNamespace)
	if err != nil {
		return nil, fmt.Errorf("Fixed ought to have valid name: %s", err)
	}
	// Fixed type must have size
	sizeRaw, ok := schemaMap["size"]
	if !ok {
		return nil, fmt.Errorf("Fixed %q ought to have size key", c.typeName)
	}
	var size uint
	switch val := sizeRaw.(type) {
	case string:
		s, err := strconv.ParseUint(val, 10, 0)
		if err != nil {
			return nil, fmt.Errorf("Fixed %q size ought to be number greater than zero: %v", c.typeName, sizeRaw)
		}
		size = uint(s)
	case float64:
		if val <= 0 {
			return nil, fmt.Errorf("Fixed %q size ought to be number greater than zero: %v", c.typeName, sizeRaw)
		}
		size = uint(val)
	default:
		return nil, fmt.Errorf("Fixed %q size ought to be number greater than zero: %v", c.typeName, sizeRaw)
	}

	c.nativeFromBinary = func(buf []byte) (interface{}, []byte, error) {
		if buflen := uint(len(buf)); size > buflen {
			return nil, nil, fmt.Errorf("cannot decode binary fixed %q: schema size exceeds remaining buffer size: %d > %d (short buffer)", c.typeName, size, buflen)
		}
		return buf[:size], buf[size:], nil
	}

	c.binaryFromNative = func(buf []byte, datum interface{}) ([]byte, error) {
		var (
			someBytes []byte
			ok        = true
		)
		switch datum.(type) {
		case []byte:
			someBytes = datum.([]byte)
		case string:
			someBytes = []byte(datum.(string))
		default:
			ok = false
		}
		if !ok {
			return nil, fmt.Errorf("cannot encode binary fixed %q: expected []byte; received: %T", c.typeName, datum)
		}
		if count := uint(len(someBytes)); count != size {
			return nil, fmt.Errorf("cannot encode binary fixed %q: datum size ought to equal schema size: %d != %d", c.typeName, count, size)
		}
		return append(buf, someBytes...), nil
	}

	c.nativeFromTextual = func(buf []byte) (interface{}, []byte, error) {
		if buflen := uint(len(buf)); size > buflen {
			return nil, nil, fmt.Errorf("cannot decode textual fixed %q: schema size exceeds remaining buffer size: %d > %d (short buffer)", c.typeName, size, buflen)
		}
		var datum interface{}
		var err error
		datum, buf, err = bytesNativeFromTextual(buf)
		if err != nil {
			return nil, buf, err
		}
		datumBytes := datum.([]byte)
		if count := uint(len(datumBytes)); count != size {
			return nil, nil, fmt.Errorf("cannot decode textual fixed %q: datum size ought to equal schema size: %d != %d", c.typeName, count, size)
		}
		return datum, buf, err
	}

	c.textualFromNative = func(buf []byte, datum interface{}) ([]byte, error) {
		someBytes, ok := datum.([]byte)
		if !ok {
			return nil, fmt.Errorf("cannot encode textual fixed %q: expected []byte; received: %T", c.typeName, datum)
		}
		if count := uint(len(someBytes)); count != size {
			return nil, fmt.Errorf("cannot encode textual fixed %q: datum size ought to equal schema size: %d != %d", c.typeName, count, size)
		}
		return bytesTextualFromNative(buf, someBytes)
	}

	return c, nil
}
