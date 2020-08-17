// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"errors"
	"fmt"
	"math"
	"math/big"
)

// decimal
//
// The decimal logical type represents an arbitrary-precision signed decimal
// number of the form unscaled × 10-scale.
//
// A decimal logical type annotates Avro bytes or fixed types. The byte array
// must contain the two's-complement representation of the unscaled integer
// value in big-endian byte order. The scale is fixed, and is specified using an
// attribute.
//
// The following attributes are supported:
//
// scale, a JSON integer representing the scale (optional). If not specified the
// scale is 0.  precision, a JSON integer representing the (maximum) precision
// of decimals stored in this type (required).  For example, the following
// schema represents decimal numbers with a maximum precision of 4 and a scale
// of 2:
//
// {
//   "type": "bytes",
//   "logicalType": "decimal",
//   "precision": 4,
//   "scale": 2
// }
//
// Precision must be a positive integer greater than zero. If the underlying
// type is a fixed, then the precision is limited by its size. An array of
// length n can store at most floor(log_10(28 × n - 1 - 1)) base-10 digits of
// precision.
//
// Scale must be zero or a positive integer less than or equal to the precision.
//
// For the purposes of schema resolution, two schemas that are decimal logical
// types match if their scales and precisions match.

/////////////////////////////////////////////////////////////////////////////////////////////
// decimal logical-type - byte/fixed - to/from math/big.Rat
// two's complement algorithm taken from:
// https://groups.google.com/d/msg/golang-nuts/TV4bRVrHZUw/UcQt7S4IYlcJ by rog
/////////////////////////////////////////////////////////////////////////////////////////////

type makeCodecFn func(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error)

func precisionAndScaleFromSchemaMap(schemaMap map[string]interface{}) (int, int, error) {
	p1, ok := schemaMap["precision"]
	if !ok {
		return 0, 0, errors.New("cannot create decimal logical type without precision")
	}
	p2, ok := p1.(float64)
	if !ok {
		return 0, 0, fmt.Errorf("cannot create decimal logical type with wrong precision type; expected: float64; received: %T", p1)
	}
	p3 := int(p2)
	if p3 <= 1 {
		return 0, 0, fmt.Errorf("cannot create decimal logical type when precision is less than one: %d", p3)
	}
	var s3 int // scale defaults to 0 if not set
	if s1, ok := schemaMap["scale"]; ok {
		s2, ok := s1.(float64)
		if !ok {
			return 0, 0, fmt.Errorf("cannot create decimal logical type with wrong precision type; expected: float64; received: %T", p1)
		}
		s3 = int(s2)
		if s3 < 0 {
			return 0, 0, fmt.Errorf("cannot create decimal logical type when scale is less than zero: %d", s3)
		}
		if s3 > p3 {
			// FIXME This violates: "If a logical type is invalid, for example a
			// decimal with scale greater than its precision, then
			// implementations should ignore the logical type and use the
			// underlying Avro type."
			return 0, 0, fmt.Errorf("cannot create decimal logical type when scale is larger than precision: %d > %d", s3, p3)
		}
	}
	return p3, s3, nil
}

var one = big.NewInt(1)

func makeDecimalBytesCodec(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	precision, scale, err := precisionAndScaleFromSchemaMap(schemaMap)
	if err != nil {
		return nil, err
	}
	if _, ok := schemaMap["name"]; !ok {
		schemaMap["name"] = "bytes.decimal"
	}
	c, err := registerNewCodec(st, schemaMap, enclosingNamespace)
	if err != nil {
		return nil, fmt.Errorf("Bytes ought to have valid name: %s", err)
	}
	c.binaryFromNative = decimalBytesFromNative(bytesBinaryFromNative, toSignedBytes, precision, scale)
	c.textualFromNative = decimalBytesFromNative(bytesTextualFromNative, toSignedBytes, precision, scale)
	c.nativeFromBinary = nativeFromDecimalBytes(bytesNativeFromBinary, precision, scale)
	c.nativeFromTextual = nativeFromDecimalBytes(bytesNativeFromTextual, precision, scale)
	return c, nil
}

func nativeFromDecimalBytes(fn toNativeFn, precision, scale int) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		d, b, err := fn(bytes)
		if err != nil {
			return d, b, err
		}
		bs, ok := d.([]byte)
		if !ok {
			return nil, bytes, fmt.Errorf("cannot transform to native decimal, expected []byte, received %T", d)
		}
		i := big.NewInt(0)
		fromSignedBytes(i, bs)
		if i.BitLen() > 64 {
			// Avro spec specifies we return underlying type if the logicalType is invalid
			return d, b, err
		}
		r := big.NewRat(i.Int64(), int64(math.Pow10(scale)))
		return r, b, nil
	}
}

func decimalBytesFromNative(fromNativeFn fromNativeFn, toBytesFn toBytesFn, precision, scale int) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		r, ok := d.(*big.Rat)
		if !ok {
			return nil, fmt.Errorf("cannot transform to bytes, expected *big.Rat, received %T", d)
		}
		// we reduce accuracy to precision by dividing and multiplying by digit length
		num := big.NewInt(0).Set(r.Num())
		denom := big.NewInt(0).Set(r.Denom())

		// we get the scaled decimal representation
		i := new(big.Int).Mul(num, big.NewInt(int64(math.Pow10(scale))))
		// divide that by the denominator
		precnum := new(big.Int).Div(i, denom)
		bout, err := toBytesFn(precnum)
		if err != nil {
			return nil, err
		}
		return fromNativeFn(b, bout)
	}
}

func makeDecimalFixedCodec(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	precision, scale, err := precisionAndScaleFromSchemaMap(schemaMap)
	if err != nil {
		return nil, err
	}
	if _, ok := schemaMap["name"]; !ok {
		schemaMap["name"] = "fixed.decimal"
	}
	c, err := makeFixedCodec(st, enclosingNamespace, schemaMap)
	if err != nil {
		return nil, err
	}
	size, err := sizeFromSchemaMap(c.typeName, schemaMap)
	if err != nil {
		return nil, err
	}
	c.binaryFromNative = decimalBytesFromNative(c.binaryFromNative, toSignedFixedBytes(size), precision, scale)
	c.textualFromNative = decimalBytesFromNative(c.textualFromNative, toSignedFixedBytes(size), precision, scale)
	c.nativeFromBinary = nativeFromDecimalBytes(c.nativeFromBinary, precision, scale)
	c.nativeFromTextual = nativeFromDecimalBytes(c.nativeFromTextual, precision, scale)
	return c, nil
}

func padBytes(bytes []byte, fixedSize uint) []byte {
	s := int(fixedSize)
	padded := make([]byte, s, s)
	if s >= len(bytes) {
		copy(padded[s-len(bytes):], bytes)
	}
	return padded
}

type toBytesFn func(n *big.Int) ([]byte, error)

// fromSignedBytes sets the value of n to the big-endian two's complement
// value stored in the given data. If data[0]&80 != 0, the number
// is negative. If data is empty, the result will be 0.
func fromSignedBytes(n *big.Int, data []byte) {
	n.SetBytes(data)
	if len(data) > 0 && data[0]&0x80 > 0 {
		n.Sub(n, new(big.Int).Lsh(one, uint(len(data))*8))
	}
}

// toSignedBytes returns the big-endian two's complement
// form of n.
func toSignedBytes(n *big.Int) ([]byte, error) {
	switch n.Sign() {
	case 0:
		return []byte{0}, nil
	case 1:
		b := n.Bytes()
		if b[0]&0x80 > 0 {
			b = append([]byte{0}, b...)
		}
		return b, nil
	case -1:
		length := uint(n.BitLen()/8+1) * 8
		b := new(big.Int).Add(n, new(big.Int).Lsh(one, length)).Bytes()
		// When the most significant bit is on a byte
		// boundary, we can get some extra significant
		// bits, so strip them off when that happens.
		if len(b) >= 2 && b[0] == 0xff && b[1]&0x80 != 0 {
			b = b[1:]
		}
		return b, nil
	}
	return nil, fmt.Errorf("toSignedBytes: error big.Int.Sign() returned unexpected value")
}

// toSignedFixedBytes returns the big-endian two's complement
// form of n for a given length of bytes.
func toSignedFixedBytes(size uint) func(*big.Int) ([]byte, error) {
	return func(n *big.Int) ([]byte, error) {
		switch n.Sign() {
		case 0:
			return []byte{0}, nil
		case 1:
			b := n.Bytes()
			if b[0]&0x80 > 0 {
				b = append([]byte{0}, b...)
			}
			return padBytes(b, size), nil
		case -1:
			length := size * 8
			b := new(big.Int).Add(n, new(big.Int).Lsh(one, length)).Bytes()
			// Unlike a variable length byte length we need the extra bits to meet byte length
			return b, nil
		}
		return nil, fmt.Errorf("toSignedBytes: error big.Int.Sign() returned unexpected value")
	}
}
