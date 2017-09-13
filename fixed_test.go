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
	"testing"
)

func TestSchemaFixed(t *testing.T) {
	testSchemaValid(t, `{"type": "fixed", "size": 16, "name": "md5"}`)
}

func TestFixedName(t *testing.T) {
	testSchemaInvalid(t, `{"type":"fixed","size":16}`, "Fixed ought to have valid name: schema ought to have name key")
	testSchemaInvalid(t, `{"type":"fixed","name":3}`, "Fixed ought to have valid name: schema name ought to be non-empty string")
	testSchemaInvalid(t, `{"type":"fixed","name":""}`, "Fixed ought to have valid name: schema name ought to be non-empty string")
	testSchemaInvalid(t, `{"type":"fixed","name":"&foo","size":16}`, "Fixed ought to have valid name: schema name ought to start with")
	testSchemaInvalid(t, `{"type":"fixed","name":"foo&","size":16}`, "Fixed ought to have valid name: schema name ought to have second and remaining")
}

func TestFixedSize(t *testing.T) {
	testSchemaInvalid(t, `{"type":"fixed","name":"f1"}`, `Fixed "f1" ought to have size key`)
	testSchemaInvalid(t, `{"type":"fixed","name":"f1","size":"16"}`, `Fixed "f1" size ought to be number greater than zero`)
	testSchemaInvalid(t, `{"type":"fixed","name":"f1","size":-1}`, `Fixed "f1" size ought to be number greater than zero`)
	testSchemaInvalid(t, `{"type":"fixed","name":"f1","size":0}`, `Fixed "f1" size ought to be number greater than zero`)
}

func TestFixedDecodeBufferUnderflow(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"fixed","name":"md5","size":16}`, nil, "short buffer")
}

func TestFixedDecodeWithExtra(t *testing.T) {
	c, err := NewCodec(`{"type":"fixed","name":"foo","size":4}`)
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	val, buf, err := c.NativeFromBinary([]byte("abcdefgh"))
	if actual, expected := string(val.([]byte)), "abcd"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if actual, expected := string(buf), "efgh"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
}

func TestFixedEncodeUnsupportedType(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, `{"type":"fixed","name":"foo","size":4}`, 13)
}

func TestFixedEncodeWrongSize(t *testing.T) {
	testBinaryEncodeFail(t, `{"type":"fixed","name":"foo","size":4}`, []byte("abcde"), "datum size ought to equal schema size")
	testBinaryEncodeFail(t, `{"type":"fixed","name":"foo","size":4}`, []byte("abc"), "datum size ought to equal schema size")
}

func TestFixedEncode(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"fixed","name":"foo","size":4}`, []byte("abcd"), []byte("abcd"))
}

func TestFixedTextCodec(t *testing.T) {
	schema := `{"type":"fixed","name":"f1","size":4}`
	testTextDecodeFail(t, schema, []byte(`"\u0001\u0002\u0003"`), "datum size ought to equal schema size")
	testTextDecodeFail(t, schema, []byte(`"\u0001\u0002\u0003\u0004\u0005"`), "datum size ought to equal schema size")
	testTextEncodeFail(t, schema, []byte{1, 2, 3}, "datum size ought to equal schema size")
	testTextEncodeFail(t, schema, []byte{1, 2, 3, 4, 5}, "datum size ought to equal schema size")
	testTextEncodePass(t, schema, []byte{1, 2, 3, 4}, []byte(`"\u0001\u0002\u0003\u0004"`))
}
