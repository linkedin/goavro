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
	"bytes"
	"testing"
)

func TestSchemaFixed(t *testing.T) {
	testSchemaValid(t, `{"type": "fixed", "size": 16, "name": "md5"}`)
	testSchemaValid(t, `{"type":"fixed","name":"f1","size":"16"}`)
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
	testSchemaInvalid(t, `{"type":"fixed","name":"f1","size":-1}`, `Fixed "f1" size ought to be number greater than zero`)
	testSchemaInvalid(t, `{"type":"fixed","name":"f1","size":0}`, `Fixed "f1" size ought to be number greater than zero`)
}

func TestFixedDecodeBufferUnderflow(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"fixed","name":"md5","size":16}`, nil, "short buffer")
}

func TestFixedDecodeWithExtra(t *testing.T) {
	c, err := NewCodec(`{"type":"fixed","name":"foo","size":4}`)
	if err != nil {
		t.Errorf("GOT: %#v; WANT: %#v", err, nil)
	}
	val, buf, err := c.NativeFromBinary([]byte("abcdefgh"))
	if actual, expected := string(val.([]byte)), "abcd"; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
	if actual, expected := string(buf), "efgh"; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
	if err != nil {
		t.Errorf("GOT: %#v; WANT: %#v", err, nil)
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

func TestFixedCodecAcceptsString(t *testing.T) {
	schema := `{"type":"fixed","name":"f1","size":4}`
	t.Run("binary", func(t *testing.T) {
		testBinaryEncodePass(t, schema, "abcd", []byte(`abcd`))
	})
	t.Run("text", func(t *testing.T) {
		testTextEncodePass(t, schema, "abcd", []byte(`"abcd"`))
	})
}

// TestFixedSlicesUseIndependentBackingArrays ensures that decoded fixed values
// do not share the same underlying array as the input buffer or other decoded
// fixed values. This prevents unexpected behavior when appending to decoded
// slices.
func TestFixedSlicesUseIndependentBackingArrays(t *testing.T) {
	schema := `{
  "name":"example",
  "type":"record",
  "fields":[
    {"name":"foo","type":{"type":"fixed","name":"fixed2","size":2}},
    {"name":"bar","type":"fixed2"}
  ]
}`

	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	// binary data contains {foo:[1,2],bar:[3,4]}
	binary := []byte{1, 2, 3, 4}
	gotNative, _, err := codec.NativeFromBinary(binary)
	if err != nil {
		t.Fatal(err)
	}

	got := gotNative.(map[string]interface{})
	foo := got["foo"].([]byte)
	bar := got["bar"].([]byte)

	if want := []byte{1, 2}; !bytes.Equal(foo, want) {
		t.Fatalf("expected foo to be %v, actually got %v", want, foo)
	}
	if want := []byte{3, 4}; !bytes.Equal(bar, want) {
		t.Fatalf("expected bar to be %v, actually got %v", want, bar)
	}

	// Appending to foo should not affect bar or binary because they should
	// use independent backing arrays
	foo = append(foo, 0, 0)

	if want := []byte{1, 2, 0, 0}; !bytes.Equal(foo, want) {
		t.Fatalf("expected foo to be %v, actually got %v", want, foo)
	}
	if want := []byte{3, 4}; !bytes.Equal(bar, want) {
		t.Errorf("expected bar to be %v, actually got %v", want, bar)
	}
	if want := []byte{1, 2, 3, 4}; !bytes.Equal(binary, want) {
		t.Errorf("expected binary to be %v, actually got %v", want, binary)
	}
}
