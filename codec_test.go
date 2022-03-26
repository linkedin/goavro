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
	"fmt"
	"os"
	"testing"
)

func ExampleCodec_CanonicalSchema() {
	schema := `{"type":"map","values":{"type":"enum","name":"foo","symbols":["alpha","bravo"]}}`
	codec, err := NewCodec(schema)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(codec.CanonicalSchema())
	}
	// Output: {"type":"map","values":{"name":"foo","type":"enum","symbols":["alpha","bravo"]}}
}

func TestCodecRabin(t *testing.T) {
	cases := []struct {
		Schema string
		Rabin  uint64
	}{
		{
			Schema: `"null"`,
			Rabin:  0x63dd24e7cc258f8a,
		},
		{
			Schema: `"boolean"`,
			Rabin:  0x9f42fc78a4d4f764,
		},
		{
			Schema: `"int"`,
			Rabin:  0x7275d51a3f395c8f,
		},
		{
			Schema: `"long"`,
			Rabin:  0xd054e14493f41db7,
		},
		{
			Schema: `"float"`,
			Rabin:  0x4d7c02cb3ea8d790,
		},
		{
			Schema: `"double"`,
			Rabin:  0x8e7535c032ab957e,
		},
		{
			Schema: `"bytes"`,
			Rabin:  0x4fc016dac3201965,
		},
		{
			Schema: `"string"`,
			Rabin:  0x8f014872634503c7,
		},
		{
			Schema: `[ "int"  ]`,
			Rabin:  0xb763638a48b2fb03,
		},
		{
			Schema: `[ "int" , {"type":"boolean"} ]`,
			Rabin:  0x4ad63578080c1602,
		},
		{
			Schema: `{"fields":[], "type":"record", "name":"foo"}`,
			Rabin:  0xbd0c50c84319be7e,
		},
		{
			Schema: `{"fields":[], "type":"record", "name":"foo", "namespace":"x.y"}`,
			Rabin:  0x521d1a6b830ec4ab,
		},
		{
			Schema: `{"fields":[], "type":"record", "name":"a.b.foo", "namespace":"x.y"}`,
			Rabin:  0xbfefe5be5021e2b2,
		},
		{
			Schema: `{"fields":[], "type":"record", "name":"foo", "doc":"Useful info"}`,
			Rabin:  0xbd0c50c84319be7e,
		},
		{
			Schema: `{"fields":[], "type":"record", "name":"foo", "aliases":["foo","bar"]}`,
			Rabin:  0xbd0c50c84319be7e,
		},
		{
			Schema: `{"fields":[], "type":"record", "name":"foo", "doc":"foo", "aliases":["foo","bar"]}`,
			Rabin:  0xbd0c50c84319be7e,
		},
		{
			Schema: `{"fields":[{"type":{"type":"boolean"}, "name":"f1"}], "type":"record", "name":"foo"}`,
			Rabin:  0x6cd8eaf1c968a33b,
		},
		{
			Schema: `{ "fields":[{"type":"boolean", "aliases":[], "name":"f1", "default":true}, {"order":"descending","name":"f2","doc":"Hello","type":"int"}], "type":"record", "name":"foo"}`,
			Rabin:  0xbc8d05bd57f4934a,
		},
		{
			Schema: `{"type":"enum", "name":"foo", "symbols":["A1"]}`,
			Rabin:  0xa7fc039e15aa3169,
		},
		{
			Schema: `{"namespace":"x.y.z", "type":"enum", "name":"foo", "doc":"foo bar", "symbols":["A1", "A2"]}`,
			Rabin:  0xc2433ae5f4999d8b,
		},
		{
			Schema: `{"name":"foo","type":"fixed","size":15}`,
			Rabin:  0x18602ec3ed31a504,
		},
		{
			Schema: `{"namespace":"x.y.z", "type":"fixed", "name":"foo", "doc":"foo bar", "size":32}`,
			Rabin:  0xd579d47693a6171e,
		},
		{
			Schema: `{ "items":{"type":"null"}, "type":"array"}`,
			Rabin:  0xf7d13f2f68170a6d,
		},
		{
			Schema: `{ "values":"string", "type":"map"}`,
			Rabin:  0x86ce965d92864572,
		},
		{
			Schema: `{"name":"PigValue","type":"record", "fields":[{"name":"value", "type":["null", "int", "long", "PigValue"]}]}`,
			Rabin:  0xe795dc6656b7e95b,
		},
	}

	for _, c := range cases {
		codec, err := NewCodec(c.Schema)
		if err != nil {
			t.Fatalf("CASE: %s; cannot create code: %s", c.Schema, err)
		}
		if got, want := codec.Rabin, c.Rabin; got != want {
			t.Errorf("CASE: %s; GOT: %#x; WANT: %#x", c.Schema, got, want)
		}
	}
}

func TestSingleObjectEncoding(t *testing.T) {
	t.Run("int", func(*testing.T) {
		schema := `"int"`

		codec, err := NewCodec(schema)
		if err != nil {
			t.Fatalf("cannot create code: %s", err)
		}

		t.Run("encoding", func(t *testing.T) {
			t.Run("does not modify source buf when cannot encode", func(t *testing.T) {
				buf := []byte{0xDE, 0xAD, 0xBE, 0xEF}

				buf, err = codec.SingleFromNative(buf, "strings cannot be encoded as int")
				ensureError(t, err, "cannot encode binary int")

				if got, want := buf, []byte("\xDE\xAD\xBE\xEF"); !bytes.Equal(got, want) {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})

			t.Run("appends header then encoded data", func(t *testing.T) {
				const original = "\x01\x02\x03\x04"
				buf := []byte(original)

				buf, err = codec.SingleFromNative(buf, 3)
				ensureError(t, err)

				fp := "\xC3\x01" + "\x8F\x5C\x39\x3F\x1A\xD5\x75\x72"

				if got, want := buf, []byte(original+fp+"\x06"); !bytes.Equal(got, want) {
					t.Errorf("\nGOT:\n\t%v;\nWANT:\n\t%v", got, want)
				}
			})
		})

		t.Run("decoding", func(t *testing.T) {
			buf, err := codec.SingleFromNative(nil, 3)
			ensureError(t, err)

			buf = append(buf, "\xDE\xAD"...) // append some junk

			datum, newBuf, err := codec.NativeFromSingle(buf)
			ensureError(t, err)

			if got, want := datum, int32(3); got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			// ensure junk is not disturbed
			if got, want := newBuf, []byte("\xDE\xAD"); !bytes.Equal(got, want) {
				t.Errorf("\nGOT:\n\t%q;\nWANT:\n\t%q", got, want)
			}
		})
	})

	t.Run("record round trip", func(t *testing.T) {
		codec, err := NewCodec(`
{
  "type": "record",
  "name": "LongList",
  "fields" : [
    {"name": "next", "type": ["null", "LongList"], "default": null}
  ]
}
`)
		ensureError(t, err)

		// NOTE: May omit fields when using default value
		initial := `{"next":{"LongList":{}}}`

		// NOTE: Textual encoding will show all fields, even those with values that
		// match their default values
		final := `{"next":{"LongList":{"next":null}}}`

		// Convert textual Avro data (in Avro JSON format) to native Go form
		datum, _, err := codec.NativeFromTextual([]byte(initial))
		ensureError(t, err)

		// Convert native Go form to single-object encoding form
		buf, err := codec.SingleFromNative(nil, datum)
		ensureError(t, err)

		// Convert single-object encoding form back to native Go form
		datum, _, err = codec.NativeFromSingle(buf)
		ensureError(t, err)

		// Convert native Go form to textual Avro data
		buf, err = codec.TextualFromNative(nil, datum)
		ensureError(t, err)

		if got, want := string(buf), final; got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
	})
}

func ExampleCodec_SingleFromNative() {
	codec, err := NewCodec(`"int"`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return
	}

	buf, err := codec.SingleFromNative(nil, 3)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return
	}

	fmt.Println(buf)
	// Output: [195 1 143 92 57 63 26 213 117 114 6]
}

func ExampleCodec_NativeFromBinary_singleItemDecoding() {
	codec1, err := NewCodec(`"int"`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return
	}

	// Create a map of fingerprint values to corresponding Codec instances.
	codex := make(map[uint64]*Codec)
	codex[codec1.Rabin] = codec1

	// Later on when you want to decode such a slice of bytes as a Single-Object
	// Encoding, obtain the Rabin fingerprint of the schema used to encode the
	// data.
	buf := []byte{195, 1, 143, 92, 57, 63, 26, 213, 117, 114, 6}

	fingerprint, newBuf, err := FingerprintFromSOE(buf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return
	}

	// Get a previously stored Codec from the codex map.
	codec2, ok := codex[fingerprint]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown codec: %d\n", fingerprint)
		return
	}

	// Use the fetched Codec to decode the buffer as a SOE.
	datum, _, err := codec2.NativeFromBinary(newBuf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return
	}
	fmt.Println(datum)
	// Output: 3
}

func Test_buildCodecForTypeDescribedByString_CacheRespectsPrecisionScale(t *testing.T) {
	schemaMap := map[string]interface{}{
		"type":        "bytes",
		"logicalType": "decimal",
		"precision":   float64(4),
		"scale":       float64(2),
	}
	cachedCodecIdentifier := "preexisting-cached-coded"
	cache := map[string]*Codec{
		"bytes.decimal": nil, // precision.scale-agnostic codec
		"bytes.decimal.4.2": {
			schemaOriginal: cachedCodecIdentifier, // using field as identifier
		},
	}

	// cached bytes.decimal codec with matching precision.scale is returned
	cacheHit, err := buildCodecForTypeDescribedByString(cache, "", "bytes", schemaMap, nil)
	ensureError(t, err) // ensure NO error
	if cacheHit.schemaOriginal != cachedCodecIdentifier {
		t.Errorf("GOT: %v; WANT: %v", cacheHit.schemaOriginal, cachedCodecIdentifier)
	}

	// cached codec with unmatching precision.scale is not returned
	schemaMap["scale"] = float64(1)
	cacheMiss, err := buildCodecForTypeDescribedByString(cache, "", "bytes", schemaMap, nil)
	ensureError(t, err) // ensure NO error
	if cacheMiss.schemaOriginal == cachedCodecIdentifier {
		t.Errorf("GOT: %v; WANT: %v", cacheMiss.schemaOriginal, "!= "+cachedCodecIdentifier)
	}
}
