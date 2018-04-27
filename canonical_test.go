package goavro_test

import (
	"testing"

	"github.com/linkedin/goavro"
)

func TestCanonicalSchema(t *testing.T) {

	// Test cases are taken from the reference implementation here:
	// https://github.com/apache/avro/blob/master/share/test/data/schema-tests.txt

	cases := []struct {
		Schema    string
		Canonical string
	}{
		{
			Schema:    `"null"`,
			Canonical: `"null"`,
		},
		{
			Schema:    `{"type":"null"}`,
			Canonical: `"null"`,
		},
		{
			Schema:    `"boolean"`,
			Canonical: `"boolean"`,
		},
		{
			Schema:    `{"type":"boolean"}`,
			Canonical: `"boolean"`,
		},
		{
			Schema:    `"int"`,
			Canonical: `"int"`,
		},
		{
			Schema:    `{"type":"int"}`,
			Canonical: `"int"`,
		},
		{
			Schema:    `"long"`,
			Canonical: `"long"`,
		},
		{
			Schema:    `{"type":"long"}`,
			Canonical: `"long"`,
		},
		{
			Schema:    `"float"`,
			Canonical: `"float"`,
		},
		{
			Schema:    `{"type":"float"}`,
			Canonical: `"float"`,
		},
		{
			Schema:    `"double"`,
			Canonical: `"double"`,
		},
		{
			Schema:    `{"type":"double"}`,
			Canonical: `"double"`,
		},
		{
			Schema:    `"bytes"`,
			Canonical: `"bytes"`,
		},
		{
			Schema:    `{"type":"bytes"}`,
			Canonical: `"bytes"`,
		},
		{
			Schema:    `"string"`,
			Canonical: `"string"`,
		},
		{
			Schema:    `{"type":"string"}`,
			Canonical: `"string"`,
		},
		/*
			// Supported by the reference implementation but not by goavro at this point
			{
				Schema:    "[  ]",
				Canonical: "[]",
			},
		*/
		{
			Schema:    `[ "int"  ]`,
			Canonical: `["int"]`,
		},
		{
			Schema:    `[ "int" , {"type":"boolean"} ]`,
			Canonical: `["int","boolean"]`,
		},

		// The following 7 test cases differ from the reference implementation since goavro doesn't
		// currently support empty fields array. A field name "dummy" is added since these tests are
		// testing other aspects of canonicalization than empty field array.
		{
			Schema:    `{"fields":[{"name":"dummy","type":"int"}], "type":"record", "name":"foo"}`,
			Canonical: `{"name":"foo","type":"record","fields":[{"name":"dummy","type":"int"}]}`,
		},
		{
			Schema:    `{"fields":[{"name":"dummy","type":"int"}], "type":"record", "name":"foo", "namespace":"x.y"}`,
			Canonical: `{"name":"x.y.foo","type":"record","fields":[{"name":"dummy","type":"int"}]}`,
		},
		{
			Schema:    `{"fields":[{"name":"dummy","type":"int"}], "type":"record", "name":"foo", "namespace":"x.y"}`,
			Canonical: `{"name":"x.y.foo","type":"record","fields":[{"name":"dummy","type":"int"}]}`,
		},
		{
			Schema:    `{"fields":[{"name":"dummy","type":"int"}], "type":"record", "name":"a.b.foo", "namespace":"x.y"}`,
			Canonical: `{"name":"a.b.foo","type":"record","fields":[{"name":"dummy","type":"int"}]}`,
		},
		{
			Schema:    `{"fields":[{"name":"dummy","type":"int"}], "type":"record", "name":"foo", "doc":"Useful info"}`,
			Canonical: `{"name":"foo","type":"record","fields":[{"name":"dummy","type":"int"}]}`,
		},
		{
			Schema:    `{"fields":[{"name":"dummy","type":"int"}], "type":"record", "name":"foo", "aliases":["foo","bar"]}`,
			Canonical: `{"name":"foo","type":"record","fields":[{"name":"dummy","type":"int"}]}`,
		},
		{
			Schema:    `{"fields":[{"name":"dummy","type":"int"}], "type":"record", "name":"foo", "doc":"foo", "aliases":["foo","bar"]}`,
			Canonical: `{"name":"foo","type":"record","fields":[{"name":"dummy","type":"int"}]}`,
		},

		{
			Schema:    `{"fields":[{"type":{"type":"boolean"}, "name":"f1"}], "type":"record", "name":"foo"}`,
			Canonical: `{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}`,
		},
		{
			Schema: `{"fields": [
		                {"type": "boolean", "aliases": [], "name": "f1", "default": true},
		                {"order": "descending", "name": "f2", "doc": "Hello", "type": "int"}
		              ],
		              "type": "record",
		              "name": "foo"}`,
			Canonical: `{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"},{"name":"f2","type":"int"}]}`,
		},
		{
			Schema:    `{"type":"enum", "name":"foo", "symbols":["A1"]}`,
			Canonical: `{"name":"foo","type":"enum","symbols":["A1"]}`,
		},
		{
			Schema:    `{"namespace":"x.y.z", "type":"enum", "name":"foo", "doc":"foo bar", "symbols":["A1", "A2"]}`,
			Canonical: `{"name":"x.y.z.foo","type":"enum","symbols":["A1","A2"]}`,
		},
		{
			Schema:    `{"name":"foo","type":"fixed","size":15}`,
			Canonical: `{"name":"foo","type":"fixed","size":15}`,
		},
		{
			Schema:    `{"namespace":"x.y.z", "type":"fixed", "name":"foo", "doc":"foo bar", "size":32}`,
			Canonical: `{"name":"x.y.z.foo","type":"fixed","size":32}`,
		},
		{
			Schema:    `{ "items":{"type":"null"}, "type":"array"}`,
			Canonical: `{"type":"array","items":"null"}`,
		},
		{
			Schema:    `{ "values":"string", "type":"map"}`,
			Canonical: `{"type":"map","values":"string"}`,
		},
		{
			Schema: `  {"name":"PigValue","type":"record",
			   "fields":[{"name":"value", "type":["null", "int", "long", "PigValue"]}]}`,
			Canonical: `{"name":"PigValue","type":"record","fields":[{"name":"value","type":["null","int","long","PigValue"]}]}`,
		},

		// [INTEGERS] Eliminate quotes around and any leading zeros in front of
		// JSON integer literals (which appear in the size attributes of fixed
		// schemas).
		{
			Schema:    `{"size":"15","type":"fixed","name":"foo"}`,
			Canonical: `{"name":"foo","type":"fixed","size":15}`,
		},

		// [STRINGS] For all JSON string literals in the schema text, replace
		// any escaped characters (e.g., \uXXXX escapes) with their UTF-8
		// equivalents.
		{
			// primitive
			Schema:    `"\u0069\u006e\u0074"`,
			Canonical: `"int"`,
		},
		{
			// primitive wrapped in JSON object
			Schema:    `{"type":"\u0069\u006e\u0074"}`,
			Canonical: `"int"`,
		},
		{
			// array items
			Schema:    `{"type":"array","items":"\u0069\u006e\u0074"}`,
			Canonical: `{"type":"array","items":"int"}`,
		},
		{
			// enum symbols
			Schema:    `{"type":"enum","symbols":["\u0047\u006f","\u0041\u0076\u0072\u006f"],"name":"\u0046\u006f\u006f"}`,
			Canonical: `{"name":"Foo","type":"enum","symbols":["Go","Avro"]}`,
		},
		{
			// fixed name
			Schema:    `{"size":16,"type":"fixed","name":"\u0046\u006f\u006f"}`,
			Canonical: `{"name":"Foo","type":"fixed","size":16}`,
		},
		{
			// map values
			Schema:    `{"values":"\u0069\u006e\u0074","type":"map"}`,
			Canonical: `{"type":"map","values":"int"}`,
		},
		{
			// record name
			Schema:    `{"fields":[{"name":"hi","type":"int"}], "type":"record", "name":"\u0046\u006f\u006f", "namespace":"x.y"}`,
			Canonical: `{"name":"x.y.Foo","type":"record","fields":[{"name":"hi","type":"int"}]}`,
		},
		{
			// record namespace
			Schema:    `{"fields":[{"name":"hi","type":"int"}], "type":"record", "name":"Foo", "namespace":"\u0078\u002e\u0079"}`,
			Canonical: `{"name":"x.y.Foo","type":"record","fields":[{"name":"hi","type":"int"}]}`,
		},
		{
			// record field name
			Schema:    `{"fields":[{"name":"\u0068\u0069","type":"int"}], "type":"record", "name":"Foo", "namespace":"x.y"}`,
			Canonical: `{"name":"x.y.Foo","type":"record","fields":[{"name":"hi","type":"int"}]}`,
		},
		{
			// record field type
			Schema:    `{"fields":[{"name":"hi","type":"\u0069\u006e\u0074"}], "type":"record", "name":"Foo", "namespace":"x.y"}`,
			Canonical: `{"name":"x.y.Foo","type":"record","fields":[{"name":"hi","type":"int"}]}`,
		},
		{
			// union children
			Schema:    `["\u006e\u0075\u006c\u006c","\u0069\u006e\u0074"]`,
			Canonical: `["null","int"]`,
		},
	}

	for _, c := range cases {
		codec, err := goavro.NewCodec(c.Schema)
		if err != nil {
			t.Errorf("Unable to create codec for schema: %s\nwith error: %s", c.Schema, err)
		} else {
			if got, want := codec.CanonicalSchema(), c.Canonical; got != want {
				t.Errorf("Test failed for schema: %s\n\tgot canonical:\t\t%s\n\texpected canonical:\t%s", c.Schema, got, want)
			}
		}
	}
}
