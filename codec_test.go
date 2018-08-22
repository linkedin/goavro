package goavro_test

import (
	"fmt"
	"testing"

	"github.com/linkedin/goavro"
)

func ExampleCodecCanonicalSchema() {
	schema := `{"type":"map","values":{"type":"enum","name":"foo","symbols":["alpha","bravo"]}}`
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(codec.CanonicalSchema())
	}
	// Output: {"type":"map","values":{"name":"foo","type":"enum","symbols":["alpha","bravo"]}}
}

func TestSchemaCRC64Avro(t *testing.T) {
	cases := []struct {
		Schema      string
		Fingerprint int64
	}{
		{
			Schema:      `"null"`,
			Fingerprint: 7195948357588979594,
		},
		{
			Schema:      `"boolean"`,
			Fingerprint: -6970731678124411036,
		},
		{
			Schema:      `"int"`,
			Fingerprint: 8247732601305521295,
		},
		{
			Schema:      `"long"`,
			Fingerprint: -3434872931120570953,
		},
		{
			Schema:      `"float"`,
			Fingerprint: 5583340709985441680,
		},
		{
			Schema:      `"double"`,
			Fingerprint: -8181574048448539266,
		},
		{
			Schema:      `"bytes"`,
			Fingerprint: 5746618253357095269,
		},
		{
			Schema:      `"string"`,
			Fingerprint: -8142146995180207161,
		},
		{
			Schema:      `[ "int"  ]`,
			Fingerprint: -5232228896498058493,
		},
		{
			Schema:      `[ "int" , {"type":"boolean"} ]`,
			Fingerprint: 5392556393470105090,
		},
		{
			Schema:      `{"fields":[], "type":"record", "name":"foo"}`,
			Fingerprint: -4824392279771201922,
		},
		{
			Schema:      `{"fields":[], "type":"record", "name":"foo", "namespace":"x.y"}`,
			Fingerprint: 5916914534497305771,
		},
		{
			Schema:      `{"fields":[], "type":"record", "name":"a.b.foo", "namespace":"x.y"}`,
			Fingerprint: -4616218487480524110,
		},
		{
			Schema:      `{"fields":[], "type":"record", "name":"foo", "doc":"Useful info"}`,
			Fingerprint: -4824392279771201922,
		},
		{
			Schema:      `{"fields":[], "type":"record", "name":"foo", "aliases":["foo","bar"]}`,
			Fingerprint: -4824392279771201922,
		},
		{
			Schema:      `{"fields":[], "type":"record", "name":"foo", "doc":"foo", "aliases":["foo","bar"]}`,
			Fingerprint: -4824392279771201922,
		},
		{
			Schema:      `{"fields":[{"type":{"type":"boolean"}, "name":"f1"}], "type":"record", "name":"foo"}`,
			Fingerprint: 7843277075252814651,
		},
		{
			Schema:      `{ "fields":[{"type":"boolean", "aliases":[], "name":"f1", "default":true}, {"order":"descending","name":"f2","doc":"Hello","type":"int"}], "type":"record", "name":"foo"}`,
			Fingerprint: -4860222112080293046,
		},
		{
			Schema:      `{"type":"enum", "name":"foo", "symbols":["A1"]}`,
			Fingerprint: -6342190197741309591,
		},
		{
			Schema:      `{"namespace":"x.y.z", "type":"enum", "name":"foo", "doc":"foo bar", "symbols":["A1", "A2"]}`,
			Fingerprint: -4448647247586288245,
		},
		{
			Schema:      `{"name":"foo","type":"fixed","size":15}`,
			Fingerprint: 1756455273707447556,
		},
		{
			Schema:      `{"namespace":"x.y.z", "type":"fixed", "name":"foo", "doc":"foo bar", "size":32}`,
			Fingerprint: -3064184465700546786,
		},
		{
			Schema:      `{ "items":{"type":"null"}, "type":"array"}`,
			Fingerprint: -589620603366471059,
		},
		{
			Schema:      `{ "values":"string", "type":"map"}`,
			Fingerprint: -8732877298790414990,
		},
		{
			Schema:      `{"name":"PigValue","type":"record", "fields":[{"name":"value", "type":["null", "int", "long", "PigValue"]}]}`,
			Fingerprint: -1759257747318642341,
		},
	}

	for _, c := range cases {
		codec, err := goavro.NewCodec(c.Schema)
		if err != nil {
			t.Fatalf("CASE: %s; cannot create code: %s", c.Schema, err)
		}
		if got, want := codec.SchemaCRC64Avro(), c.Fingerprint; got != want {
			t.Errorf("CASE: %s; GOT: %#x; WANT: %#x", c.Schema, got, want)
		}
	}
}
