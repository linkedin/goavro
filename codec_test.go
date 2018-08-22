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
