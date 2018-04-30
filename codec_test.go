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
	codec, err := goavro.NewCodec(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	_ = codec
	if got, want := codec.SchemaCRC64Avro(), uint64(13); got != want {
		t.Errorf("GOT: %#x; WANT: %#x", got, want)
	}
}
