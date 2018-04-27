package goavro_test

import (
	"fmt"

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
