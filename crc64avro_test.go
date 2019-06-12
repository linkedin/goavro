package goavro

import (
	"testing"
)

func TestCrc64Avro(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		if got, want := crc64Avro([]byte(`"int"`)), uint64(0x7275d51a3f395c8f); got != want {
			t.Errorf("GOT: %x; WANT: %x", got, want)
		}
	})

	t.Run("string", func(t *testing.T) {
		if got, want := crc64Avro([]byte(`"string"`)), uint64(0x8f014872634503c7); got != want {
			t.Errorf("GOT: %x; WANT: %x", got, want)
		}
	})
}
