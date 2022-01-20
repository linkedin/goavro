package main

import (
	"fmt"

	"github.com/linkedin/goavro/v2"
)

func main() {
	codex := initCodex()

	err := decode(codex, []byte("\xC3\x01"+"\x8F\x5C\x39\x3F\x1A\xD5\x75\x72"+"\x06"))
	if err != nil {
		panic(err)
	}

	err = decode(codex, []byte("\xC3\x01"+"\xC7\x03\x45\x63\x72\x48\x01\x8F"+"\x0ahello"))
	if err != nil {
		panic(err)
	}
}

// initCodex returns a codex with a small handful of example Codec instances.
func initCodex() map[uint64]*goavro.Codec {
	codex := make(map[uint64]*goavro.Codec)

	for _, primitive := range []string{"int", "long", "boolean", "float", "double", "string"} {
		codec, err := goavro.NewCodec(`"` + primitive + `"`)
		if err != nil {
			panic(err)
		}
		codex[codec.Rabin] = codec
	}

	return codex
}

// decode attempts to decode the bytes in buf using one of the Codec instances
// in codex.  The buf must start with the single-object encoding prefix,
// followed by the unsigned 64-bit Rabin fingerprint of the canonical schema
// used to encode the datum, finally followed by the encoded bytes.  This is a
// simplified example of fetching the fingerprint from the SOE buffer, using
// that fingerprint to select a Codec from a dictionary of Codec instances,
// called codex in this case, and finally sends the buf to be decoded by that
// Codec.
func decode(codex map[uint64]*goavro.Codec, buf []byte) error {
	// Perform a sanity check on the buffer, then return the Rabin fingerprint
	// of the schema used to encode the data.
	fingerprint, newBuf, err := goavro.FingerprintFromSOE(buf)
	if err != nil {
		panic(err)
		return err
	}

	// Get a previously stored Codec from the codex map.
	codec, ok := codex[fingerprint]
	if !ok {
		return fmt.Errorf("unknown codec: %#x", fingerprint)
	}

	// Use the fetched Codec to decode the buffer as a SOE.
	var datum interface{}

	// Both of the following branches work, but provided to illustrate two
	// use-cases.
	if true {
		// Faster because SOE magic prefix and schema fingerprint already
		// checked and used to fetch the Codec.  Just need to decode the binary
		// bytes remaining after the prefix were removed.
		datum, _, err = codec.NativeFromBinary(newBuf)
	} else {
		// This way re-checks the SOE magic prefix and Codec fingerprint, doing
		// repetitive work, but provided as an example for cases when there is
		// only a single schema, a single Codec, and you do not use
		// the FingerprintFromSOE function above.
		datum, _, err = codec.NativeFromSingle(buf)
	}
	if err != nil {
		panic(err)
	}

	_, err = fmt.Println(datum)
	return err
}
