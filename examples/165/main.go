// #165
//
// This exemplifies three ways to encode an Avro record.  Note that I did not
// say "Go struct" because there is no struct in this example.  `goavro` expects
// data that is to be encoded as an Avro record to be given in the form of a
// `map[string]interface{}`, so create the map, populate whichever key-value
// pairs that the Avro record type requires, and pass it on to one of the
// encoding methods.
//
// Note that there are three ways to encode Avro data into binary.  The first
// way is to use the BinaryFromNative method, which simply encodes the provided
// value as a sequence of bytes, appending the new bytes to the provided byte
// slice, and returning the new byte slice.  This binary data is completely
// unusable by any process that wants to decode the bytes unless the original
// schema that was used to encode the data is known when trying to decode the
// bytes.
//
// The second example is using Avro's Single-Object Encoding specification,
// where a magic byte sequence, then the schema's fingerprint is first appended
// to the provided byte slice, then finally the binary encoded bytes of the data
// is appended.  This method is useful for processes where the decoding reader
// will pull off a chunk of bytes, use the fingerprint to look up the schema in
// some sort of schema registry, then use that schema to decode the bytes that
// follow.  This method is used by Kafka producers and consumers, where rather
// than shoving the schema text on the wire for each method is wasteful compared
// to shoving a tiny schema fingerprint on the wire.  This method only uses 10
// more bytes to uniquely identify the schema.
//
// Finally the third example uses the Avro Object Container File format to
// encode the data, where the OCF file has a copy of the schema used to encode
// the file.  Because the original schema prefixes the entire file, any Avro
// reader can decode the contents of the entire file without having to look up
// its schema in a registry.

package main

import (
	"os"

	"github.com/linkedin/goavro/v2"
)

const loginEventAvroSchema = `{"type": "record", "name": "LoginEvent", "fields": [{"name": "Username", "type": "string"}]}`

func main() {
	codec, err := goavro.NewCodec(loginEventAvroSchema)
	if err != nil {
		panic(err)
	}

	m := map[string]interface{}{
		"Username": "superman",
	}

	// Let's dip our feet into just encoding a single item into binary format.
	// There is not much to do with the output from binary if you intend on
	// creating an OCF file, because OCF will do this encoding for us.  The
	// result is an unadorned stream of binary bytes that can never be decoded
	// unless you happen to know the schema that was used to encode it.
	binary, err := codec.BinaryFromNative(nil, m)
	if err != nil {
		panic(err)
	}
	_ = binary

	// Next, let's try encoding the same item using Single-Object Encoding,
	// another format that is useful when sending a bunch of objects into a
	// Kafka stream.  Note this method prefixes the binary bytes with a schema
	// fingerprint, used by the reader on the stream to lookup the contents of
	// the schema used to encode the value.  Again, unless the reader can fetch
	// the schema contents from a schema source-of-truth, this binary sequence
	// will never be decodable.
	single, err := codec.SingleFromNative(nil, m)
	if err != nil {
		panic(err)
	}
	_ = single

	// Next, let's make an OCF file from the values.  The OCF format prefixes
	// the entire file with the required schema that was used to encode the
	// data, so it is readable from any Avro decoder that can read OCF files.
	// No other source of information is needed to decode the file created by
	// this process, unlike the above two examples.  Also note that we do not
	// send OCF the encoded blobs to write, but just append the values and it
	// will encode each of the values for us.
	var values []map[string]interface{}
	values = append(values, m)
	values = append(values, map[string]interface{}{"Username": "batman"})
	values = append(values, map[string]interface{}{"Username": "wonder woman"})

	f, err := os.Create("event.avro")
	if err != nil {
		panic(err)
	}
	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:     f,
		Codec: codec,
	})
	if err != nil {
		panic(err)
	}
	if err = ocfw.Append(values); err != nil {
		panic(err)
	}
}
