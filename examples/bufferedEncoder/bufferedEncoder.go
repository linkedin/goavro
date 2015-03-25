package main

import (
	"bufio"
	"bytes"
	"github.com/linkedin/goavro"
	"io"
	"log"
)

func main() {
	bits, err := bufferedEncoder(`"string"`, "filibuster")
	if err != nil {
		log.Fatal(err)
	}
	expected := []byte("\x14filibuster")
	if bytes.Compare(bits, expected) != 0 {
		log.Fatalf("Actual: %#v; Expected: %#v", bits, expected)
	}
}

func bufferedEncoder(someSchemaJson string, datum interface{}) (bits []byte, err error) {
	bb := new(bytes.Buffer)
	defer func() {
		bits = bb.Bytes()
	}()

	var c goavro.Codec
	c, err = goavro.NewCodec(someSchemaJson)
	if err != nil {
		return
	}
	err = encodeWithBufferedWriter(c, bb, datum)
	return
}

func encodeWithBufferedWriter(c goavro.Codec, w io.Writer, datum interface{}) error {
	bw := bufio.NewWriter(w)
	err := c.Encode(bw, datum)
	if err != nil {
		return err
	}
	return bw.Flush()
}
