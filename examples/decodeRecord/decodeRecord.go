package main

import (
	"bytes"
	"fmt"
	"github.com/linkedin/goavro"
	"log"
)

func main() {
	recordSchemaJson := `{"type":"record","name":"comments","namespace":"com.example","fields":[{"name":"username","type":"string","doc":"Name of user"},{"name":"comment","type":"string","doc":"The content of the user's message"},{"name":"timestamp","type":"long","doc":"Unix epoch time in milliseconds"}],"doc:":"A basic schema for storing blog comments"}`
	codec, err := goavro.NewCodec(recordSchemaJson)
	if err != nil {
		log.Fatal(err)
	}
	encoded := []byte("\x0eAquamanPThe Atlantic is oddly cold this morning!\x88\x88\x88\x88\x08")
	bb := bytes.NewBuffer(encoded)
	decoded, err := codec.Decode(bb)
	fmt.Println(decoded)
}
