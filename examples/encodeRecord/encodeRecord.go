package main

import (
	"bytes"
	"github.com/linkedin/goavro"
	"log"
)

func main() {
	recordSchemaJson := `{"type":"record","name":"comments","namespace":"com.example","fields":[{"name":"username","type":"string","doc":"Name of user"},{"name":"comment","type":"string","doc":"The content of the user's message"},{"name":"timestamp","type":"long","doc":"Unix epoch time in milliseconds"}],"doc:":"A basic schema for storing blog comments"}`
	someRecord, err := goavro.NewRecord(goavro.RecordSchemaJson(recordSchemaJson))
	if err != nil {
		log.Fatal(err)
	}
	someRecord.Fields[0].Datum = "Aquaman"
	someRecord.Fields[1].Datum = "The Atlantic is oddly cold this morning!"
	someRecord.Fields[2].Datum = int64(1082196484)

	codec, err := goavro.NewCodec(recordSchemaJson)
	if err != nil {
		log.Fatal(err)
	}

	bb := new(bytes.Buffer)
	if err = codec.Encode(bb, someRecord); err != nil {
		log.Fatal(err)
	}

	actual := bb.Bytes()
	expected := []byte("\x0eAquamanPThe Atlantic is oddly cold this morning!\x88\x88\x88\x88\x08")
	if bytes.Compare(actual, expected) != 0 {
		log.Printf("Actual: %#v; Expected: %#v", actual, expected)
	}
}
