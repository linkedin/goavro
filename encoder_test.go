package goavro

import (
	"testing"
	"bytes"
)

const testSchema= `
{
 "type": "record", 
 "name": "test",
 "fields" : [
   {"name": "a", "type": "long"},
   {"name": "b", "type": "string"}
 ]
}
`
func TestEncode(t *testing.T) {
	record,err := NewRecord(RecordSchema(testSchema))
	if err != nil {
		t.Fatal(err)
	}
	record.Set("a",int64(27))
	record.Set("b","foo")
	
	codec, err := NewCodec(testSchema)
	if err != nil {
		t.Fatal(err)
	}
	
	bb := new(bytes.Buffer)
	if err = codec.Encode(bb, record); err !=nil {
		t.Fatal(err)
	}
	actual := bb.Bytes()
	expected := []byte("\x36\x06\x66\x6f\x6f")

	if bytes.Compare(actual, expected) != 0 {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}
