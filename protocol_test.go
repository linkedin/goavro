package goavro

import (
	"testing"
	"bytes"
	"encoding/json"
)

func TestProtoParse(t *testing.T) {
	proto, err := NewProtocol() 
	if err!=nil {
			t.Fatal(err)
	}
	if "AvroSourceProtocol" !=proto.Name {
		t.Errorf("Proto Name not pared; Expected AvroSourceProtocol / actual %#v (%#v", proto.Name, proto)
	}

	if len(proto.MD5)==0 {
		t.Errorf("Proto MD5 not calculated; actual %#v ", proto)
	}
	if len(proto.Types)!=2 { t.Errorf("Types not parsed; Expect 2, actual %i", len(proto.Types)) }
	t.Logf("proto %#v", proto)
}


func jsonCompact(in string) (out string) {
	var json_bytes = []byte(in)
	buffer := new(bytes.Buffer)
	json.Compact(buffer, json_bytes)
	out =  buffer.String()
	return
}

func TestToJson(t *testing.T) {
	protocol, err := NewProtocol()
	if err!= nil {
		t.Fatal("%#v", err)
	}

	result, err := protocol.Json()
	if err !=nil {
		t.Fatal("%#v", err)
	}
	if result!= jsonCompact(proto)  {
		t.Errorf("Proto to Json not equals; Expected %#v, actual %#v",jsonCompact(proto), result)
	}
}
