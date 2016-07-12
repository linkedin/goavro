package goavro

import (
	"testing"
)

func TestProtoParse(t *testing.T) {
	proto, err := NewProtocol() 
	if err!=nil {
			t.Fatal("%v",err)
	}
	if "AvroSourceProtocol" !=proto.Name {
		t.Errorf("Proto Name not pared; Expected AvroSourceProtocol / actual %#v (%#v", proto.Name, proto)
	}

	if len(proto.MD5)==0 {
		t.Errorf("Proto MD5 not calculated; actual %#v ", proto)
	}
	t.Logf("proto %#v", proto)
}

func TestToJson(t *testing.T) {
	protocol, err := NewProtocol()
	if err!= nil {
		t.Fatal("%#v", err)
	}

	json, err := protocol.Json()
	if err !=nil {
		t.Fatal("%#v", err)
	}
	if  json!= proto  {
		t.Errorf("Proto to Json not equals; Expected %#v, actual %#v", proto, json)
	}
}
