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
