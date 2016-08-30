package goavro

import (
	"testing"
	"bytes"
	"encoding/json"
	"reflect"
)

func TestMD5(t *testing.T) {
	proto, err := NewProtocol()
	if err!=nil {
		t.Fatal(err)
	}
	expected := []byte("\x86\xaa\xda\xe2\xc4\x54\x74\xc0\xfe\x93\xff\xd0\xf2\x35\x0a\x65")
	if !reflect.DeepEqual(expected, proto.MD5) {
		t.Errorf("MD5 not equals: %x / %x", expected, proto.MD5)
	}


}


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

	if len(proto.Messages)!=2 {
		t.Errorf("Message not parsed: Expected2 , actual %i", len(proto.Messages))
	}

	message, ok := proto.Messages["append"]
	if  !ok {
		t.Errorf("Message append not found : %v", proto.Messages)
	}
	if len(message.Request) !=1 {
		t.Errorf("Message request not equals to 1: %v", message.Request)
	}
	if message.Request[0].Name!="event" {
		t.Errorf("Request parameter not equals event / %v", message.Request[0].Name)
	}
	if message.Request[0].TypeX.ref!="AvroFlumeEvent" {
		t.Errorf("Request parameter type not equals AvroFlumeEvent / %v", message.Request[0].TypeX.ref)
	}
	typeFound, found := TYPES_CACHE[message.Request[0].TypeX.ref]
	if !found {
		t.Errorf("Type not found on cache %v", message.Request[0].TypeX.ref)
	}
	if !reflect.DeepEqual(typeFound, *message.Request[0].TypeX.ProtocolType) {
		t.Errorf("Type not equals with cache %v / %v",typeFound, *message.Request[0].TypeX.ProtocolType)
	}
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
		t.Errorf("Proto to Json not equals; Expected \n%#v\nactual \n%#v",jsonCompact(proto), result)
	}
}

func TestGetCodec(t *testing.T) {
	protocol, err := NewProtocol()
	if err!= nil {
		t.Fatal("%#v", err)
	}


	flumeRecord, errFlume := protocol.NewRecord("AvroFlumeEvent")
	if errFlume != nil {
		t.Fatal(errFlume)
	}
	headers := make(map[string]interface{})
	headers["host_header"] = "127.0.0.1"
	flumeRecord.Set("headers", headers)
	flumeRecord.Set("body", []byte("test"))
	bb := new(bytes.Buffer)
	codec, err := protocol.MessageRequestCodec("append")
	if err != nil {
		t.Fatal(err)
	}
	codec.Encode(bb, flumeRecord)

	t.Logf("AvroFlumeEvent test: %v", bb)

}
