package goavro

import (
	"testing"
	"bytes"
	"encoding/json"
	"crypto/md5"
	"reflect"
)

func TestMD5(t *testing.T) {




	t.Logf("golang md5 hex %x", md5.Sum([]byte(`{"protocol":"AvroSourceProtocol","namespace":"org.apache.flume.source.avro","doc":"* Licensed to the Apache Software Foundation (ASF) under one\n * or more contributor license agreements.  See the NOTICE file\n * distributed with this work for additional information\n * regarding copyright ownership.  The ASF licenses this file\n * to you under the Apache License, Version 2.0 (the\n * \"License\"); you may not use this file except in compliance\n * with the License.  You may obtain a copy of the License at\n *\n * http://www.apache.org/licenses/LICENSE-2.0\n *\n * Unless required by applicable law or agreed to in writing,\n * software distributed under the License is distributed on an\n * \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n * KIND, either express or implied.  See the License for the\n * specific language governing permissions and limitations\n * under the License.","types":[{"type":"enum","name":"Status","symbols":["OK","FAILED","UNKNOWN"]},{"type":"record","name":"AvroFlumeEvent","fields":[{"name":"headers","type":{"type":"map","values":"string"}},{"name":"body","type":"bytes"}]}],"messages":{"append":{"request":[{"name":"event","type":"AvroFlumeEvent"}],"response":"Status"},"appendBatch":{"request":[{"name":"events","type":{"type":"array","items":"AvroFlumeEvent"}}],"response":"Status"}}}`)))
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
		t.Errorf("Proto to Json not equals; Expected %#v, actual %#v",jsonCompact(proto), result)
	}
}

func TestGetCodec(t *testing.T) {
	protocol, err := NewProtocol()
	if err!= nil {
		t.Fatal("%#v", err)
	}


	flumeRecord, errFlume := protocol.getNewRecord("AvroFlumeEvent")
	if errFlume != nil {
		t.Fatal(errFlume)
	}
	headers := make(map[string]interface{})
	headers[AVRO_SCHEMA_LITERAL_HEADER] = stringSchema
	headers["host_header"] = "127.0.0.1"
	flumeRecord.Set("headers", headers)
	flumeRecord.Set("body", []byte("test"))
	bb := new(bytes.Buffer)
	codec, err := protocol.getMessageRequestCodec("append")
	if err != nil {
		t.Fatal(err)
	}
	codec.Encode(bb, flumeRecord)

	t.Logf("AvroFlumeEvent test: %v", bb)

}
