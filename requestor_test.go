package goavro

import (
	"testing"
	"net"
	"bytes"
	"reflect"
)

func TestWrite_handshake_request(t *testing.T) {
	//t.SkipNow()
	rAddr, err := net.ResolveTCPAddr("tcp", "10.98.80.113:63001")
	conn, err := net.DialTCP("tcp", nil, rAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	transceiver := NewNettyTransceiver(conn)
	protocol, err := NewProtocol()
	if err != nil {
		t.Fatal(err)
	}
	requestor := NewRequestor(protocol, transceiver)

	bb := new(bytes.Buffer)
	requestor.write_handshake_request(bb)
	//	conn.Write(bb.Bytes())
	t.Logf("Handshake_request size %v %x\n",bb.Len(),  bb.Bytes())
	t.Logf( "Handshake_request %v\n", bb.String())

	refHandshake := []byte("\x86\xaa\xda\xe2\xc4\x54\x74\xc0\xfe\x93\xff\xd0\xf2\x35\x0a\x65\x00\x86\xaa\xda\xe2\xc4\x54\x74\xc0\xfe\x93\xff\xd0\xf2\x35\x0a\x65\x02\x00")
	//bytes := bb.Bytes()
	//if !reflect.DeepEqual(refHandshake, bytes) {
	//	t.Fatalf("Handshake not equals to ref %n%x, %n%x", len(refHandshake), refHandshake, len(bytes), bytes)
	//}


	codecHandshake, err := NewCodec(handshakeRequestshema)
	if err != nil {
		t.Fatal(err)
	}
	record, err := codecHandshake.Decode(bytes.NewBuffer(refHandshake))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("\nHandshake_request decoded %v\n", record)

}


func TestWrite_call_request(t *testing.T) {
	//t.SkipNow()
	rAddr, err := net.ResolveTCPAddr("tcp", "10.98.80.113:63001")
	conn, err := net.DialTCP("tcp", nil, rAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	transceiver := NewNettyTransceiver(conn)
	protocol, err := NewProtocol()
	if err != nil {
		t.Fatal(err)
	}
	requestor := NewRequestor(protocol, transceiver)

	bb := new(bytes.Buffer)
	datum, err := protocol.NewRecord("AvroFlumeEvent")
	if err != nil {
		t.Fatal(err)
	}

	headers := make(map[string]interface{})
	headers["host_header"] = "127.0.0.1"
	datum.Set("headers", headers)
	datum.Set("body", []byte("2016-08-02 14:45:38|flume.composantTechnique_IS_UNDEFINED|flume.application_IS_UNDEFINED|flume.client_IS_UNDEFINED|flume.plateforme_IS_UNDEFINED|instance_IS_UNDEFINED|logname_IS_UNDEFINED|WARN |test.LogGenerator|test !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"))



	requestor.write_call_request("append", datum,bb)
	//	conn.Write(bb.Bytes())
	t.Logf("\nCall_request size %v %v\n",bb.Len(),  bb.Bytes())
	t.Logf("\nCall_request %v\n", bb.String())

	codec, err := protocol.MessageRequestCodec("append")
	if err != nil {
		t.Fatal(err)
	}
	value, err := codec.Decode(bb)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(datum, value) {
		t.Fatalf("Request not equals to ref %x, %x", datum, value)
	}
}

func TestWrite_call_requestHeader(t *testing.T) {
	//t.SkipNow()
	rAddr, err := net.ResolveTCPAddr("tcp", "10.98.80.113:63001")
	conn, err := net.DialTCP("tcp", nil, rAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	transceiver := NewNettyTransceiver(conn)
	protocol, err := NewProtocol()
	if err != nil {
		t.Fatal(err)
	}
	requestor := NewRequestor(protocol, transceiver)

	bb := new(bytes.Buffer)

	requestor.write_call_requestHeader("append", bb)

	refHeader := []byte("\x00\x0c\x61\x70\x70\x65\x6e\x64")
	bytes := bb.Bytes()
	if !reflect.DeepEqual(refHeader, bytes) {
		t.Fatalf("Request_Header not equals to ref %n%x, %n%x", len(refHeader), refHeader, len(bytes), bytes)
	}
}


