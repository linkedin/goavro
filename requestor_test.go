package goavro

import (
	"testing"
	"net"
	"bytes"
	"reflect"
	netty "github.com/sebglon/goavro/transceiver/netty"
	"github.com/sebglon/goavro/transceiver"
	"runtime"
	"strconv"
)

func TestWrite_handshake_request(t *testing.T) {
	//t.SkipNow()

	transceiver, err := netty.NewTransceiver(transceiver.Config{Host:HOST, Port:PORT})
	if err != nil {
		t.Fatal(err)
	}
	protocol, err := NewProtocol()
	if err != nil {
		t.Fatal(err)
	}
	requestor := NewRequestor(protocol, transceiver)


	hds, err := requestor.write_handshake_request()
	//	conn.Write(bb.Bytes())
	t.Logf("Handshake_request size %v %x\n",len(hds),  hds)
	t.Logf( "Handshake_request %v\n", hds)

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

func TestRead_handshake_reponse(t *testing.T) {
	codecHandshake, err := NewCodec(handshakeResponseshema)
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewRecord(RecordSchema(handshakeResponseshema))
	if err != nil {
		t.Fatal(err)
	}
	record.Set("match", Enum{"match", "BOTH"})
	record.Set("serverProtocol", nil)
	record.Set("serverHash", nil)
	record.Set("meta", nil)

	bb := new(bytes.Buffer)
	err = codecHandshake.Encode(bb, record)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Encode HandshakeResponse %v", bb.Bytes())

	_, err = codecHandshake.Decode(bytes.NewReader(bb.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	transceiver, err := netty.NewTransceiver(transceiver.Config{Host:HOST, Port:PORT})
	if err != nil {
		t.Fatal(err)
	}

	protocol, err := NewProtocol()
	if err != nil {
		t.Fatal(err)
	}
	requestor := NewRequestor(protocol, transceiver)

	_, err = requestor.read_handshake_response(bytes.NewReader(bb.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
}

func TestWrite_call_request(t * testing.T) {
		//t.SkipNow()
		transceiver, err := netty.NewTransceiver(transceiver.Config{Host:HOST, Port:PORT})

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

		requestor.write_call_request("append", datum, bb)
		//	conn.Write(bb.Bytes())
		t.Logf("\nCall_request size %v %v\n", bb.Len(), bb.Bytes())
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

func TestWrite_call_requestHeader(t * testing.T) {
		//t.SkipNow()
		transceiver, err := netty.NewTransceiver(transceiver.Config{Host:HOST, Port:PORT})

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

func TestRead_call_responseMessage(t * testing.T) 	{

		transceiver, err := netty.NewTransceiver(transceiver.Config{Host:HOST, Port:PORT})

		protocol, err := NewProtocol()
		if err != nil {
			t.Fatal(err)
		}
		requestor := NewRequestor(protocol, transceiver)

		codec, err := protocol.MessageResponseCodec("append")
		if err != nil {
			t.Fatal(err)
		}
		bb := new(bytes.Buffer)
		codec.Encode(bb, Enum{"Status", "OK"})
		t.Logf("Bytes for OK %x", bb.Bytes())

		err = requestor.read_call_responseMessage("append", bb)
		if err != nil {
			t.Fatal(err)
		}

		codec.Encode(bb, Enum{"Status", "FAILED"})
		t.Logf("Bytes for FAILED %x", bb.Bytes())
		err = requestor.read_call_responseMessage("append", bb)
		if err == nil || err.Error() != "Reponse failure. status == FAILED" {
			t.Fatalf("Status FAILED can return error")
		}

	}


const (
	RECV_BUF_LEN = 1024
	NETWORK = "tcp"
	HOST = "127.0.0.1"
	PORT=6666
	ADDR="127.0.0.1:6666"
)


func init() {
	numProcs := runtime.NumCPU()
	if numProcs < 2 {
		numProcs = 2
	}
	runtime.GOMAXPROCS(numProcs)

	listener, err := net.Listen(NETWORK, "0.0.0.0:"+strconv.Itoa(PORT))
	if err != nil {
		println("error listening:", err.Error())
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				println("Error accept:", err.Error())
				return
			}
			go EchoFunc(conn)
		}
	}()
}

func EchoFunc(conn net.Conn) {
	for {
		buf := make([]byte, RECV_BUF_LEN)
		n, err := conn.Read(buf)
		if err != nil {
			println("Error reading:", err.Error())
			return
		}
		println("received ", n, " bytes of data =", string(buf))
		n, err = conn.Write(buf)
		if err != nil {
			println("Error writing:", err.Error())
			return
		}
		println("sended ", n, " bytes of data =", string(buf))
	}
}


