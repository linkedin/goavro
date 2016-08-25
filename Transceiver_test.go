package goavro

import (
	"testing"

	"bytes"
	"reflect"
	"io/ioutil"
)

func TestPack(t *testing.T) {
	transceiver := new(NettyTransceiver)
	frame := new(bytes.Buffer)
	transceiver.Pack(frame,*new([]bytes.Buffer))
	if frame.Len()!=8 {
		t.Fatalf("Frame not equals to 8: %x", frame.Len())
	}
	reflect.DeepEqual(frame.Bytes(), []byte("\x00\x00\x00\x01\x00\x00\x00\x00"))
	frame.Reset()

	requests:= make([]bytes.Buffer, 2)
	requests[0] = *bytes.NewBuffer([]byte("buf1"))
	requests[1] = *bytes.NewBuffer([]byte("buf2xxxx"))
	transceiver.Pack(frame, requests)
	expectedSize:= 8+ requests[0].Len()+4 + requests[1].Len() + 4
	if frame.Len()!= expectedSize {
		t.Fatalf("Frame not equals to %x: %x / %x",expectedSize,  frame.Len(), frame.Bytes())
	}
}

func TestUnpack(t *testing.T) {
	transceiver := new(NettyTransceiver)
	frame := []byte("\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x01\x0a\x00\x00\x00\x01\x0b")
	respons, err := transceiver.Unpack(frame)
	if err != nil {
		t.Fatalf("%v",err)
	}

	if  len(respons)!=2 {
		t.Fatalf("Number of reponse frame not equals %x / %x",2,  len(respons))
	}

	var resp1 []byte
	var resp2 []byte
	resp1, _  =ioutil.ReadAll(respons[0])
	respons[1].Read(resp2)
	if !reflect.DeepEqual(resp1, []byte("\x0a")) && !reflect.DeepEqual(resp2, []byte("\x0b")) {
		t.Fatalf("Reponse message not equals (0) %x/%x; (1) %x/%x","\x0a", respons[0], "\x0b", respons[1])
	}

}