package goavro

import (
	"testing"

	"bytes"
	"reflect"
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
