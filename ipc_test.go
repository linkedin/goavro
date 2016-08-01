package goavro

import (
	"testing"
	"net"

)

func TestRequestor(t *testing.T) {
	conn, err := net.Dial("tcp", "10.98.80.113:63001")
	if err != nil {
		t.Fatal(err)
	}
	transport := NewTransport(&conn)
	protocol, err := NewProtocol()
        if err != nil {
                t.Fatal(err)
        }

	NewRequestor(protocol , *transport)

	response := transport.Transceive([]byte("test"))
	t.Log(response)
}

