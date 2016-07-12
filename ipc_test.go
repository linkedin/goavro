package goavro

import (
	"testing"
	"bytes"
)


func TestNewClient(t *testing.T) {
        client, err := NewDefaultClient()
        if err!=nil {
                        t.Fatal("%v",err)
        }
        t.Logf("proto %#v",client) 
	client.Close()
}

func TestHandshake(t *testing.T) {
        client, err := NewDefaultClient()
        if err!=nil {
                        t.Fatal("%v",err)
        }
        bb := new(bytes.Buffer)
        err = client.Write_handshake_request(bb)
        if err !=nil {
                t.Fatal("%v", err)
        }
	t.Logf("handshake message %#v", bb.Bytes())
	client.Send(bb.Bytes())
	client.Close()

}


func TestLog(t *testing.T) {
        client, err := NewDefaultClient()
        if err!=nil {
                        t.Fatal("%v",err)
        }
	err = client.Log("Message test")
	if err !=nil {
		t.Fatal("%v", err)
	}
client.Close()
	
}

