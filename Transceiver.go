package goavro

import (
	"bytes"
	"net"
	"encoding/binary"
	"fmt"
)

type Transceiver interface {
	Transceive(request []bytes.Buffer) ([]byte, error)
	RemoteName() string
	SetRemoteName(string)
}


type NettyTransceiver struct {
	sock       *net.TCPConn
	remoteName string
}
func NewNettyTransceiver(sock *net.TCPConn) Transceiver{
	return NettyTransceiver {
		sock: 	sock,
	}
}
func (t NettyTransceiver) RemoteName() string {
	return t.remoteName
	return t.remoteName
	return t.remoteName
}

func (t NettyTransceiver) SetRemoteName(name string) {
	t.remoteName = name
}

func (t NettyTransceiver) Transceive(requests []bytes.Buffer) ([]byte, error){
	nettyFrame := new(bytes.Buffer)
	t.Pack(nettyFrame, requests)

	// Send request
	_, err := t.sock.Write(nettyFrame.Bytes())
	if err!=nil {
		return nil, fmt.Errorf("Fail to write on socket %v", err)
	}
	//sfmt.Fprintf(os.Stdout, "BufferSize %v", nettyFrame)
	// Read Response
	bodyBytes := make([]byte, 1024)
	t.sock.Read(bodyBytes)
	return bodyBytes, nil
}

func (t *NettyTransceiver) Pack(frame *bytes.Buffer, requests []bytes.Buffer) {
	// Set Netty Serial

	nettySerial :=make([]byte, 4)
	binary.BigEndian.PutUint32(nettySerial, uint32(1))
	frame.Write(nettySerial)


	nettySizeBuffer :=make([]byte, 4)
	binary.BigEndian.PutUint32(nettySizeBuffer, uint32(len(requests)))
	frame.Write(nettySizeBuffer)

	for _, request := range requests {
		requestSize :=make([]byte, 4)
		binary.BigEndian.PutUint32(requestSize, uint32(request.Len()))
		frame.Write(requestSize)
		frame.Write(request.Bytes())
	}
}