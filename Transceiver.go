package goavro

import (
	"bytes"
	"net"
	"encoding/binary"
	"fmt"
	"io"
)

type Transceiver interface {
	Transceive(request []bytes.Buffer) ([]io.Reader, error)
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

func (t NettyTransceiver) Transceive(requests []bytes.Buffer) ([]io.Reader, error){
	nettyFrame := new(bytes.Buffer)
	t.Pack(nettyFrame, requests)

	// Send request
	_, err := t.sock.Write(nettyFrame.Bytes())
	if err!=nil {
		return nil, fmt.Errorf("Fail to write on socket %v", err)
	}

	// Read Response
	bodyBytes := make([]byte, 1024)
	_, err = t.sock.Read(bodyBytes)
	if err!=nil {
		return nil, fmt.Errorf("Fail to read on socket %v", err)
	}
	return t.Unpack(bodyBytes)
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

func (t *NettyTransceiver) Unpack(frame []byte) ([]io.Reader, error) {
	nettyNumberFame := binary.BigEndian.Uint32(frame[4:8])
	result := make([]io.Reader, nettyNumberFame)
	startFrame := uint32(8)
	i:=uint32(0)
	for i < nettyNumberFame  {
		messageSize := uint32(binary.BigEndian.Uint32(frame[startFrame:startFrame+4]))
		message := frame[startFrame+4:startFrame+4+messageSize]
		startFrame = startFrame+4+messageSize
		br := bytes.NewReader(message)
		result[i] = br
		i++
	}

	return  result, nil
}