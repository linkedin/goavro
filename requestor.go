package goavro

import (
	"bytes"
	"fmt"
	"io"
	"net"
)

var REMOTE_HASHES map[string][]byte
var REMOTE_PROTOCOLS map[string]Protocol

var META_WRITER Codec


type Requestor struct {
	// Base class for the client side of protocol interaction.
	local_protocol		Protocol
	transport		Transport
	remote_protocol 	Protocol
	remote_hash		[]byte
	send_protocol		bool
}

func NewRequestor(localProto Protocol, transport Transport) *Requestor {
	return &Requestor{
		local_protocol: localProto,
		transport: transport,
//		remote_protocol: nil,
//		remote_hash: nil,
//		send_protocol: nil,
	}
}


func (a *Requestor) RemoteProtocol(proto Protocol) {
	a.remote_protocol = proto
	REMOTE_PROTOCOLS[a.transport.RemoteName] = proto
}

func (a *Requestor) RemoteHash(hash []byte) {
	a.remote_hash =  hash
	REMOTE_HASHES[a.transport.RemoteName] = hash
}

func (a *Requestor) Request(message_name string, request_datum  []byte) {
	// wrtie a request message and reads a response or error message.
	// build handshale and call request
	buffer_writer := new(bytes.Buffer)
	a.write_handshake_request(buffer_writer)
	a.write_call_request(message_name, request_datum, buffer_writer)

	// sen the handshake and call request; block until call response
	call_request := buffer_writer.Bytes()
	call_response := a.transport.Transceive(call_request)

	// process the handshake and call response
	buffer_decoder := bytes.NewBuffer(call_response)
	if a.read_handshake_response(buffer_decoder) {
		a.read_call_response(message_name, buffer_decoder)
	} else {
		a.Request(message_name, request_datum)
	}
}

func (a *Requestor) write_handshake_request( buffer io.Writer ) (err error) {
        local_hash :=a.transport.protocol.MD5
        remote_name := a.transport.RemoteName
        remote_hash := REMOTE_HASHES[remote_name]
        if len(remote_hash)==0  {
                remote_hash = local_hash
		a.remote_protocol = a.local_protocol
        }

        record, err := NewRecord(RecordSchema(handshakeRequestshema))
        if err != nil {
                return fmt.Errorf("Avro fail to  init record handshakeRequest",err)
        }

        record.Set("clientHash", local_hash)
        record.Set("serverHash", remote_hash)
        codecHandshake, err := NewCodec(handshakeRequestshema)
        if err != nil {
               return err
        }

	if a.send_protocol {
		json, err := a.local_protocol.Json()
		if err!=nil {		
			return err
		}
		record.Set("clientProtocol", json)
	}

        if err = codecHandshake.Encode(buffer, record); err !=nil {
                return  fmt.Errorf("Encode handshakeRequest ",err)
        }
        return nil
}
func (a *Requestor) write_call_request(message_name string, request_datum []byte, buffer io.Writer) error {
      // The format of a call request is:
      //   * request metadata, a map with values of type bytes
      //   * the message name, an Avro string, followed by
      //   * the message parameters. Parameters are serialized according to
      //     the message's request declaration.

      // TODO request metadata (not yet implemented)
	request_metadata := make(map[string]interface{})

	// encode metadata
        if err :=  META_WRITER.Encode(buffer, make(map[string]interface{})); err !=nil {
                return  fmt.Errorf("Encode metadata ",err)
        }
	
	message := a.local_protocol.Messages[message_name]
	if message==nil {
		fmt.Errorf("Unknown message: #{message_name}")
	}
	buffer.WriteString(message_name)
	return nil
} 

func (a *Requestor) read_handshake_response(decder io.Writer) bool {
	return false // TODO 
}

func (a *Requestor) read_call_response(message_name string, decoder io.Writer) {
}



type Transport struct {
	sock		*net.Conn
	RemoteName	string
	protocol	Protocol
}

func NewTransport(sock *net.Conn) *Transport{
	return & Transport {
		sock: 	sock,
	}
}
func (t *Transport) Transceive(request []byte) []byte{
	return new(bytes.Buffer).Bytes()
}	
