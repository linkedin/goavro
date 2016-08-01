package goavro

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"log"
	"os"
)

var REMOTE_HASHES map[string][]byte
var REMOTE_PROTOCOLS map[string]Protocol

var META_WRITER Codec
var HANDSHAKE_REQUESTOR_READER Codec

type Requestor struct {
	// Base class for the client side of protocol interaction.
	local_protocol		Protocol
	transport		Transport
	remote_protocol 	Protocol
	remote_hash		[]byte
	send_protocol		bool
}
func init() {
	var err error
	HANDSHAKE_REQUESTOR_READER, err = NewCodec(handshakeResponseshema)
	if  err!=nil {
	log.Fatal(err)	
	}
	META_WRITER, err = NewCodec(metadataSchema)
        if  err!=nil {
        log.Fatal(err)
        }

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

func (a *Requestor) Request(message_name string, request_datum  []byte)  error {
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
	ok, err := a.read_handshake_response(buffer_decoder)
	if err!=nil {
		return err
	} else if ok {
		a.read_call_response(message_name, buffer_decoder)
	} else {
		a.Request(message_name, request_datum)
	}
	return nil
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
        if err :=  META_WRITER.Encode(buffer, request_metadata); err !=nil {
                return  fmt.Errorf("Encode metadata ",err)
        }
	
	message, found := a.local_protocol.Messages[message_name]
	if !found {
		fmt.Errorf("Unknown message: #{message_name}")
	}
	fmt.Fprint(buffer, message_name)

	return nil
	return a.write_request(message.Request[0].TypeX, request_datum  , buffer)
} 

func (a *Requestor) write_request(request_schema AbsType, request_datum []byte, buffer io.Writer) (err error) {

	codec, err := NewCodec(flumeSchema)
	if err !=nil {
		return
	}
	if err = codec.Encode(buffer, request_datum); err != nil {
		return
	}
	return nil
}

func (a *Requestor) read_handshake_response(decoder io.Reader) (bool, error) {
	datum, err := HANDSHAKE_REQUESTOR_READER.Decode(decoder)
	if err != nil {
		return false, err
	}

	record, ok := datum.(*Record)
	if !ok {
		return false, fmt.Errorf("Fail to decode handshake %T", datum)
	}

	var we_have_matching_schema  =false
	match, err := record.Get("match")
	if err!= nil {
		return false, err
	}
	switch match {
	case "BOTH":
		a.send_protocol  = false
		we_have_matching_schema =true
	case "CLIENT":
                err = fmt.Errorf("Handshake failure. match == CLIENT")
		if a.send_protocol {
			field , err := record.Get("serverProtocol")
		        if err!= nil {
		                return false, err
		        }
			a.remote_protocol = field.(Protocol)
			field, err =  record.Get("serverHash")
                        if err!= nil {
                                return false, err
                        }
			a.remote_hash = field.([]byte)

			a.send_protocol = false
			we_have_matching_schema = true
		}
	case "NONE":
		err = fmt.Errorf("Handshake failure. match == NONE")
		if a.send_protocol {
                        field , err := record.Get("serverProtocol")
                        if err!= nil {
                                return false, err
                        }
			a.remote_protocol = field.(Protocol)
                        field, err =  record.Get("serverHash")
                        if err!= nil {
                                return false, err
                        }
			a.remote_hash = field.([]byte)

			a.send_protocol = true
		}
	default: 
		err = fmt.Errorf("Unexpected match: #{match}")
	}

	return we_have_matching_schema, nil 
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
	fmt.Fprintf(os.Stdout, "Transceive %s", request)
	return new(bytes.Buffer).Bytes()
}	
