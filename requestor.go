package goavro

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
)

var REMOTE_HASHES map[string][]byte
var REMOTE_PROTOCOLS map[string]Protocol

var BUFFER_HEADER_LENGTH = 4
var BUFFER_SIZE = 8192 

var META_WRITER Codec
var META_READER Codec
var HANDSHAKE_REQUESTOR_READER Codec

type Requestor struct {
	// Base class for the client side of protocol interaction.
	local_protocol		Protocol
	transceiver		Transceiver
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
	META_READER, err = NewCodec(metadataSchema)
	if  err!=nil {
		log.Fatal(err)
	}

}

func NewRequestor(localProto Protocol, transceiver Transceiver) *Requestor {
	return &Requestor{
		local_protocol: localProto,
		transceiver: transceiver,
//		remote_protocol: nil,
//		remote_hash: nil,
//		send_protocol: nil,
	}
}


func (a *Requestor) RemoteProtocol(proto Protocol) {
	a.remote_protocol = proto
	REMOTE_PROTOCOLS[a.transceiver.RemoteName()] = proto
}

func (a *Requestor) RemoteHash(hash []byte) {
	a.remote_hash =  hash
	REMOTE_HASHES[a.transceiver.RemoteName()] = hash
}

func (a *Requestor) Request(message_name string, request_datum  interface{})  error {
	// wrtie a request message and reads a response or error message.
	// build handshale and call request
	frame1 := new(bytes.Buffer)
	frame2 := new(bytes.Buffer)

	err := a.write_handshake_request(frame1)
	if err!=nil {
		return err
	}

	err = a.write_call_requestHeader(message_name, frame1)
	if err!=nil {
		return err
	}
	err = a.write_call_request(message_name, request_datum, frame2)
	if err!=nil {
		return err
	}

	// sen the handshake and call request; block until call response
	buffer_writers := []bytes.Buffer{*frame1, *frame2}
	decoder, err := a.transceiver.Transceive(buffer_writers)
	if err!=nil {
		return err
	}
	buffer_decoder := bytes.NewBuffer(decoder)
	// process the handshake and call response
	//ok, err := a.read_handshake_response(buffer_decoder)
	fmt.Sprintf("Response %v", buffer_decoder)
	//if err!=nil {
	//	return err
	//} else if ok {
	//	a.read_call_response(message_name, buffer_decoder)
	//} else {
	//	a.Request(message_name, request_datum)
	//}
	return nil
}

func (a *Requestor) write_handshake_request( buffer io.Writer ) (err error) {
        local_hash :=a.local_protocol.MD5

	//local_hash :=[]byte("\x86\xaa\xda\xe2\xc4\x54\x74\xc0\xfe\x93\xff\xd0\xf2\x35\x0a\x65")
        remote_name := a.remote_protocol.Name
	remote_hash := REMOTE_HASHES[remote_name]
        if len(remote_hash)==0  {
                remote_hash = local_hash
		a.remote_protocol = a.local_protocol
        }

        record, err := NewRecord(RecordSchema(handshakeRequestshema))
        if err != nil {
                return fmt.Errorf("Avro fail to  init record handshakeRequest %v",err)
        }

        record.Set("clientHash", local_hash)
        record.Set("serverHash", remote_hash)
	record.Set("meta", make(map[string]interface{}))
        codecHandshake, err := NewCodec(handshakeRequestshema)
        if err != nil {
               return fmt.Errorf("Avro fail to  get codec handshakeRequest %v",err)
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

func (a *Requestor) write_call_request(message_name string, request_datum interface{}, frame io.Writer) (err error) {
	codec, err := a.local_protocol.getMessageRequestCodec(message_name)

	if err != nil {
		return fmt.Errorf("fail to get codec for message %s:  %v", message_name, err)
	}
	a.write_request(codec, request_datum, frame)
	return err
}

func (a *Requestor) write_call_requestHeader(message_name string, frame1 io.Writer) error {
	// The format of a call request is:
	//   * request metadata, a map with values of type bytes
	//   * the message name, an Avro string, followed by
	//   * the message parameters. Parameters are serialized according to
	//     the message's request declaration.

	// TODO request metadata (not yet implemented)
	request_metadata := make(map[string]interface{})
	// encode metadata
	if err := META_WRITER.Encode(frame1, request_metadata); err != nil {
		return fmt.Errorf("Encode metadata ", err)
	}


	stringCodec.Encode(frame1,message_name)
	return nil
} 

func (a *Requestor) write_request(request_codec Codec, request_datum interface{}, buffer io.Writer) error {


	if err := request_codec.Encode(buffer, request_datum); err != nil {
		return fmt.Errorf("Fail to encode request_datum %v", err)
	}
	return nil
}

func (a *Requestor) read_handshake_response(decoder io.Reader) (bool, error) {
	resp, _ := ioutil.ReadAll(decoder)
	datum, err := HANDSHAKE_REQUESTOR_READER.Decode(decoder)
	if err != nil {

		return false,fmt.Errorf("Fail to decode %v with error %v", resp, err)
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
	// The format of a call response is:
	//   * response metadata, a map with values of type bytes
	//   * a one-byte error flag boolean, followed by either:
	//     * if the error flag is false,
	//       the message response, serialized per the message's response schema.
	//     * if the error flag is true,
	//       the error, serialized per the message's error union schema.
//	META_READER.Decode(decoder)
}



