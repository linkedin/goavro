// Package avro provides th log driver for forwarding server logs to
// flume endpoints.
package goavro

import (
        "fmt"
        "net/rpc"
	"bytes"
	"io"
)
type Client struct {
	hostname    string
	extra       map[string]interface{}
	conn		*rpc.Client
	codecFlume	Codec
	codecDocker	Codec
	codecMeta	Codec
	codecString	Codec
	protocol	Protocol
}

const (
	defaultHost = "10.98.80.113"
	defaultPort = "63001"

	hostKey = "avro-host"
	portKey = "avro-port"
)

const AVRO_SCHEMA_LITERAL_HEADER = "flume.avro.schema.literal"


const stringSchema = `
{"type": "string"}
`
const flumeSchema = `
{
 "type": "record",
 "name": "AvroFlumeEvent",
 "fields": [{
   "name": "headers",
   "type": {
     "type": "map",
     "values": "string"
   }
 }, {
   "name": "body",
   "type": "bytes"
 }]
}
`

const recordSchema = `
{
  "type": "record",
  "name": "docker_logs",
  "doc:": "A basic schema for storing docker container logs",
  "namespace": "docker",
  "fields": [
    {
      "doc": "Docker container ID",
      "type": "string",
      "name": "container_id"
    },
    {
      "doc": "Docker container Name",
      "type": "string",
      "name": "container_name"
    },
    {
      "doc": "Docker image ID",
      "type": "string",
      "name": "image_id"
    },
    {
      "doc": "Docker image Name",
      "type": "string",
      "name": "image_name"
    },
    {
      "doc": "Docker container commmand",
      "type": "string",
      "name": "command"
    },
    {
      "doc": "Docker container created timestamp",
      "type": "long",
      "name": "created"
    },
    {
      "doc": "Source of log (stdout, stderr)",
      "type": "string",
      "name": "source"
    },
    {
      "doc": "Docker host",
      "type": "string",
      "name": "docker_host"
    },
    {
      "doc": "Log message",
      "type": "string",
      "name": "log"
    },
    {
      "doc": "Unix timestamp in milliseconds",
      "type": "long",
      "name": "timestamp"
    }
  ]
}
`

func NewDefaultClient() (*Client, error) {
	return NewClient(defaultHost, defaultPort)

}
// New create a avro logger using the configuration passed in on
// the context.
func NewClient(host string, port string) (*Client, error) {
	servAddr  := host+":"+port
        conn, err := rpc.Dial("tcp", servAddr)
        if err != nil {
                return nil, err
        }


        codec, err := NewCodec(flumeSchema)
        if err != nil {
               return nil, err
        }

        codecDocker, err := NewCodec(recordSchema)
        if err != nil {
               return nil, err
        }

        codecMeta, err := NewCodec(metadataSchema)
        if err != nil {
               return nil, err
        }

        codecString, err := NewCodec(stringSchema)
        if err != nil {
               return nil, err
        }

	proto, err := NewProtocol()
        if err != nil {
               return nil, err
        }

        return &Client {
                extra:     make(map[string]interface{}),
                hostname:    "TODO",
		conn:        conn,
		codecFlume:	codec,
		codecDocker:	codecDocker,
		codecMeta:	codecMeta,
		codecString:	codecString,
		protocol: proto,
        }, nil
}

func (a *Client) Write_handshake_request( buffer io.Writer ) (err error) {
	local_hash :=a.protocol.MD5
	// remote_name :="" // only setted by handshake response
	remote_hash := make([]byte,0)
	if len(remote_hash)==0  {
		remote_hash = local_hash
	}

	record, err := NewRecord(RecordSchema(handshakeRequestshema))
        if err != nil {
                return fmt.Errorf("Avro fail to  init record handshakeRequest",err)
        }

	record.Set("clientHash", local_hash)
	record.Set("serverHash", remote_hash)
//	record.Set("clientProtocol", a.protocol.Name)
        codecHandshake, err := NewCodec(handshakeRequestshema)
        if err != nil {
               return err
        }

        if err = codecHandshake.Encode(buffer, record); err !=nil {
                return  fmt.Errorf("Encode handshakeRequest ",err)
        }
	return nil
}


func (a *Client) Log(msg string) error {

	bb := new(bytes.Buffer)
	a.Write_handshake_request(bb)

       flumeRecord, errFlume := NewRecord(RecordSchema(flumeSchema))
        if errFlume != nil {
                return fmt.Errorf("Avro fail to  init record",errFlume)
        }
        headers := make(map[string]interface{})
        headers[AVRO_SCHEMA_LITERAL_HEADER] = stringSchema
      	headers["host_header"] = "127.0.0.1"
        flumeRecord.Set("headers", headers)
        flumeRecord.Set("body", []byte("test"))

	
	// encode metadata
        if err :=  a.codecMeta.Encode(bb, make(map[string]interface{})); err !=nil {
                return  fmt.Errorf("Encode metadata ",err)
        }

	// encode message name
        if err :=  a.codecString.Encode(bb, "append"); err !=nil {
                return  fmt.Errorf("Encode message name ",err)
        }


	// encode message parameters

	if err :=  a.codecFlume.Encode(bb, flumeRecord); err !=nil {
		return  fmt.Errorf("Encode flumeRecord ",err)
	}

        return a.Send(bb.Bytes())
}

func (a *Client) Send(bytes []byte  ) error {
        var reply []byte
	err := a.conn.Call("", bytes,&reply)
        if  err !=nil  {
                return err
        }
	return  nil
}

func (a *Client) Close() error {
	return a.conn.Close()
}


// ValidateLogOpt looks for avro specific log option avro-host avro-port.
func ValidateLogOpt(cfg map[string]string) error {
        for key := range cfg {
                switch key {
                case "env":
                case "labes":
                case hostKey:
                case portKey:
                //  Accepted
                default:
                        return fmt.Errorf("unknown log opt '%s' for avro log driver", key)
                }
        }
        if len(cfg[hostKey]) == 0  {
		cfg[hostKey] = defaultHost
	}
	if len(cfg[portKey]) == 0 {
		cfg[portKey] = defaultPort
        }
        return nil
}

