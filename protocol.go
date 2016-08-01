package goavro

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
)

var TYPES_CACHE map[string]ProtocolType

type Protocol struct {
        Name string             `json:"protocol"`
	Namespace string	`json:"namespace"`
	Fullname string		`json:"-"`
	Doc string		`json:"doc"`
	Types []AbsType		`json:"types"`
	Messages map[string]ProtocolMessage `json:"messages"`
	MD5	[]byte		`json:"-"`
}

type ProtocolType struct {
	TypeX string	`json:"type"`
	Name string	`json:"name,omitempty"`
	Symbols []string `json:"symbols,omitempty"`
	Fields []Field	`json:"fields,omitempty"`
	Values string	`json:"values,omitempty"`
	Items string	`json:"items,omitempty"`
}

type Field struct{
	Name string     `json:"name"`
	TypeX AbsType	`json:"type"`
}

type AbsType struct {
	*ProtocolType
	ref string 
}

type ProtocolMessage struct {
	Name	string		`json:"-"`
	Doc	string		`json:"doc,omitempty"`
	Request []Field	`json:"request"`
	Response string		`json:"response"`
	Errors []string		`json:"errors,omitempty"`
	One_way bool		`json:"one-way,omitempty"`
}

const proto = `
{
   "protocol":"AvroSourceProtocol",
   "namespace":"org.apache.flume.source.avro",
   "doc":"* Licensed to the Apache Software Foundation (ASF).",
   "types":[
      {
         "type":"enum",
         "name":"Status",
         "symbols":[
            "OK",
            "FAILED",
            "UNKNOWN"
         ]
      },
      {
         "type":"record",
         "name":"AvroFlumeEvent",
         "fields":[
            {
               "name":"headers",
               "type":{
                  "type":"map",
                  "values":"string"
               }
            },
            {
               "name":"body",
               "type":"bytes"
            }
         ]
      }
   ],
   "messages":{
      "append":{
         "request":[
            {
               "name":"event",
               "type":"AvroFlumeEvent"
            }
         ],
         "response":"Status"
      },
      "appendBatch":{
         "request":[
            {
               "name":"events",
               "type":{
                  "type":"array",
                  "items":"AvroFlumeEvent"
               }
            }
         ],
         "response":"Status"
      }
   }
}
`

func init() {
	TYPES_CACHE = make(map[string]ProtocolType)
	TYPES_CACHE["bytes"] = ProtocolType{Name:"bytes", TypeX:"bytes"}
}
func (t *AbsType) UnmarshalJSON(data []byte) error {
	var nameType	string
	var protocolType ProtocolType
	if err := json.Unmarshal(data, &nameType); err==nil {
		protoType, ok := TYPES_CACHE[nameType]
		if ok {
			t.ref = nameType
			t.ProtocolType = &protoType
		} else {
			return fmt.Errorf("Type %s not found on protocol type cache %#v", data, TYPES_CACHE)
		}
	} else if err := json.Unmarshal(data, &protocolType); err!=nil {
		return fmt.Errorf("Fail to Parse AbsType, %s  %s", data,err )
	}
                t.ProtocolType = &protocolType
                TYPES_CACHE[protocolType.Name] = protocolType
	return nil
}

func (t *AbsType) MarshalJSON()([]byte, error) {
	if len(t.ref)>0 {
		return json.Marshal(t.ref)
	} else {
		return json.Marshal(t.ProtocolType)
	}
}

func NewProtocol() (Protocol, error) {
	var result Protocol 
	err := json.Unmarshal([]byte(proto), &result)

	if err!=nil {
		return result, err
	}

	if len(result.Name)==0 {
		err = fmt.Errorf("Protocol must have a non-empty name.")
	} else if len(result.Namespace) == 0 {
		err = fmt.Errorf("The namespace property must be a string.")
	}
	result.Fullname = result.Namespace +"." +  result.Name
	hasher := md5.New()
	hasher.Write([]byte(proto))
	result.MD5 = hasher.Sum(nil)
	return result, err
}

func (p *Protocol) Json() (string, error) {
	var result string
	bb, err := json.Marshal(p)
	if err != nil {
		return result, err

	}
	return string(bb), nil
}
