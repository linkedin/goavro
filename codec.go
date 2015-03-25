// Copyright 2015 LinkedIn Corp. Licensed under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License.  You may obtain a copy of the License
// at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.Copyright [201X] LinkedIn Corp. Licensed under the Apache
// License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.

// Goavro is a library that encodes and decodes of Avro data. It
// provides an interface to encode data directly to io.Writer streams,
// and to decode data from io.Reader streams. Goavro fully adheres to
// version 1.7.7 of the Avro specification and data encoding.
package goavro

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
)

// Decoder interface specifies structures that may be decoded.
type Decoder interface {
	Decode(io.Reader) (interface{}, error)
}

// Encoder interface specifies structures that may be encoded.
type Encoder interface {
	Encode(io.Writer, interface{}) error
}

// The Codec interface supports both Decode and Encode operations.
type Codec interface {
	Decoder
	Encoder
}

// CodecSetter functions are those those which are used to modify a
// newly instantiated Codec.
type CodecSetter func(Codec) error

type decoderFunction func(io.Reader) (interface{}, error)
type encoderFunction func(io.Writer, interface{}) error

type codec struct {
	nm *name
	df decoderFunction
	ef encoderFunction
}

// String returns a string representation of the codec.
func (c codec) String() string {
	return fmt.Sprintf("nm: %v, df: %v, ef: %v", c.nm, c.df, c.ef)
}

type symtab map[string]*codec // map full name to codec

// NewCodec creates a new object that supports both the Decode and
// Encode methods. It requires an Avro schema, expressed as a JSON
// string.
//
//   codec, err := goavro.NewCodec(someJsonSchema)
//   if err != nil {
//       return nil, err
//   }
//
//   // Decoding data uses codec created above, and an io.Reader,
//   // definition not shown:
//   datum, err := codec.Decode(r)
//   if err != nil {
//       return nil, err
//   }
//
//   // Encoding data uses codec created above, an io.Writer,
//   // definition not shown, and some data:
//   err := codec.Encode(w, datum)
//   if err != nil {
//       return nil, err
//   }
//
//   // Encoding data using bufio.Writer to buffer the writes
//   // during data encoding:
//
//   func encodeWithBufferedWriter(c Codec, w io.Writer, datum interface{}) error {
//	bw := bufio.NewWriter(w)
//	err := c.Encode(bw, datum)
//	if err != nil {
//		return err
//	}
//	return bw.Flush()
//   }
//
//   err := encodeWithBufferedWriter(codec, w, datum)
//   if err != nil {
//       return nil, err
//   }
func NewCodec(someJsonSchema string, setters ...CodecSetter) (Codec, error) {
	var schema interface{}
	err := json.Unmarshal([]byte(someJsonSchema), &schema)
	if err != nil {
		err = fmt.Errorf("cannot parse schema string: %#v: %v", someJsonSchema, err)
	}

	// each codec gets a unified namespace of symbols to
	// respective codecs
	st := make(symtab)

	newCodec, err := st.buildCodec(nullNamespace, schema)
	if err != nil {
		return nil, err
	}

	for _, setter := range setters {
		err = setter(newCodec)
		if err != nil {
			return nil, err
		}
	}
	return newCodec, nil
}

// Decode will read from the specified io.Reader, and return the next
// datum from the stream, or an error explaining why the stream cannot
// be converted into the Codec's schema.
func (c codec) Decode(r io.Reader) (interface{}, error) {
	return c.df(r)
}

// Encode will write the specified datum to the specified io.Writer,
// or return an error explaining why the datum cannot be converted
// into the Codec's schema.
func (c codec) Encode(w io.Writer, datum interface{}) error {
	return c.ef(w, datum)
}

var (
	nullCodec, booleanCodec, intCodec, longCodec, floatCodec, doubleCodec, bytesCodec, stringCodec *codec
)

func init() {
	// NOTE: use Go type names because for runtime resolution of
	// union member, it gets the Go type name of the datum sent to
	// the union encoder, and uses that string as a key into the
	// encoders map
	nullCodec = &codec{nm: &name{n: "null"}, df: nullDecoder, ef: nullEncoder}
	booleanCodec = &codec{nm: &name{n: "bool"}, df: booleanDecoder, ef: booleanEncoder}
	intCodec = &codec{nm: &name{n: "int32"}, df: intDecoder, ef: intEncoder}
	longCodec = &codec{nm: &name{n: "int64"}, df: longDecoder, ef: longEncoder}
	floatCodec = &codec{nm: &name{n: "float32"}, df: floatDecoder, ef: floatEncoder}
	doubleCodec = &codec{nm: &name{n: "float64"}, df: doubleDecoder, ef: doubleEncoder}
	bytesCodec = &codec{nm: &name{n: "[]uint8"}, df: bytesDecoder, ef: bytesEncoder}
	stringCodec = &codec{nm: &name{n: "string"}, df: stringDecoder, ef: stringEncoder}
}

func (st symtab) buildCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	switch schemaType := schema.(type) {
	case string:
		return st.buildString(enclosingNamespace, schemaType, schema)
	case []interface{}:
		return st.makeUnionCodec(enclosingNamespace, schema)
	case map[string]interface{}:
		return st.buildMap(enclosingNamespace, schema.(map[string]interface{}))
	default:
		return nil, fmt.Errorf("unknown schema type: %T", schema)
	}
}

func (st symtab) buildMap(enclosingNamespace string, schema map[string]interface{}) (*codec, error) {
	t, ok := schema["type"]
	if !ok {
		return nil, fmt.Errorf("schema ought have type: %v", schema)
	}
	switch t.(type) {
	case string:
		// EXAMPLE: "type":"int"
		// EXAMPLE: "type":"enum"
		return st.buildString(enclosingNamespace, t.(string), schema)
	case map[string]interface{}, []interface{}:
		// EXAMPLE: "type":{"type":fixed","name":"fixed_16","size":16}
		// EXAMPLE: "type":["null","int"]
		return st.buildCodec(enclosingNamespace, t)
	default:
		return nil, fmt.Errorf("schema type ought to be either string, map[string]interface{}, or []interface{}: %T", t)
	}
}

func (st symtab) buildString(enclosingNamespace, typeName string, schema interface{}) (*codec, error) {
	switch typeName {
	case "null":
		return nullCodec, nil
	case "boolean":
		return booleanCodec, nil
	case "int":
		return intCodec, nil
	case "long":
		return longCodec, nil
	case "float":
		return floatCodec, nil
	case "double":
		return doubleCodec, nil
	case "bytes":
		return bytesCodec, nil
	case "string":
		return stringCodec, nil
	case "record":
		return st.makeRecordCodec(enclosingNamespace, schema)
	case "enum":
		return st.makeEnumCodec(enclosingNamespace, schema)
	case "fixed":
		return st.makeFixedCodec(enclosingNamespace, schema)
	case "map":
		return st.makeMapCodec(enclosingNamespace, schema)
	case "array":
		return st.makeArrayCodec(enclosingNamespace, schema)
	default:
		t, err := newName(nameName(typeName), nameEnclosingNamespace(enclosingNamespace))
		if err != nil {
			return nil, fmt.Errorf("could not normalize name (%s): %s", enclosingNamespace, typeName)
		}
		c, ok := st[t.n]
		if !ok {
			return nil, fmt.Errorf("unknown type name: %s", t.n)
		}
		return c, nil
	}
}

type unionEncoder struct {
	ef    encoderFunction
	index int32
}

func (st symtab) makeUnionCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	errorNamespace := "null namespace"
	if enclosingNamespace != nullNamespace {
		errorNamespace = enclosingNamespace
	}
	cannotCreate := makeErrorReporter("cannot create union (%s): ", errorNamespace)

	// schema checks
	schemaArray, ok := schema.([]interface{})
	if !ok {
		return nil, cannotCreate("union ought to be array: %T", schema)
	}
	if len(schemaArray) == 0 {
		return nil, cannotCreate("union ought have at least one member")
	}

	// setup
	nameToUnionEncoder := make(map[string]unionEncoder)
	indexToDecoder := make([]decoderFunction, len(schemaArray))
	allowedNames := make([]string, len(schemaArray))

	for idx, unionMemberSchema := range schemaArray {
		c, err := st.buildCodec(enclosingNamespace, unionMemberSchema)
		if err != nil {
			return nil, cannotCreate("union member ought to be decodable: %v", err)
		}
		allowedNames[idx] = c.nm.n
		indexToDecoder[idx] = c.df
		nameToUnionEncoder[c.nm.n] = unionEncoder{ef: c.ef, index: int32(idx)}
	}

	invalidType := "datum ought match schema: expected: "
	invalidType += strings.Join(allowedNames, ", ")
	invalidType += "; actual: "

	nm, _ := newName(nameName("union"))
	cannotDecode := makeErrorReporter("cannot decode union (%s): ", nm.n)
	cannotEncode := makeErrorReporter("cannot encode union (%s): ", nm.n)

	return &codec{
		nm: nm,
		df: func(r io.Reader) (interface{}, error) {
			i, err := intDecoder(r)
			if err != nil {
				return nil, cannotDecode("%v", err)
			}
			idx, ok := i.(int32)
			if !ok {
				return nil, cannotDecode("expected: int; actual: %T", i)
			}
			index := int(idx)
			if index < 0 || index >= len(indexToDecoder) {
				return nil, cannotDecode("index must be between 0 and %d", enclosingNamespace, len(indexToDecoder)-1)
			}
			return indexToDecoder[index](r)
		},
		ef: func(w io.Writer, datum interface{}) error {
			var err error
			var name string
			switch datum.(type) {
			default:
				name = reflect.TypeOf(datum).String()
			case map[string]interface{}:
				name = "map"
			case []interface{}:
				name = "array"
			case nil:
				name = "null"
			case *Record:
				name = datum.(*Record).Name
			}
			ue, ok := nameToUnionEncoder[name]
			if !ok {
				return cannotEncode(invalidType + name)
			}
			err = intEncoder(w, ue.index)
			if err != nil {
				return cannotEncode("%v", err)
			}
			err = ue.ef(w, datum)
			if err != nil {
				return cannotEncode("%v", err)
			}
			return nil
		},
	}, nil
}

func (st symtab) makeEnumCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	errorNamespace := "null namespace"
	if enclosingNamespace != nullNamespace {
		errorNamespace = enclosingNamespace
	}
	cannotCreate := makeErrorReporter("cannot create enum (%s): ", errorNamespace)

	// schema checks
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, cannotCreate("expected: map[string]interface{}; actual: %T", schema)
	}
	nm, err := newName(nameEnclosingNamespace(enclosingNamespace), nameSchema(schemaMap))
	if err != nil {
		return nil, err
	}
	s, ok := schemaMap["symbols"]
	if !ok {
		return nil, cannotCreate("ought to have symbols key")
	}
	symtab, ok := s.([]interface{})
	if !ok || len(symtab) == 0 {
		return nil, cannotCreate("symbols ought to be non-empty array")
	}
	for _, v := range symtab {
		_, ok := v.(string)
		if !ok {
			return nil, cannotCreate("symbols array member ought to be string")
		}
	}
	cannotDecode := makeErrorReporter("cannot decode enum (%s): ", nm.n)
	cannotEncode := makeErrorReporter("cannot encode enum (%s): ", nm.n)
	c := &codec{
		nm: nm,
		df: func(r io.Reader) (interface{}, error) {
			someValue, err := longDecoder(r)
			if err != nil {
				return nil, cannotDecode("%v", err)
			}
			index, ok := someValue.(int64)
			if !ok {
				return nil, cannotDecode("expected long; actual: %T", someValue)
			}
			if index < 0 || index >= int64(len(symtab)) {
				return nil, cannotDecode("index must be between 0 and %d", len(symtab)-1)
			}
			return symtab[index], nil
		},
		ef: func(w io.Writer, datum interface{}) error {
			someString, ok := datum.(string)
			if !ok {
				return cannotEncode("expected: string; actual: %T", datum)
			}
			for idx, symbol := range symtab {
				if symbol == someString {
					if err := longEncoder(w, int64(idx)); err != nil {
						return cannotEncode("%v", err)
					}
					return nil
				}
			}
			return cannotEncode("symbol not defined: %s", someString)
		},
	}
	st[nm.n] = c
	return c, nil
}

func (st symtab) makeFixedCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	errorNamespace := "null namespace"
	if enclosingNamespace != nullNamespace {
		errorNamespace = enclosingNamespace
	}
	cannotCreate := makeErrorReporter("cannot create fixed (%s): ", errorNamespace)

	// schema checks
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, cannotCreate("expected: map[string]interface{}; actual: %T", schema)
	}
	nm, err := newName(nameSchema(schemaMap), nameEnclosingNamespace(enclosingNamespace))
	if err != nil {
		return nil, err
	}
	s, ok := schemaMap["size"]
	if !ok {
		return nil, cannotCreate("ought to have size key")
	}
	fs, ok := s.(float64)
	if !ok {
		return nil, cannotCreate("size ought to be number: %T", s)
	}
	size := int32(fs)
	cannotDecode := makeErrorReporter("cannot decode fixed (%s): ", nm.n)
	cannotEncode := makeErrorReporter("cannot encode fixed (%s): ", nm.n)
	c := &codec{
		nm: nm,
		df: func(r io.Reader) (interface{}, error) {
			buf := make([]byte, size)
			n, err := r.Read(buf)
			if err != nil {
				return nil, cannotDecode("%v", err)
			}
			if n < int(size) {
				return nil, cannotDecode("buffer underrun")
			}
			return buf, nil
		},
		ef: func(w io.Writer, datum interface{}) error {
			someBytes, ok := datum.([]byte)
			if !ok {
				return cannotEncode("expected: []byte; actual: %T", datum)
			}
			if len(someBytes) != int(size) {
				return cannotEncode("expected: %d bytes; actual: %d", size, len(someBytes))
			}
			n, err := w.Write(someBytes)
			if err != nil {
				return cannotEncode("%v", err)
			}
			if n != int(size) {
				return cannotEncode("buffer underrun")
			}
			return nil
		},
	}
	st[nm.n] = c
	return c, nil
}

func (st symtab) makeRecordCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	errorNamespace := "null namespace"
	if enclosingNamespace != nullNamespace {
		errorNamespace = enclosingNamespace
	}
	cannotCreate := makeErrorReporter("cannot create record (%s): ", errorNamespace)

	// delegate schema checks to NewRecord()
	recordTemplate, err := NewRecord(RecordSchema(schema), RecordEnclosingNamespace(enclosingNamespace))
	if err != nil {
		return nil, cannotCreate("%v", err)
	}

	fieldCodecs := make([]*codec, len(recordTemplate.Fields))
	for idx, field := range recordTemplate.Fields {
		var err error
		fieldCodecs[idx], err = st.buildCodec(recordTemplate.n.namespace(), field.schema)
		if err != nil {
			return nil, cannotCreate("record field ought to be codec: %+v: %v", st, err)
		}
	}

	cannotDecode := makeErrorReporter("cannot decode record (%s): ", recordTemplate.Name)
	cannotEncode := makeErrorReporter("cannot encode record (%s): ", recordTemplate.Name)

	c := &codec{
		nm: recordTemplate.n,
		df: func(r io.Reader) (interface{}, error) {
			someRecord, _ := NewRecord(RecordSchema(schema), RecordEnclosingNamespace(enclosingNamespace))
			for idx, codec := range fieldCodecs {
				value, err := codec.Decode(r)
				if err != nil {
					return nil, cannotDecode("%v", err)
				}
				someRecord.Fields[idx].Datum = value
			}
			return someRecord, nil
		},
		ef: func(w io.Writer, datum interface{}) error {
			someRecord, ok := datum.(*Record)
			if !ok {
				return cannotEncode("expected: Record; actual: %T", datum)
			}
			if someRecord.Name != recordTemplate.Name {
				return cannotEncode("expected: %v; actual: %v", recordTemplate.Name, someRecord.Name)
			}
			for idx, field := range someRecord.Fields {
				var value interface{}
				// check whether field datum is valid
				if reflect.ValueOf(field.Datum).IsValid() {
					value = field.Datum
				} else if reflect.ValueOf(field.defval).IsValid() {
					value = field.defval
				} else {
					return cannotEncode("field has no data and no default set: %v", field.Name)
				}
				err = fieldCodecs[idx].Encode(w, value)
				if err != nil {
					return cannotEncode("%v", err)
				}
			}
			return nil
		},
	}
	st[recordTemplate.Name] = c
	return c, nil
}

func (st symtab) makeMapCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	errorNamespace := "null namespace"
	if enclosingNamespace != nullNamespace {
		errorNamespace = enclosingNamespace
	}
	cannotCreate := makeErrorReporter("cannot create map (%s): ", errorNamespace)

	// schema checks
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, cannotCreate("expected: map[string]interface{}; actual: %T", schema)
	}
	v, ok := schemaMap["values"]
	if !ok {
		return nil, cannotCreate("ought to have values key")
	}
	valuesCodec, err := st.buildCodec(enclosingNamespace, v)
	if err != nil {
		return nil, cannotCreate("%v", err)
	}

	cannotDecode := makeErrorReporter("cannot decode map (%s): ", enclosingNamespace)
	cannotEncode := makeErrorReporter("cannot encode map (%s): ", enclosingNamespace)

	nm := &name{n: "map"}

	return &codec{
		nm: nm,
		df: func(r io.Reader) (interface{}, error) {
			data := make(map[string]interface{})
			someValue, err := longDecoder(r)
			if err != nil {
				return nil, cannotDecode("%v", err)
			}
			blockCount := someValue.(int64)

			for blockCount != 0 {
				if blockCount < 0 {
					blockCount = -blockCount
					// read and discard number of bytes in block
					_, err := longDecoder(r)
					if err != nil {
						return nil, cannotDecode("%v", err)
					}
				}
				for i := int64(0); i < blockCount; i++ {
					someValue, err := stringDecoder(r)
					if err != nil {
						return nil, cannotDecode("%v", err)
					}
					mapKey, ok := someValue.(string)
					if !ok {
						return nil, cannotDecode("key ought to be string")
					}
					datum, err := valuesCodec.df(r)
					if err != nil {
						return nil, err
					}
					data[mapKey] = datum
				}
				someValue, err = longDecoder(r)
				if err != nil {
					return nil, cannotDecode("%v", err)
				}
				blockCount = someValue.(int64)
			}
			return data, nil
		},
		ef: func(w io.Writer, datum interface{}) error {
			dict, ok := datum.(map[string]interface{})
			if !ok {
				return cannotEncode("expected: map[string]interface{}; actual: %T", datum)
			}
			if err = longEncoder(w, int64(1)); err != nil {
				return cannotEncode("%v", err)
			}
			for k, v := range dict {
				if err = stringEncoder(w, k); err != nil {
					return cannotEncode("%v", err)
				}
				if err = valuesCodec.ef(w, v); err != nil {
					return cannotEncode("%v", err)
				}
			}
			if err = longEncoder(w, int64(0)); err != nil {
				return cannotEncode("%v", err)
			}
			return nil
		},
	}, nil
}

func (st symtab) makeArrayCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	errorNamespace := "null namespace"
	if enclosingNamespace != nullNamespace {
		errorNamespace = enclosingNamespace
	}
	cannotCreate := makeErrorReporter("cannot create array (%s): ", errorNamespace)

	// schema checks
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, cannotCreate("expected: map[string]interface{}; actual: %T", schema)
	}
	v, ok := schemaMap["items"]
	if !ok {
		return nil, cannotCreate("ought to have items key")
	}
	valuesCodec, err := st.buildCodec(enclosingNamespace, v)
	if err != nil {
		return nil, cannotCreate("%v", err)
	}

	cannotDecode := makeErrorReporter("cannot decode array (%s): ", enclosingNamespace)
	cannotEncode := makeErrorReporter("cannot encode array (%s): ", enclosingNamespace)

	const itemsPerArrayBlock = 10
	nm := &name{n: "array"}

	return &codec{
		nm: nm,
		df: func(r io.Reader) (interface{}, error) {
			data := make([]interface{}, 0)

			someValue, err := longDecoder(r)
			if err != nil {
				return nil, cannotDecode("%v", err)
			}
			blockCount := someValue.(int64)

			for blockCount != 0 {
				if blockCount < 0 {
					blockCount = -blockCount
					// read and discard number of bytes in block
					_, err = longDecoder(r)
					if err != nil {
						return nil, cannotDecode("%v", err)
					}
				}
				for i := int64(0); i < blockCount; i++ {
					datum, err := valuesCodec.df(r)
					if err != nil {
						return nil, cannotDecode("%v", err)
					}
					data = append(data, datum)
				}
				someValue, err = longDecoder(r)
				if err != nil {
					return nil, cannotDecode("%v", err)
				}
				blockCount = someValue.(int64)
			}
			return data, nil
		},
		ef: func(w io.Writer, datum interface{}) error {
			someArray, ok := datum.([]interface{})
			if !ok {
				return cannotEncode("expected: []interface{}; actual: %T", datum)
			}
			for leftIndex := 0; leftIndex < len(someArray); leftIndex += itemsPerArrayBlock {
				rightIndex := leftIndex + itemsPerArrayBlock
				if rightIndex > len(someArray) {
					rightIndex = len(someArray)
				}
				items := someArray[leftIndex:rightIndex]
				err = longEncoder(w, int64(len(items)))
				if err != nil {
					return cannotEncode("%v", err)
				}
				for _, item := range items {
					err = valuesCodec.ef(w, item)
					if err != nil {
						return cannotEncode("%v", err)
					}
				}
			}
			return longEncoder(w, int64(0))
		},
	}, nil
}
