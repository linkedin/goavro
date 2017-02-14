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

// Package goavro is a library that encodes and decodes of Avro
// data. It provides an interface to encode data directly to io.Writer
// streams, and to decode data from io.Reader streams. Goavro fully
// adheres to version 1.7.7 of the Avro specification and data
// encoding.
package goavro

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

// NOTE: use Go type names because for runtime resolution of
// union member, it gets the Go type name of the datum sent to
// the union encoder, and uses that string as a key into the
// encoders map
func newJSONSymbolTable() *symtabJSON {
	return &symtabJSON{
		name:         make(map[string]*codec),
		nullCodec:    &codec{nm: &name{n: "null"}, df: nullJSONDecoder, ef: nullJSONEncoder},
		booleanCodec: &codec{nm: &name{n: "bool"}, df: booleanJSONDecoder, ef: booleanJSONEncoder},
		intCodec:     &codec{nm: &name{n: "int32"}, df: intJSONDecoder, ef: intJSONEncoder},
		longCodec:    longJSONCodec(),
		floatCodec:   &codec{nm: &name{n: "float32"}, df: floatJSONDecoder, ef: floatJSONEncoder},
		doubleCodec:  &codec{nm: &name{n: "float64"}, df: doubleJSONDecoder, ef: doubleJSONEncoder},
		bytesCodec:   &codec{nm: &name{n: "[]uint8"}, df: bytesJSONDecoder, ef: bytesJSONEncoder},
		stringCodec:  &codec{nm: &name{n: "string"}, df: stringJSONDecoder, ef: stringJSONEncoder},
	}

}

func longJSONCodec() *codec {
	return &codec{nm: &name{n: "int64"}, df: longJSONDecoder, ef: longJSONEncoder}
}

type symtabJSON struct {
	name map[string]*codec // map full name to codec

	//cache primitive codecs
	nullCodec    *codec
	booleanCodec *codec
	intCodec     *codec
	longCodec    *codec
	floatCodec   *codec
	doubleCodec  *codec
	bytesCodec   *codec
	stringCodec  *codec
}

// NewJSONCodec creates a new object that supports both the Decode and
// Encode methods. It requires an Avro schema, expressed as a JSON
// string.
//
//   codec, err := goavro.NewCodec(someJSONSchema)
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
func NewJSONCodec(someJSONSchema string, setters ...CodecSetter) (Codec, error) {
	// unmarshal into schema blob
	var schema interface{}
	if err := json.Unmarshal([]byte(someJSONSchema), &schema); err != nil {
		return nil, &ErrSchemaParse{"cannot unmarshal JSON", err}
	}
	// remarshal back into compressed json
	compressedSchema, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal schema: %v", err)
	}

	// each codec gets a unified namespace of symbols to
	// respective codecs
	st := newJSONSymbolTable()

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
	newCodec.schema = string(compressedSchema)
	return newCodec, nil
}

func (st symtabJSON) buildCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	switch schemaType := schema.(type) {
	case string:
		return st.buildString(enclosingNamespace, schemaType, schema)
	case []interface{}:
		return st.makeUnionCodec(enclosingNamespace, schema)
	case map[string]interface{}:
		return st.buildMap(enclosingNamespace, schema.(map[string]interface{}))
	default:
		return nil, newCodecBuildError("unknown", "schema type: %T", schema)
	}
}

func (st symtabJSON) buildMap(enclosingNamespace string, schema map[string]interface{}) (*codec, error) {
	t, ok := schema["type"]
	if !ok {
		return nil, newCodecBuildError("map", "ought have type: %v", schema)
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
		return nil, newCodecBuildError("map", "type ought to be either string, map[string]interface{}, or []interface{}; received: %T", t)
	}
}

func (st symtabJSON) buildString(enclosingNamespace, typeName string, schema interface{}) (*codec, error) {
	switch typeName {
	case "null":
		return st.nullCodec, nil
	case "boolean":
		return st.booleanCodec, nil
	case "int":
		return st.intCodec, nil
	case "long":
		return st.longCodec, nil
	case "float":
		return st.floatCodec, nil
	case "double":
		return st.doubleCodec, nil
	case "bytes":
		return st.bytesCodec, nil
	case "string":
		return st.stringCodec, nil
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
			return nil, newCodecBuildError(typeName, "could not normalize name: %q: %q: %s", enclosingNamespace, typeName, err)
		}
		c, ok := st.name[t.n]
		if !ok {
			return nil, newCodecBuildError("unknown", "unknown type name: %s", t.n)
		}
		return c, nil
	}
}

type unionJSONEncoder struct {
	ef  encoderFunction
	utn string
}

// Given a union schema figure out the union type name.
func getUnionTypeName(friendlyName string, enclosingNamespace string, schema interface{}) (string, error) {
	// A schema can be a primitive or a complex type.
	// Primitive types can be encoded as just "primitive" or {"type": "primitive"}.

	switch schema.(type) {
	case string:
		// "primitive"
		return schema.(string), nil
	}

	schemaJSONMap, ok := schema.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("Unsupported type in union schema %v", schema)
	}

	unionType, ok := schemaJSONMap["type"]
	if !ok {
		return "", fmt.Errorf("Type field is missing in union schema %v", schema)
	}

	unionTypeName, ok := unionType.(string)
	if !ok {
		return "", fmt.Errorf("Type attribute has to be a string in union schema %v", schema)
	}

	switch unionTypeName {
	case "record", "enum", "fixed":
		// The union type name is the fully qualified name of the named type.
		name, err := newName(nameSchema(schemaJSONMap), nameEnclosingNamespace(enclosingNamespace))
		if err != nil {
			return "", err
		}
		unionTypeName = name.n
	}

	return unionTypeName, nil
}

func (st symtabJSON) makeUnionCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	errorNamespace := "null namespace"
	if enclosingNamespace != nullNamespace {
		errorNamespace = enclosingNamespace
	}
	friendlyName := fmt.Sprintf("union (%s)", errorNamespace)

	// schema checks
	schemaArray, ok := schema.([]interface{})
	if !ok {
		return nil, newCodecBuildError(friendlyName, "ought to be array: %T", schema)
	}
	if len(schemaArray) == 0 {
		return nil, newCodecBuildError(friendlyName, " ought have at least one member")
	}

	// setup
	nameToUnionEncoder := make(map[string]unionJSONEncoder)
	nameToJSONDecoder := make(map[string]decoderFunction)

	for _, unionMemberSchema := range schemaArray {
		c, err := st.buildCodec(enclosingNamespace, unionMemberSchema)
		if err != nil {
			return nil, newCodecBuildError(friendlyName, "member ought to be decodable: %s", err)
		}
		unionTypeName, err := getUnionTypeName(friendlyName, enclosingNamespace, unionMemberSchema)
		if err != nil {
			return nil, newCodecBuildError(friendlyName, "Can't get union type name: %s", err)
		}
		nameToJSONDecoder[unionTypeName] = c.df
		nameToUnionEncoder[c.nm.n] = unionJSONEncoder{ef: c.ef, utn: unionTypeName}
	}

	nm, _ := newName(nameName("union"))
	friendlyName = fmt.Sprintf("union (%s)", nm.n)

	return &codec{
		nm: nm,
		df: func(r io.Reader) (interface{}, error) {
			// Convert to regular JSON from Avro JSON.
			// Union types are encoded in a special manner.
			// See http://avro.apache.org/docs/current/spec.html#Unions
			// Possible Avro JSON values are: null or {"type":a avro_json_value}
			// Determine the correct decoder and then recursively
			// convert avro_json_value to regular JSON.
			// Todo this:
			// 1. First convert the bytes to using regular JSON unmarshal (null||{"type": json_value})
			// 2. Figure out the union type.
			// 3. Lookup the Avro decoder for the union type.
			// 4. Serialize the json_value back to bytes.
			// 5. Run the Avro decoder on the bytes.

			// 1. First convert the bytes to using regular JSON unmarshal (null||{"type": json_value})
			jsonValue, err := jsonDecode(r, friendlyName)
			if err != nil {
				return nil, err
			}

			// 2. Figure out the union type.
			var unionTypeName string
			switch jsonValue.(type) {
			case nil:
				// Only allowed value for a non map in a union type
				unionTypeName = "null"
			case map[string]interface{}:
				// Single key: value with key = type
				jsonMap := jsonValue.(map[string]interface{})

				// extract the first and only key and value
				for k, v := range jsonMap {
					unionTypeName = k
					jsonValue = v
					break
				}
			default:
				return nil, newDecoderError(friendlyName, "unsupported union value %v", jsonValue)
			}

			// 3. Lookup the Avro decoder for the union type.
			jsonDecoderFunc, ok := nameToJSONDecoder[unionTypeName]
			if !ok {
				return nil, newDecoderError(friendlyName, "unknown union type %v", unionTypeName)
			}

			// 4. Serialize the json_value back to bytes.
			b, err := json.Marshal(jsonValue)
			if err != nil {
				return nil, newDecoderError(friendlyName, "union json decode failed: %v", err)
			}

			// 5. Run the Avro decoder on the bytes.
			return jsonDecoderFunc(bytes.NewReader(b))
		},
		ef: func(w io.Writer, datum interface{}) error {
			// Convert from regular JSON to Avro JSON for a union.
			// json_value -> avro_json_value
			// null -> null
			// json_value -> {"union type name": avro_json_value}
			//
			// 1. Lookup the union type.
			// 2. Lookup the union encoder based on the union type.
			// 3. Short circuit null
			// 4. Recursively encode the json_value
			// 5. Create a json map {"union type name" -> avro_json_value}
			// 6. Marshal the json map

			// 1. Lookup the union type
			var unionTypeName string
			switch datum.(type) {
			default:
				unionTypeName = reflect.TypeOf(datum).String()
			case map[string]interface{}:
				unionTypeName = "map"
			case []interface{}:
				unionTypeName = "array"
			case nil:
				unionTypeName = "null"
			case Enum:
				unionTypeName = datum.(Enum).Name
			case Fixed:
				unionTypeName = datum.(Fixed).Name
			case *Record:
				unionTypeName = datum.(*Record).Name
			}

			// 2. Lookup the union encoder based on the union type.
			ue, ok := nameToUnionEncoder[unionTypeName]
			if !ok {
				return newEncoderError(friendlyName, "union json encode error: invalid type %v", unionTypeName)
			}

			// 3. Short circuit null
			if unionTypeName == "null" {
				if err := ue.ef(w, datum); err != nil {
					return newEncoderError(friendlyName, "union json encode error: %v", err)
				}
				return nil
			}

			// 4. Recursively encode the json_value
			var buff bytes.Buffer
			buffWriter := bufio.NewWriter(&buff)
			if err := ue.ef(buffWriter, datum); err != nil {
				return newEncoderError(friendlyName, "union json encode error: %v", err)
			}
			if err := buffWriter.Flush(); err != nil {
				return newEncoderError(friendlyName, "union json encode error: %v", err)
			}

			// 5. Create a json map {"union type name" -> avro_json_value}
			value, err := jsonDecode(bufio.NewReader(&buff), friendlyName)
			if err != nil {
				return err
			}
			tmpDatum := map[string]interface{}{
				ue.utn: value,
			}

			// 6. Marshal the json map
			return jsonEncode(w, tmpDatum)
		},
	}, nil
}

func (st symtabJSON) makeEnumCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	errorNamespace := "null namespace"
	if enclosingNamespace != nullNamespace {
		errorNamespace = enclosingNamespace
	}
	friendlyName := fmt.Sprintf("enum (%s)", errorNamespace)

	// schema checks
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, newCodecBuildError(friendlyName, "expected: map[string]interface{}; received: %T", schema)
	}
	nm, err := newName(nameEnclosingNamespace(enclosingNamespace), nameSchema(schemaMap))
	if err != nil {
		return nil, err
	}
	friendlyName = fmt.Sprintf("enum (%s)", nm.n)

	s, ok := schemaMap["symbols"]
	if !ok {
		return nil, newCodecBuildError(friendlyName, "ought to have symbols key")
	}
	symtab, ok := s.([]interface{})
	if !ok || len(symtab) == 0 {
		return nil, newCodecBuildError(friendlyName, "symbols ought to be non-empty array")
	}
	for _, v := range symtab {
		_, ok := v.(string)
		if !ok {
			return nil, newCodecBuildError(friendlyName, "symbols array member ought to be string")
		}
	}
	c := &codec{
		nm: nm,
		df: func(r io.Reader) (interface{}, error) {
			// Enums are strings in Avro JSON
			someValue, err := stringJSONDecoder(r)
			if err != nil {
				return nil, newDecoderError(friendlyName, err)
			}
			for _, symbol := range symtab {
				if symbol == someValue {
					return Enum{nm.n, someValue.(string)}, nil
				}
			}
			return nil, newDecoderError(friendlyName, "symbol not defined: %s", someValue)
		},
		ef: func(w io.Writer, datum interface{}) error {
			// Enums are strings in Avro JSON
			var someString string
			switch datum.(type) {
			case Enum:
				someString = datum.(Enum).Value
			case string:
				someString = datum.(string)
			default:
				return newEncoderError(friendlyName, "expected: Enum or string; received: %T", datum)
			}
			for _, symbol := range symtab {
				if symbol == someString {
					return stringJSONEncoder(w, someString)
				}
			}
			return newEncoderError(friendlyName, "symbol not defined: %s", someString)
		},
	}
	st.name[nm.n] = c
	return c, nil
}

func (st symtabJSON) makeFixedCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	errorNamespace := "null namespace"
	if enclosingNamespace != nullNamespace {
		errorNamespace = enclosingNamespace
	}
	friendlyName := fmt.Sprintf("fixed (%s)", errorNamespace)

	// schema checks
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, newCodecBuildError(friendlyName, "expected: map[string]interface{}; received: %T", schema)
	}
	nm, err := newName(nameSchema(schemaMap), nameEnclosingNamespace(enclosingNamespace))
	if err != nil {
		return nil, err
	}
	friendlyName = fmt.Sprintf("fixed (%s)", nm.n)
	s, ok := schemaMap["size"]
	if !ok {
		return nil, newCodecBuildError(friendlyName, "ought to have size key")
	}
	fs, ok := s.(float64)
	if !ok {
		return nil, newCodecBuildError(friendlyName, "size ought to be number: %T", s)
	}
	size := int32(fs)
	c := &codec{
		nm: nm,
		df: func(r io.Reader) (interface{}, error) {
			// Fixed is treated in Avro JSON as a string.
			someValue, err := stringJSONDecoder(r)
			if err != nil {
				return nil, newDecoderError(friendlyName, err)
			}
			someFixed := someValue.([]byte)
			if len(someFixed) < int(size) {
				return nil, newDecoderError(friendlyName, "buffer underrun")
			}
			return Fixed{nm.n, someFixed}, nil
		},
		ef: func(w io.Writer, datum interface{}) error {
			// Fixed is treated in Avro JSON as a string.
			someFixed, ok := datum.(Fixed)
			if !ok {
				return newEncoderError(friendlyName, "expected: Fixed; received: %T", datum)
			}
			if len(someFixed.Value) != int(size) {
				return newEncoderError(friendlyName, "expected: %d bytes; received: %d", size, len(someFixed.Value))
			}
			return stringJSONEncoder(w, string(someFixed.Value))
		},
	}
	st.name[nm.n] = c
	return c, nil
}

func (st symtabJSON) makeRecordCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	errorNamespace := "null namespace"
	if enclosingNamespace != nullNamespace {
		errorNamespace = enclosingNamespace
	}
	friendlyName := fmt.Sprintf("record (%s)", errorNamespace)

	// delegate schema checks to NewRecord()
	recordTemplate, err := NewRecord(recordSchemaRaw(schema), RecordEnclosingNamespace(enclosingNamespace))
	if err != nil {
		return nil, err
	}

	if len(recordTemplate.Fields) == 0 {
		return nil, newCodecBuildError(friendlyName, "fields ought to be non-empty array")
	}

	fieldCodecs := make([]*codec, len(recordTemplate.Fields))
	fieldCodecMap := make(map[string]*codec)
	for idx, field := range recordTemplate.Fields {
		var err error
		fieldCodecs[idx], err = st.buildCodec(recordTemplate.n.namespace(), field.schema)
		if err != nil {
			return nil, newCodecBuildError(friendlyName, "record field ought to be codec: %+v", st, err)
		}
		fieldCodecMap[field.Name] = fieldCodecs[idx]
	}

	friendlyName = fmt.Sprintf("record (%s)", recordTemplate.Name)

	c := &codec{
		nm: recordTemplate.n,
		df: func(r io.Reader) (interface{}, error) {
			// Record is Avro JSON encoded as a map with field names as key field values
			// recursively Avro JSON encoded.
			// 1. Unmarshal the bytes as regular JSON.
			// 2. Go through each field and convert from regular JSON to Avro JSON.

			someRecord, _ := NewRecord(recordSchemaRaw(schema), RecordEnclosingNamespace(enclosingNamespace))

			// 1. Unmarshal the bytes as regular JSON.
			datum, err := jsonDecode(r, friendlyName)
			if err != nil {
				return nil, newDecoderError(friendlyName, err)
			}
			jsonMap, ok := datum.(map[string]interface{})
			if !ok {
				return nil, newCodecBuildError(friendlyName, "Expected JSON map but got %T", datum)
			}

			// 2. Go through each field and convert from regular JSON to Avro JSON.
			for key, value := range jsonMap {
				b, err := json.Marshal(value)
				if err != nil {
					return nil, newDecoderError(friendlyName, err)
				}
				field, err := someRecord.getField(key)
				if err != nil {
					return nil, newDecoderError(friendlyName, "Got unknown field %v", key)
				}
				fieldDatum, err := fieldCodecMap[field.Name].Decode(bytes.NewBuffer(b))
				if err != nil {
					return nil, newDecoderError(friendlyName, err)
				}
				field.Datum = fieldDatum
			}
			return someRecord, nil
		},
		ef: func(w io.Writer, datum interface{}) error {
			// Record is Avro JSON encoded as a map with field names as key field values
			// recursively Avro JSON encoded.

			someRecord, ok := datum.(*Record)
			if !ok {
				return newEncoderError(friendlyName, "expected: Record; received: %T", datum)
			}
			if someRecord.Name != recordTemplate.Name {
				return newEncoderError(friendlyName, "expected: %v; received: %v", recordTemplate.Name, someRecord.Name)
			}

			// Recursively Avro JSON encode each field in the right order.
			var orderedMap OrderedMap
			for idx, field := range someRecord.Fields {
				var value interface{}
				// check whether field datum is valid
				if reflect.ValueOf(field.Datum).IsValid() {
					value = field.Datum
				} else if field.hasDefault {
					value = field.defval
				} else {
					return newEncoderError(friendlyName, "field has no data and no default set: %v", field.Name)
				}

				// Avro encode each field value and then unmarshal back as we to finally stick
				// it in a JSON map which gets marshalled out. Too many marshal and unmarshals!
				var buff bytes.Buffer
				tmpWriter := bufio.NewWriter(&buff)
				err = fieldCodecs[idx].Encode(tmpWriter, value)
				if err != nil {
					return newEncoderError(friendlyName, err)
				}
				if err := tmpWriter.Flush(); err != nil {
					return newEncoderError(friendlyName, "record json encode error: %v", err)
				}
				jsonValue, err := jsonDecode(bufio.NewReader(&buff), friendlyName)
				if err != nil {
					return newEncoderError(friendlyName, err)
				}

				// Add the json value to the ordered map
				n, err := newName(nameName(field.Name))
				if err != nil {
					return newEncoderError(friendlyName, err)
				}
				orderedMap = append(orderedMap, KeyVal{n.basename(), jsonValue})
			}

			err := jsonEncode(w, orderedMap)
			if err != nil {
				return newEncoderError(friendlyName, "record json encode error: %v", err)
			}
			return nil
		},
	}
	st.name[recordTemplate.Name] = c
	return c, nil
}

func (st symtabJSON) makeMapCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	errorNamespace := "null namespace"
	if enclosingNamespace != nullNamespace {
		errorNamespace = enclosingNamespace
	}
	friendlyName := fmt.Sprintf("map (%s)", errorNamespace)

	// schema checks
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, newCodecBuildError(friendlyName, "expected: map[string]interface{}; received: %T", schema)
	}
	v, ok := schemaMap["values"]
	if !ok {
		return nil, newCodecBuildError(friendlyName, "ought to have values key")
	}
	valuesCodec, err := st.buildCodec(enclosingNamespace, v)
	if err != nil {
		return nil, newCodecBuildError(friendlyName, err)
	}

	nm := &name{n: "map"}
	friendlyName = fmt.Sprintf("map (%s)", nm.n)

	return &codec{
		nm: nm,
		df: func(r io.Reader) (interface{}, error) {
			// Map is a regular JSON object except each value has to be recursively decoded.
			data := make(map[string]interface{})

			rawDatum, err := jsonDecode(r, friendlyName)
			if err != nil {
				return nil, newDecoderError(friendlyName, err)
			}

			mapDatum, ok := rawDatum.(map[string]interface{})
			if !ok {
				return nil, newDecoderError(friendlyName, "Expected map but got %T", rawDatum)
			}

			for k, v := range mapDatum {
				b, err := json.Marshal(v)
				if err != nil {
					return nil, newDecoderError(friendlyName, err)
				}
				datum, err := valuesCodec.Decode(bytes.NewReader(b))
				if err != nil {
					return nil, newDecoderError(friendlyName, err)
				}
				data[k] = datum
			}
			return data, nil
		},
		ef: func(w io.Writer, datum interface{}) error {
			// Map is a regular JSON object except each value has to be recursively encoded.

			jsonMap, ok := datum.(map[string]interface{})
			if !ok {
				return newEncoderError(friendlyName, "expected: map[string]interface{}; received: %T", datum)
			}

			avroMap := make(map[string]interface{})
			for k, v := range jsonMap {
				var buff bytes.Buffer
				writer := bufio.NewWriter(&buff)
				if err := valuesCodec.Encode(writer, v); err != nil {
					return newEncoderError(friendlyName, err)
				}
				err := writer.Flush()
				if err != nil {
					return newEncoderError(friendlyName, err)
				}
				avroValue, err := jsonDecode(bufio.NewReader(&buff), friendlyName)
				if err != nil {
					return newEncoderError(friendlyName, err)
				}
				avroMap[k] = avroValue
			}
			err := jsonEncode(w, avroMap)
			if err != nil {
				return newEncoderError(friendlyName, err)
			}
			return nil
		},
	}, nil
}

func (st symtabJSON) makeArrayCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	errorNamespace := "null namespace"
	if enclosingNamespace != nullNamespace {
		errorNamespace = enclosingNamespace
	}
	friendlyName := fmt.Sprintf("array (%s)", errorNamespace)

	// schema checks
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, newCodecBuildError(friendlyName, "expected: map[string]interface{}; received: %T", schema)
	}
	v, ok := schemaMap["items"]
	if !ok {
		return nil, newCodecBuildError(friendlyName, "ought to have items key")
	}
	valuesCodec, err := st.buildCodec(enclosingNamespace, v)
	if err != nil {
		return nil, newCodecBuildError(friendlyName, err)
	}

	const itemsPerArrayBlock = 10
	nm := &name{n: "array"}
	friendlyName = fmt.Sprintf("array (%s)", nm.n)

	return &codec{
		nm: nm,
		df: func(r io.Reader) (interface{}, error) {
			// Avro JSON Decode each array value.
			datum, err := jsonDecode(r, friendlyName)
			if err != nil {
				return nil, err
			}
			avroArray, ok := datum.([]interface{})
			if !ok {
				return nil, newDecoderError(friendlyName, "Expected array got %T", datum)
			}

			var jsonArray []interface{}
			for _, avroValue := range avroArray {
				b, err := json.Marshal(avroValue)
				if err != nil {
					return nil, newDecoderError(friendlyName, err)
				}
				datum, err := valuesCodec.Decode(bytes.NewReader(b))
				if err != nil {
					return nil, newDecoderError(friendlyName, err)
				}
				jsonArray = append(jsonArray, datum)
			}
			return jsonArray, nil
		},
		ef: func(w io.Writer, datum interface{}) error {
			// Avro JSON Encode each array value.
			someArray, ok := datum.([]interface{})
			if !ok {
				return newEncoderError(friendlyName, "expected: []interface{}; received: %T", datum)
			}

			var avroArray []interface{}
			for _, someValue := range someArray {
				var buff bytes.Buffer
				writer := bufio.NewWriter(&buff)
				if err := valuesCodec.Encode(writer, someValue); err != nil {
					return newEncoderError(friendlyName, err)
				}
				if err := writer.Flush(); err != nil {
					return newEncoderError(friendlyName, "array json encode error: %v", err)
				}
				avroValue, err := jsonDecode(bufio.NewReader(&buff), friendlyName)
				if err != nil {
					return newEncoderError(friendlyName, err)
				}
				avroArray = append(avroArray, avroValue)
			}
			err := jsonEncode(w, avroArray)
			if err != nil {
				return err
			}
			return nil
		},
	}, nil
}
