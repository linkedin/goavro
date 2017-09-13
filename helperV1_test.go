// Copyright [2017] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"bytes"
	"encoding/json"
	"testing"

	v1 "gopkg.in/linkedin/goavro.v1"
)

func newCodecUsingV1(tb testing.TB, schema string) v1.Codec {
	codec, err := v1.NewCodec(schema)
	if err != nil {
		tb.Fatal(err)
	}
	return codec
}

func nativeFromAvroUsingV1(tb testing.TB, avroBlob []byte) ([]interface{}, v1.Codec) {
	ocf, err := v1.NewReader(v1.FromReader(bytes.NewReader(avroBlob)))
	if err != nil {
		tb.Fatal(err)
	}

	var nativeData []interface{}
	for ocf.Scan() {
		datum, err := ocf.Read()
		if err != nil {
			break // Read error sets OCFReader error
		}
		nativeData = append(nativeData, datum)
	}
	if err := ocf.Close(); err != nil {
		tb.Fatal(err)
	}

	codec, err := v1.NewCodec(ocf.DataSchema)
	if err != nil {
		tb.Fatal(err)
	}
	return nativeData, codec
}

func binaryFromNativeUsingV1(tb testing.TB, codec v1.Codec, nativeData []interface{}) [][]byte {
	binaryData := make([][]byte, len(nativeData))
	for i, datum := range nativeData {
		bb := new(bytes.Buffer)
		err := codec.Encode(bb, datum)
		if err != nil {
			tb.Fatal(err)
		}
		binaryData[i] = bb.Bytes()
	}
	return binaryData
}

func nativeFromBinaryUsingV1(tb testing.TB, codec v1.Codec, binaryData [][]byte) []interface{} {
	nativeData := make([]interface{}, len(binaryData))
	for i, binaryDatum := range binaryData {
		bb := bytes.NewReader(binaryDatum)
		nativeDatum, err := codec.Decode(bb)
		if err != nil {
			tb.Fatal(err)
		}
		if bb.Len() > 0 {
			tb.Fatalf("Decode ought to have emptied buffer")
		}
		nativeData[i] = nativeDatum
	}
	return nativeData
}

func textFromNativeUsingJSONMarshal(tb testing.TB, _ v1.Codec, nativeData []interface{}) [][]byte {
	textData := make([][]byte, len(nativeData))
	for i, nativeDatum := range nativeData {
		textDatum, err := json.Marshal(nativeDatum)
		if err != nil {
			tb.Fatal(err)
		}
		textData[i] = textDatum
	}
	return textData
}

func nativeFromTextUsingJSONUnmarshal(tb testing.TB, _ v1.Codec, textData [][]byte) []interface{} {
	nativeData := make([]interface{}, len(textData))
	for i, textDatum := range textData {
		var nativeDatum interface{}
		err := json.Unmarshal(textDatum, &nativeDatum)
		if err != nil {
			tb.Fatal(err)
		}
		nativeData[i] = nativeDatum
	}
	return nativeData
}
