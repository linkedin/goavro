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
	"testing"
)

func newCodecUsingV2(tb testing.TB, schema string) *Codec {
	codec, err := NewCodec(schema)
	if err != nil {
		tb.Fatal(err)
	}
	return codec
}

func nativeFromAvroUsingV2(tb testing.TB, avroBlob []byte) ([]interface{}, *Codec) {
	ocf, err := NewOCFReader(bytes.NewReader(avroBlob))
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
	if err := ocf.Err(); err != nil {
		tb.Fatal(err)
	}
	return nativeData, ocf.Codec()
}

func binaryFromNativeUsingV2(tb testing.TB, codec *Codec, nativeData []interface{}) [][]byte {
	binaryData := make([][]byte, len(nativeData))
	for i, datum := range nativeData {
		binaryDatum, err := codec.BinaryFromNative(nil, datum)
		if err != nil {
			tb.Fatal(err)
		}
		binaryData[i] = binaryDatum
	}
	return binaryData
}

func textFromNativeUsingV2(tb testing.TB, codec *Codec, nativeData []interface{}) [][]byte {
	textData := make([][]byte, len(nativeData))
	for i, nativeDatum := range nativeData {
		textDatum, err := codec.TextualFromNative(nil, nativeDatum)
		if err != nil {
			tb.Fatal(err)
		}
		textData[i] = textDatum
	}
	return textData
}

func nativeFromBinaryUsingV2(tb testing.TB, codec *Codec, binaryData [][]byte) []interface{} {
	nativeData := make([]interface{}, len(binaryData))
	for i, binaryDatum := range binaryData {
		nativeDatum, buf, err := codec.NativeFromBinary(binaryDatum)
		if err != nil {
			tb.Fatal(err)
		}
		if len(buf) > 0 {
			tb.Fatalf("BinaryDecode ought to have returned nil buffer: %v", buf)
		}
		nativeData[i] = nativeDatum
	}
	return nativeData
}

func nativeFromTextUsingV2(tb testing.TB, codec *Codec, textData [][]byte) []interface{} {
	nativeData := make([]interface{}, len(textData))
	for i, textDatum := range textData {
		nativeDatum, buf, err := codec.NativeFromTextual(textDatum)
		if err != nil {
			tb.Fatal(err)
		}
		if len(buf) > 0 {
			tb.Fatalf("TextDecode ought to have returned nil buffer: %v", buf)
		}
		nativeData[i] = nativeDatum
	}
	return nativeData
}
