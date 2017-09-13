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
	"io/ioutil"
	"testing"
)

func benchmarkNewCodecUsingV1(b *testing.B, avscPath string) {
	schema, err := ioutil.ReadFile(avscPath)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = newCodecUsingV1(b, string(schema))
	}
}

func benchmarkNewCodecUsingV2(b *testing.B, avscPath string) {
	schema, err := ioutil.ReadFile(avscPath)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = newCodecUsingV2(b, string(schema))
	}
}

func benchmarkNativeFromAvroUsingV1(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = nativeFromAvroUsingV1(b, avroBlob)
	}
}

func benchmarkNativeFromAvroUsingV2(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = nativeFromAvroUsingV2(b, avroBlob)
	}
}

func benchmarkBinaryFromNativeUsingV1(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV1(b, avroBlob)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = binaryFromNativeUsingV1(b, codec, nativeData)
	}
}

func benchmarkBinaryFromNativeUsingV2(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV2(b, avroBlob)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = binaryFromNativeUsingV2(b, codec, nativeData)
	}
}

func benchmarkNativeFromBinaryUsingV1(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV1(b, avroBlob)
	binaryData := binaryFromNativeUsingV1(b, codec, nativeData)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = nativeFromBinaryUsingV1(b, codec, binaryData)
	}
}

func benchmarkNativeFromBinaryUsingV2(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV2(b, avroBlob)
	binaryData := binaryFromNativeUsingV2(b, codec, nativeData)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = nativeFromBinaryUsingV2(b, codec, binaryData)
	}
}

func benchmarkTextualFromNativeUsingJSONMarshal(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV1(b, avroBlob)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = textFromNativeUsingJSONMarshal(b, codec, nativeData)
	}
}

func benchmarkTextualFromNativeUsingV2(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV2(b, avroBlob)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = textFromNativeUsingV2(b, codec, nativeData)
	}
}

func benchmarkNativeFromTextualUsingJSONUnmarshal(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV1(b, avroBlob)
	textData := textFromNativeUsingJSONMarshal(b, codec, nativeData)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = nativeFromTextUsingJSONUnmarshal(b, codec, textData)
	}
}

func benchmarkNativeFromTextualUsingV2(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV2(b, avroBlob)
	textData := textFromNativeUsingV2(b, codec, nativeData)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = nativeFromTextUsingV2(b, codec, textData)
	}
}
