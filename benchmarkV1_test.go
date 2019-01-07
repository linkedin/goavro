// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

// +build v1

package goavro

import (
	"io/ioutil"
	"testing"
)

func BenchmarkNewCodecUsingV1(b *testing.B) {
	schema, err := ioutil.ReadFile("fixtures/quickstop.avsc")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = newCodecUsingV1(b, string(schema))
	}
}

func BenchmarkNativeFromAvroUsingV1(b *testing.B) {
	avroBlob, err := ioutil.ReadFile("fixtures/quickstop-null.avro")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = nativeFromAvroUsingV1(b, avroBlob)
	}
}

func BenchmarkBinaryFromNativeUsingV1(b *testing.B) {
	avroBlob, err := ioutil.ReadFile("fixtures/quickstop-null.avro")
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV1(b, avroBlob)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = binaryFromNativeUsingV1(b, codec, nativeData)
	}
}

func BenchmarkNativeFromBinaryUsingV1(b *testing.B) {
	avroBlob, err := ioutil.ReadFile("fixtures/quickstop-null.avro")
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

func BenchmarkTextualFromNativeUsingJSONMarshal(b *testing.B) {
	avroBlob, err := ioutil.ReadFile("fixtures/quickstop-null.avro")
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV1(b, avroBlob)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = textFromNativeUsingJSONMarshal(b, codec, nativeData)
	}
}

func BenchmarkNativeFromTextualUsingJSONUnmarshal(b *testing.B) {
	avroBlob, err := ioutil.ReadFile("fixtures/quickstop-null.avro")
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
