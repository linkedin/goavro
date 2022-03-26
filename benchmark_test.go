// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"os"
	"testing"
)

func BenchmarkNewCodecUsingV2(b *testing.B) {
	schema, err := os.ReadFile("fixtures/quickstop.avsc")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = newCodecUsingV2(b, string(schema))
	}
}

func BenchmarkNativeFromAvroUsingV2(b *testing.B) {
	avroBlob, err := os.ReadFile("fixtures/quickstop-null.avro")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = nativeFromAvroUsingV2(b, avroBlob)
	}
}

func BenchmarkBinaryFromNativeUsingV2(b *testing.B) {
	avroBlob, err := os.ReadFile("fixtures/quickstop-null.avro")
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV2(b, avroBlob)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = binaryFromNativeUsingV2(b, codec, nativeData)
	}
}

func BenchmarkNativeFromBinaryUsingV2(b *testing.B) {
	avroBlob, err := os.ReadFile("fixtures/quickstop-null.avro")
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

func BenchmarkTextualFromNativeUsingV2(b *testing.B) {
	avroBlob, err := os.ReadFile("fixtures/quickstop-null.avro")
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV2(b, avroBlob)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = textFromNativeUsingV2(b, codec, nativeData)
	}
}

func BenchmarkNativeFromTextualUsingV2(b *testing.B) {
	avroBlob, err := os.ReadFile("fixtures/quickstop-null.avro")
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
