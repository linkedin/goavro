// Copyright [2017] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import "testing"

func BenchmarkNewCodecUsingV1(b *testing.B) {
	benchmarkNewCodecUsingV1(b, "fixtures/quickstop.avsc")
}

func BenchmarkNewCodecUsingV2(b *testing.B) {
	benchmarkNewCodecUsingV2(b, "fixtures/quickstop.avsc")
}

func BenchmarkNativeFromAvroUsingV1(b *testing.B) {
	benchmarkNativeFromAvroUsingV1(b, "fixtures/quickstop-null.avro")
}

func BenchmarkNativeFromAvroUsingV2(b *testing.B) {
	benchmarkNativeFromAvroUsingV2(b, "fixtures/quickstop-null.avro")
}

func BenchmarkBinaryFromNativeUsingV1(b *testing.B) {
	benchmarkBinaryFromNativeUsingV1(b, "fixtures/quickstop-null.avro")
}

func BenchmarkBinaryFromNativeUsingV2(b *testing.B) {
	benchmarkBinaryFromNativeUsingV2(b, "fixtures/quickstop-null.avro")
}

func BenchmarkNativeFromBinaryUsingV1(b *testing.B) {
	benchmarkNativeFromBinaryUsingV1(b, "fixtures/quickstop-null.avro")
}

func BenchmarkNativeFromBinaryUsingV2(b *testing.B) {
	benchmarkNativeFromBinaryUsingV2(b, "fixtures/quickstop-null.avro")
}

func BenchmarkTextualFromNativeUsingJSONMarshal(b *testing.B) {
	benchmarkTextualFromNativeUsingJSONMarshal(b, "fixtures/quickstop-null.avro")
}

func BenchmarkTextualFromNativeUsingV2(b *testing.B) {
	benchmarkTextualFromNativeUsingV2(b, "fixtures/quickstop-null.avro")
}

func BenchmarkNativeFromTextualUsingJSONUnmarshal(b *testing.B) {
	benchmarkNativeFromTextualUsingJSONUnmarshal(b, "fixtures/quickstop-null.avro")
}

func BenchmarkNativeFromTextualUsingV2(b *testing.B) {
	benchmarkNativeFromTextualUsingV2(b, "fixtures/quickstop-null.avro")
}
