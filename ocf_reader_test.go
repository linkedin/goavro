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

// readOCFHeader, magic bytes

func TestReadOCFHeaderMagicBytes(t *testing.T) {
	_, err := NewOCFReader(bytes.NewBuffer([]byte("Obj"))) // missing fourth byte
	ensureError(t, err, "cannot create OCF")

	_, err = NewOCFReader(bytes.NewBuffer([]byte("....")))
	ensureError(t, err, "cannot create OCF")
}

//
// cannot read OCF header
//

func testCannotReadOCFHeader(t *testing.T, input []byte, expected ...string) {
	_, err := NewOCFReader(bytes.NewBuffer(append([]byte("Obj\x01"), input...)))
	ensureError(t, err, append([]string{"cannot read OCF header"}, expected...)...)
}

// readOCFHeader, metadataBinaryReader, block count

func TestReadOCFHeaderMetadataBinaryReaderBlockCount(t *testing.T) {
	testCannotReadOCFHeader(t, nil, "cannot read map block count", "EOF")
	testCannotReadOCFHeader(t, mostNegativeBlockCount, "cannot read map with block count")
	testCannotReadOCFHeader(t, []byte("\x01"), "cannot read map block size", "EOF")
	testCannotReadOCFHeader(t, morePositiveThanMaxBlockCount, "cannot read map when block count exceeds")
}

// readOCFHeader, metadataBinaryReader, bytesBinaryReader

func TestReadOCFHeaderMetadataBinaryReaderMapKey(t *testing.T) {
	testCannotReadOCFHeader(t, []byte("\x02"), "cannot read map key", "cannot read bytes", "cannot read size", "EOF")
	testCannotReadOCFHeader(t, []byte("\x02\x01"), "cannot read map key", "cannot read bytes", "size is negative")
	testCannotReadOCFHeader(t, append([]byte("\x02"), morePositiveThanMaxBlockCount...), "cannot read map key", "cannot read bytes", "size exceeds MaxBlockSize")
	testCannotReadOCFHeader(t, append([]byte("\x02"), mostNegativeBlockCount...), "cannot read map key", "cannot read bytes", "size is negative")
	testCannotReadOCFHeader(t, append([]byte("\x02"), moreNegativeThanMaxBlockCount...), "cannot read map key", "cannot read bytes", "size is negative")
	testCannotReadOCFHeader(t, []byte("\x02\x02"), "cannot read map key", "cannot read bytes", "EOF")
	testCannotReadOCFHeader(t, []byte("\x02\x04k1\x04v1\x02\x04k1"), "cannot read map", "duplicate key")
	testCannotReadOCFHeader(t, []byte("\x04\x04k1\x04v1\x04k1"), "cannot read map", "duplicate key")
}

func TestReadOCFHeaderMetadataBinaryReaderMapValue(t *testing.T) {
	testCannotReadOCFHeader(t, []byte("\x02\x04k1"), "cannot read map value for key", "cannot read bytes", "EOF")
	// have already tested all other binaryBytesReader errors above
	testCannotReadOCFHeader(t, []byte("\x02\x04k1\x04v1"), "cannot read map block count", "EOF")
	testCannotReadOCFHeader(t, append([]byte("\x02\x04k1\x04v1"), mostNegativeBlockCount...), "cannot read map with block count")
	testCannotReadOCFHeader(t, []byte("\x02\x04k1\x04v1"), "cannot read map block count", "EOF")
	testCannotReadOCFHeader(t, []byte("\x02\x04k1\x04v1\x01"), "cannot read map block size", "EOF")
	testCannotReadOCFHeader(t, append(append([]byte("\x02\x04k1\x04v1"), moreNegativeThanMaxBlockCount...), []byte("\x02")...), "cannot read map when block count exceeds")
	testCannotReadOCFHeader(t, append([]byte("\x02\x04k1\x04v1"), morePositiveThanMaxBlockCount...), "cannot read map when block count exceeds")
}

// readOCFHeader, avro.codec

func TestReadOCFHeaderMetadataAvroCodecUnknown(t *testing.T) {
	testCannotReadOCFHeader(t, []byte("\x02\x14avro.codec\x06bad\x00"), "cannot read OCF header", "unrecognized compression", "bad")
}

// readOCFHeader, avro.schema

func TestReadOCFHeaderMetadataAvroSchemaMissing(t *testing.T) {
	testCannotReadOCFHeader(t, []byte("\x00"), "without avro.schema")
	testCannotReadOCFHeader(t, []byte("\x02\x16avro.schema\x04{}\x00"), "invalid avro.schema")
}

// readOCFHeader, sync marker

func TestReadOCFHeaderMetadataSyncMarker(t *testing.T) {
	testCannotReadOCFHeader(t, []byte("\x02\x16avro.schema\x1e{\"type\":\"null\"}\x00"), "sync marker", "EOF")
}

// TODO: writeOCFHeader

//
// OCFReader
//

// func testOCFReader(t *testing.T, schema string, input []byte, expected ...string) {
// 	_, err := NewOCFReader(bytes.NewBuffer(append([]byte("Obj\x01"), input...)))
// 	ensureError(t, err, append([]string{"any prefix?"}, expected...)...)
// }

// func TestOCFReaderRead(t *testing.T) {
// 	testOCFReader(t,
// }
