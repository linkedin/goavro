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
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

const (
	// CompressionNullLabel is used when OCF blocks are not compressed.
	CompressionNullLabel = "null"

	// CompressionDeflateLabel is used when OCF blocks are compressed using the
	// deflate algorithm.
	CompressionDeflateLabel = "deflate"

	// CompressionSnappyLabel is used when OCF blocks are compressed using the
	// snappy algorithm.
	CompressionSnappyLabel = "snappy"
)

// compressionID are values used to specify compression algorithm used to compress
// and decompress Avro Object Container File (OCF) streams.
type compressionID uint8

const (
	compressionNull compressionID = iota
	compressionDeflate
	compressionSnappy
)

const (
	ocfBlockConst      = 24 // Each OCF block has two longs prefix, and sync marker suffix
	ocfHeaderSizeConst = 48 // OCF header is usually about 48 bytes longer than its compressed schema
	ocfMagicString     = "Obj\x01"
	OCFSyncLength      = 16
)

var (
	ocfMagicBytes    = []byte(ocfMagicString)
	ocfMetadataCodec *Codec
)

func init() {
	ocfMetadataCodec, _ = NewCodec(`{"type":"map","values":"bytes"}`) // elide error checking for known good schema
}

// OCFHeader represents the unvalidated values from the header of an OCF file.
type OCFHeader struct {
	// SyncMarker is a 16-byte, randomly-generated sync marker for a given OCF
	// file.
	SyncMarker [OCFSyncLength]byte

	// MetaData is an association of string names to byte slices:
	//     {"type": "map", "values": "bytes"}.
	MetaData map[string][]byte

	// CompressionName corresponds to the "avro.codec" metadata value, and is
	// the name of the compression codec used to compress blocks, as a string.
	CompressionName string

	// Schema corresponds to the "avro.schema" metadata value, and contains the
	// schema of objects stored in the file, as JSON data.
	Schema string
}

// NewOCFHeaderFromReader reads enough bytes from ior to consume and return the
// OCF header for the stream, returning an error only when cannot read from the
// stream, or cannot parse the values from the bytes.  No checking is done on
// the validity of the metadata values, or whether the "avro.codec" refers to a
// supported compression name, or whether the "avro.schema" represents a legal
// schema.  This function merely returns what was found in the OCF header.
func NewOCFHeaderFromReader(ior io.Reader) (*OCFHeader, error) {
	// magic bytes
	var magicBytes [4]byte // store bytes on stack
	magic := magicBytes[:]
	_, err := io.ReadFull(ior, magic)
	if err != nil {
		return nil, fmt.Errorf("cannot read OCF header magic bytes: %s", err)
	}
	if !bytes.Equal(magic, ocfMagicBytes) {
		return nil, fmt.Errorf("cannot read OCF header with invalid magic bytes: %#q", magic)
	}

	// metadata
	metadata, err := metadataBinaryReader(ior)
	if err != nil {
		return nil, fmt.Errorf("cannot read OCF header metadata: %s", err)
	}

	// sync marker
	var syncMarkerBytes [OCFSyncLength]byte
	syncMarker := syncMarkerBytes[:]
	if n, err := io.ReadFull(ior, syncMarker); err != nil {
		return nil, fmt.Errorf("cannot read OCF header without sync marker: only read %d of %d bytes: %s", n, OCFSyncLength, err)
	}

	h := &OCFHeader{
		MetaData:        metadata,
		CompressionName: string(metadata["avro.codec"]),
		Schema:          string(metadata["avro.schema"]),
	}
	copy(h.SyncMarker[:], syncMarker)

	return h, nil
}

type ocfHeader struct {
	codec         *Codec
	compressionID compressionID
	syncMarker    [OCFSyncLength]byte
	metadata      map[string][]byte
}

func newOCFHeader(config OCFConfig) (*ocfHeader, error) {
	var err error

	header := new(ocfHeader)

	// avro.codec
	switch config.CompressionName {
	case "", CompressionNullLabel:
		header.compressionID = compressionNull
	case CompressionDeflateLabel:
		header.compressionID = compressionDeflate
	case CompressionSnappyLabel:
		header.compressionID = compressionSnappy
	default:
		return nil, fmt.Errorf("cannot create OCF header using unrecognized compression algorithm: %q", config.CompressionName)
	}

	// avro.schema
	if config.Codec != nil {
		header.codec = config.Codec
	} else if config.Schema == "" {
		return nil, fmt.Errorf("cannot create OCF header without either Codec or Schema specified")
	} else {
		if header.codec, err = NewCodec(config.Schema); err != nil {
			return nil, fmt.Errorf("cannot create OCF header: %s", err)
		}
	}

	header.metadata = config.MetaData

	// The 16-byte, randomly-generated sync marker for this file.
	_, err = rand.Read(header.syncMarker[:])
	if err != nil {
		return nil, err
	}

	return header, nil
}

func readOCFHeader(ior io.Reader) (*ocfHeader, error) {
	ocfh, err := NewOCFHeaderFromReader(ior)
	if err != nil {
		return nil, err
	}

	//
	// avro.codec
	//
	// NOTE: Avro specification states that `null` cID is used by default when
	// "avro.codec" was not included in the metadata header. The specification
	// does not talk about the case when "avro.codec" was included with the
	// empty string as its value. I believe it is an error for an OCF file to
	// provide the empty string as the cID algorithm. While it is trivially easy
	// to gracefully handle here, I'm not sure whether this happens a lot, and
	// don't want to accept bad input unless we have significant reason to do
	// so.
	var cID compressionID
	switch ocfh.CompressionName {
	case "", CompressionNullLabel:
		cID = compressionNull
	case CompressionDeflateLabel:
		cID = compressionDeflate
	case CompressionSnappyLabel:
		cID = compressionSnappy
	default:
		return nil, fmt.Errorf("cannot read OCF header using unrecognized compression algorithm from avro.codec: %q", ocfh.CompressionName)
	}

	if ocfh.Schema == "" {
		return nil, errors.New("cannot read OCF header without avro.schema")
	}

	codec, err := NewCodec(ocfh.Schema)
	if err != nil {
		return nil, fmt.Errorf("cannot read OCF header with invalid avro.schema: %s", err)
	}

	header := &ocfHeader{codec: codec, compressionID: cID, metadata: ocfh.MetaData}
	copy(header.syncMarker[:], ocfh.SyncMarker[:])

	//
	// header is valid
	//
	return header, nil
}

func writeOCFHeader(header *ocfHeader, iow io.Writer) (err error) {
	// avro.codec
	var avroCodec string
	switch header.compressionID {
	case compressionNull:
		avroCodec = CompressionNullLabel
	case compressionDeflate:
		avroCodec = CompressionDeflateLabel
	case compressionSnappy:
		avroCodec = CompressionSnappyLabel
	default:
		return fmt.Errorf("should not get here: cannot write OCF header using unrecognized compression algorithm: %d", header.compressionID)
	}

	//
	// avro.schema
	//
	// Create buffer for OCF header. The first four bytes are magic, and we'll
	// use copy to fill them in, so initialize buffer's length with 4, and its
	// capacity equal to length of avro schema plus a constant.
	schema := header.codec.Schema()
	buf := make([]byte, 4, len(schema)+ocfHeaderSizeConst)
	_ = copy(buf, ocfMagicBytes)

	// file metadata, including the schema
	meta := make(map[string]interface{})
	for k, v := range header.metadata {
		meta[k] = v
	}
	meta["avro.schema"] = []byte(schema)
	meta["avro.codec"] = []byte(avroCodec)

	buf, err = ocfMetadataCodec.BinaryFromNative(buf, meta)
	if err != nil {
		return fmt.Errorf("should not get here: cannot write OCF header: %s", err)
	}

	// 16-byte sync marker
	buf = append(buf, header.syncMarker[:]...)

	// emit buffer with encoded OCF header
	_, err = iow.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write OCF header: %s", err)
	}
	return nil
}
