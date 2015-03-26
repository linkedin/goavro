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

package goavro

import (
	"bufio"
	"bytes"
	"code.google.com/p/snappy-go/snappy"
	"compress/flate"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
)

const DefaultWriterBlockSize = 10

// ErrWriterInit describes an initialization error for a
// Writer instance.
type ErrWriterInit struct {
	Message string
	Err     error
}

// Error converts the error instance to a string.
func (e *ErrWriterInit) Error() string {
	if e.Err == nil {
		return "cannot create Writer: " + e.Message
	} else if e.Message == "" {
		return "cannot create Writer: " + e.Err.Error()
	} else {
		return "cannot create Writer: " + e.Message + "; " + e.Err.Error()
	}
}

// WriterSetter functions are those those which are used to
// instantiate a new Writer.
type WriterSetter func(*Writer) error

// ToWriter specifies which io.Writer is the target of the Writer
// stream.
func ToWriter(w io.Writer) WriterSetter {
	return func(fw *Writer) error {
		fw.w = w
		return nil
	}
}

// BufferToWriter specifies which io.Writer is the target of the
// Writer stream, and creates a bufio.Writer around that
// io.Writer.
func BufferToWriter(w io.Writer) WriterSetter {
	return func(fw *Writer) error {
		fw.w = bufio.NewWriter(w)
		fw.buffered = true
		return nil
	}
}

// BlockSize specifies the default number of data items to be
// grouped in a block, compressed, and written to the stream.
func BlockSize(blockSize int64) WriterSetter {
	return func(fw *Writer) error {
		if blockSize <= 0 {
			return fmt.Errorf("blockSize must be larger than 0: %d", blockSize)
		}
		fw.blockSize = blockSize
		return nil
	}
}

// Compression is used to set the compression codec of
// a new Writer instance.
func Compression(someCompressionCodec string) WriterSetter {
	return func(fw *Writer) error {
		fw.CompressionCodec = someCompressionCodec
		return nil
	}
}

// Sync is used to set the sync marker bytes of a new
// instance. It checks to ensure the byte slice is 16 bytes long, but
// does not check that it has been set to something other than the
// zero value. Usually you can elide the `Sync` call and allow it
// to create a random byte sequence.
func Sync(someSync []byte) WriterSetter {
	return func(fw *Writer) error {
		if syncLength != len(someSync) {
			return fmt.Errorf("sync marker ought to be %d bytes long: %d", syncLength, len(someSync))
		}
		fw.Sync = make([]byte, syncLength)
		copy(fw.Sync, someSync)
		return nil
	}
}

// WriterSchema is used to set the Avro schema of a new instance.
func WriterSchema(someSchema string) WriterSetter {
	return func(fw *Writer) error {
		var err error
		fw.DataSchema = someSchema
		fw.dataCodec, err = NewCodec(fw.DataSchema)
		if err != nil {
			return fmt.Errorf("error compiling schema: %v", err)
		}
		return nil
	}
}

// Writer structure contains data necessary to write Avro files.
type Writer struct {
	CompressionCodec string
	DataSchema       string
	Sync             []byte
	blockSize        int64
	buffered         bool
	dataCodec        Codec
	err              error
	toBlock          chan interface{}
	w                io.Writer
	writerDone       chan struct{}
}

// NewWriter returns a object to write data to an io.Writer using the
// Avro Object Container Files format.
//
// func serveClient(conn net.Conn) {
// 	fw, err := goavro.NewWriter(
// 		goavro.BlockSize(100),
// 		goavro.Compression(goavro.CompressionSnappy),
// 		goavro.WriterSchema(recordSchema),
// 		goavro.ToWriter(conn))
// 	if err != nil {
// 		log.Fatal("cannot create Writer: ", err)
// 	}
// 	defer fw.Close()
//
// 	// create a record that matches the schema we want to encode
// 	someRecord, err := goavro.NewRecord(goavro.RecordSchema(recordSchema))
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	// identify field name to set datum for
// 	someRecord.Set("username", "Aquaman")
// 	someRecord.Set("comment", "The Atlantic is oddly cold this morning!")
// 	// you can fully qualify the field name
// 	someRecord.Set("com.example.timestamp", int64(1082196484))
// 	fw.Write(someRecord)
//
// 	// create another record
// 	someRecord, err = goavro.NewRecord(goavro.RecordSchema(recordSchema))
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	someRecord.Set("username", "Batman")
// 	someRecord.Set("comment", "Who are all of these crazies?")
// 	someRecord.Set("com.example.timestamp", int64(1427383430))
// 	fw.Write(someRecord)
// }
func NewWriter(setters ...WriterSetter) (*Writer, error) {
	var err error
	fw := &Writer{CompressionCodec: CompressionNull}
	for _, setter := range setters {
		err = setter(fw)
		if err != nil {
			return nil, &ErrWriterInit{Err: err}
		}
	}
	if fw.w == nil {
		return nil, &ErrWriterInit{Message: "must specify io.Writer"}
	}
	// writer: stuff should already be initialized
	if !IsCompressionCodecSupported(fw.CompressionCodec) {
		return nil, &ErrWriterInit{Message: fmt.Sprintf("unsupported codec: %s", fw.CompressionCodec)}
	}
	if fw.DataSchema == "" {
		return nil, &ErrWriterInit{Message: "missing schema"}
	}
	fw.DataSchema, err = compressJson(fw.DataSchema)
	if err != nil {
		return nil, &ErrWriterInit{Err: err}
	}
	if fw.dataCodec, err = NewCodec(fw.DataSchema); err != nil {
		return nil, &ErrWriterInit{Err: err}
	}
	if fw.Sync == nil {
		// create random sequence of bytes for file sync marker
		fw.Sync = make([]byte, syncLength)
		for i := range fw.Sync {
			fw.Sync[i] = byte(rand.Intn(256))
		}
	}
	if err = fw.writeHeader(); err != nil {
		return nil, &ErrWriterInit{Err: err}
	}
	// setup writing pipeline
	fw.toBlock = make(chan interface{})
	toEncode := make(chan *writerBlock)
	toCompress := make(chan *writerBlock)
	toWrite := make(chan *writerBlock)
	fw.writerDone = make(chan struct{})
	go blocker(fw, fw.toBlock, toEncode)
	go encoder(fw, toEncode, toCompress)
	go compressor(fw, toCompress, toWrite)
	go writer(fw, toWrite)
	return fw, nil
}

// Close is called when the open file is no longer needed. It flushes
// the bytes to the `io.Writer` if the file is being writtern.
func (fw *Writer) Close() error {
	close(fw.toBlock)
	<-fw.writerDone
	if fw.buffered {
		return fw.w.(*bufio.Writer).Flush()
	}
	return nil
}

// Write places a datum into the pipeline to be written to the Writer.
func (fw *Writer) Write(datum interface{}) {
	fw.toBlock <- datum
}

func (fw *Writer) writeHeader() (err error) {
	if _, err = fw.w.Write([]byte(magicBytes)); err != nil {
		return
	}
	// header metadata
	hm := make(map[string]interface{})
	hm["avro.schema"] = []byte(fw.DataSchema)
	hm["avro.codec"] = []byte(fw.CompressionCodec)
	if err = metadataCodec.Encode(fw.w, hm); err != nil {
		return
	}
	_, err = fw.w.Write(fw.Sync)
	return
}

func compressJson(schemaJson string) (string, error) {
	var err error
	var compressed []byte
	var schema interface{}
	// unmarshal into schema blob
	if err = json.Unmarshal([]byte(schemaJson), &schema); err != nil {
		return "", fmt.Errorf("cannot unmarshal schema string: %#v: %v", schemaJson, err)
	}
	// remarshal back into compressed json
	if compressed, err = json.Marshal(schema); err != nil {
		return "", fmt.Errorf("cannot marshal schema: %v", err)
	}
	return string(compressed), nil
}

type writerBlock struct {
	items      []interface{}
	encoded    *bytes.Buffer
	compressed []byte
	err        error
}

func blocker(fw *Writer, toBlock <-chan interface{}, toEncode chan<- *writerBlock) {
	items := make([]interface{}, 0, fw.blockSize)
	for item := range toBlock {
		items = append(items, item)
		if int64(len(items)) == fw.blockSize {
			toEncode <- &writerBlock{items: items}
			items = make([]interface{}, 0)
		}
	}
	if len(items) > 0 {
		toEncode <- &writerBlock{items: items}
	}
	close(toEncode)
}

func encoder(fw *Writer, toEncode <-chan *writerBlock, toCompress chan<- *writerBlock) {
	for block := range toEncode {
		if block.err == nil {
			block.encoded = new(bytes.Buffer)
			for _, item := range block.items {
				block.err = fw.dataCodec.Encode(block.encoded, item)
				if block.err != nil {
					break
				}
			}
		}
		toCompress <- block
	}
	close(toCompress)
}

func compressor(fw *Writer, toCompress <-chan *writerBlock, toWrite chan<- *writerBlock) {
	switch fw.CompressionCodec {
	case CompressionDeflate:
		bb := new(bytes.Buffer)
		comp, _ := flate.NewWriter(bb, flate.DefaultCompression)
		for block := range toCompress {
			_, block.err = comp.Write(block.encoded.Bytes())
			comp.Close()
			block.compressed = bb.Bytes()
			toWrite <- block
			bb = new(bytes.Buffer)
			comp.Reset(bb)
		}
	case CompressionNull:
		for block := range toCompress {
			block.compressed = block.encoded.Bytes()
			toWrite <- block
		}
	case CompressionSnappy:
		for block := range toCompress {
			block.compressed, block.err = snappy.Encode(block.compressed, block.encoded.Bytes())
			if block.err != nil {
				block.err = fmt.Errorf("cannot compress: %v", block.err)
			}
			toWrite <- block
		}
	}
	close(toWrite)
}

func writer(fw *Writer, toWrite <-chan *writerBlock) {
	for block := range toWrite {
		if block.err == nil {
			block.err = longCodec.Encode(fw.w, int64(len(block.items)))
		}
		if block.err == nil {
			block.err = longCodec.Encode(fw.w, int64(len(block.compressed)))
		}
		if block.err == nil {
			_, block.err = fw.w.Write(block.compressed)
		}
		if block.err == nil {
			_, block.err = fw.w.Write(fw.Sync)
		}
		if block.err != nil {
			log.Printf("[WARNING] cannot write block: %v", block.err)
			fw.err = block.err // ???
			break
			// } else {
			// 	log.Printf("[DEBUG] block written: %d, %d, %v", len(block.items), len(block.compressed), block.compressed)
		}
	}
	if fw.err = longCodec.Encode(fw.w, int64(0)); fw.err == nil {
		fw.err = longCodec.Encode(fw.w, int64(0))
	}
	fw.writerDone <- struct{}{}
}
