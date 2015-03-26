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
	"fmt"
	"io"
	"log"
	"math/rand"
)

const DefaultFileWriterBlockSize = 10

type ErrFileWriterInit struct {
	Message string
	Err     error
}

func (e *ErrFileWriterInit) Error() string {
	if e.Err == nil {
		return "cannot create FileWriter: " + e.Message
	} else {
		return "cannot create FileWriter: " + e.Message + "; " + e.Err.Error()
	}
}

// FileWriter structure contains data necessary to write Avro files.
type FileWriter struct {
	CompressionCodec string
	DataSchema       string
	Sync             []byte
	dataCodec        Codec
	w                io.Writer
	err              error
	buffered         bool
	blockSize        int64
	toBlock          chan interface{}
	writerDone       chan struct{}
}

// Close is called when the open file is no longer needed. It flushes
// the bytes to the `io.Writer` if the file is being writtern.
func (fw *FileWriter) Close() error {
	close(fw.toBlock)
	<-fw.writerDone
	if fw.buffered {
		// log.Printf("[DEBUG] flushing buffer")
		return fw.w.(*bufio.Writer).Flush()
	}
	return nil
}

// FileWriterSetter functions are those those which are used to instantiate
// a new FileWriter.
type FileWriterSetter func(*FileWriter) error

// NewFileWriter returns a File object to write data to an `io.Writer`
// using the Avro Object Container Files format.
func NewFileWriter(setters ...FileWriterSetter) (*FileWriter, error) {
	var err error
	fw := &FileWriter{CompressionCodec: CompressionNull}
	for _, setter := range setters {
		err = setter(fw)
		if err != nil {
			return nil, &ErrFileWriterInit{Message: "setter", Err: err}
		}
	}
	if fw.w == nil {
		return nil, &ErrFileWriterInit{Message: "must specify io.Writer"}
	}
	// writer: stuff should already be initialized
	if !IsCompressionCodecSupported(fw.CompressionCodec) {
		return nil, &ErrFileWriterInit{Message: fmt.Sprintf("unsupported codec: %s", fw.CompressionCodec)}
	}
	if fw.DataSchema == "" {
		return nil, &ErrFileWriterInit{Message: "missing schema"}
	}
	fw.dataCodec, err = NewCodec(fw.DataSchema)
	if err != nil {
		return nil, &ErrFileWriterInit{Message: "compiling schema", Err: err}
	}
	if fw.Sync == nil {
		// create random sequence of bytes for file sync marker
		fw.Sync = make([]byte, syncLength)
		for i := range fw.Sync {
			fw.Sync[i] = byte(rand.Intn(256))
		}
	}
	if err = fw.writeHeader(); err != nil {
		return nil, &ErrFileWriterInit{Err: err}
	}

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

func (fw *FileWriter) Enqueue(datum interface{}) {
	fw.toBlock <- datum
}

func (fw *FileWriter) Queue() chan<- interface{} {
	return fw.toBlock
}

func ToWriter(w io.Writer) FileWriterSetter {
	return func(fw *FileWriter) error {
		fw.w = w
		return nil
	}
}

func BufferToWriter(w io.Writer) FileWriterSetter {
	return func(fw *FileWriter) error {
		fw.w = bufio.NewWriter(w)
		fw.buffered = true
		return nil
	}
}

func FileBlockSize(blockSize int64) FileWriterSetter {
	return func(fw *FileWriter) error {
		if blockSize <= 0 {
			return fmt.Errorf("blockSize must be larger than 0: %d", blockSize)
		}
		fw.blockSize = blockSize
		return nil
	}
}

// FileCompression is used to set the compression codec of
// a new File instance.
func FileCompression(someCompressionCodec string) FileWriterSetter {
	return func(fw *FileWriter) error {
		fw.CompressionCodec = someCompressionCodec
		return nil
	}
}

// FileSchema is used to set the Avro schema of a new File
// instance.
func FileSchema(someSchema string) FileWriterSetter {
	return func(fw *FileWriter) error {
		var err error
		fw.DataSchema = someSchema
		fw.dataCodec, err = NewCodec(fw.DataSchema)
		if err != nil {
			return fmt.Errorf("error compiling schema: %v", err)
		}
		return nil
	}
}

// FileSync is used to set the sync marker bytes of a new File
// instance. It checks to ensure the byte slice is 16 bytes long, but
// does not check that it has been set to something other than the
// zero value. Usually you can elide the `FileSync` call and allow it
// to create a random byte sequence.
func FileSync(someSync []byte) FileWriterSetter {
	return func(fw *FileWriter) error {
		if syncLength != len(someSync) {
			return fmt.Errorf("sync marker ought to be %d bytes long: %d", syncLength, len(someSync))
		}
		fw.Sync = make([]byte, syncLength)
		copy(fw.Sync, someSync)
		return nil
	}
}

func (fw *FileWriter) writeHeader() error {
	_, err := fw.w.Write([]byte(magicBytes))
	if err != nil {
		return err
	}
	// header metadata
	hm := make(map[string]interface{})
	hm["avro.codec"] = []byte(fw.CompressionCodec)
	hm["avro.schema"] = []byte(fw.DataSchema)
	if err = metadataCodec.Encode(fw.w, hm); err != nil {
		return err
	}
	_, err = fw.w.Write(fw.Sync)
	return err
}

type writerBlock struct {
	items      []interface{}
	encoded    *bytes.Buffer
	compressed []byte
	err        error
}

func blocker(fw *FileWriter, toBlock <-chan interface{}, toEncode chan<- *writerBlock) {
	items := make([]interface{}, 0, fw.blockSize)
	for item := range toBlock {
		items = append(items, item)
		if int64(len(items)) == fw.blockSize {
			toEncode <- &writerBlock{items: items}
			items = make([]interface{}, 0)
		}
	}
	if len(items) > 0 {
		// log.Printf("[DEBUG] blocker emptying last %d items", len(items))
		toEncode <- &writerBlock{items: items}
	}
	close(toEncode)
}

func encoder(fw *FileWriter, toEncode <-chan *writerBlock, toCompress chan<- *writerBlock) {
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

func compressor(fw *FileWriter, toCompress <-chan *writerBlock, toWrite chan<- *writerBlock) {
	switch fw.CompressionCodec {
	case CompressionNull:
		for block := range toCompress {
			block.compressed = block.encoded.Bytes()
			toWrite <- block
		}
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
	case CompressionSnappy:
		bb := new(bytes.Buffer)
		comp := snappy.NewWriter(bb)
		for block := range toCompress {
			_, block.err = comp.Write(block.encoded.Bytes())
			block.compressed = bb.Bytes()
			toWrite <- block
			bb = new(bytes.Buffer)
			comp.Reset(bb)
		}
	}
	close(toWrite)
}

func writer(fw *FileWriter, toWrite <-chan *writerBlock) {
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
