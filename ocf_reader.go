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
	"io/ioutil"
	"strings"
)

type ErrReaderInit struct {
	Message string
	Err     error
}

func (e *ErrReaderInit) Error() string {
	if e.Err == nil {
		return "cannot create Reader: " + e.Message
	} else if e.Message == "" {
		return "cannot create Reader: " + e.Err.Error()
	} else {
		return "cannot create Reader: " + e.Message + "; " + e.Err.Error()
	}
}

type ErrReaderBlockCount struct {
	Err error
}

func (e *ErrReaderBlockCount) Error() string {
	return "cannot read block count and size: " + e.Err.Error()
}

// ReaderSetter functions are those those which are used to instantiate
// a new Reader.
type ReaderSetter func(*Reader) error

// Reader wraps the specified `io.Reader` using a `bufio.Reader`
// to read from a file.
func BufferFromReader(r io.Reader) ReaderSetter {
	return func(fr *Reader) error {
		fr.r = bufio.NewReader(r)
		return nil
	}
}

// Reader specifies the `io.Reader` to use when reading a file.
func FromReader(r io.Reader) ReaderSetter {
	return func(fr *Reader) error {
		fr.r = r
		return nil
	}
}

// Reader structure contains data necessary to read Avro files.
type Reader struct {
	CompressionCodec string
	DataSchema       string
	Sync             []byte
	dataCodec        Codec
	datum            Datum
	deblocked        chan Datum
	done             bool
	err              error
	r                io.Reader
}

// NewReader returns a object to read data from an io.Reader using the
// Avro Object Container Files format.
//
//     func main() {
//         conn, err := net.Dial("tcp", "127.0.0.1:8080")
//         if err != nil {
//             log.Fatal(err)
//         }
//         fr, err := goavro.NewReader(goavro.FromReader(conn))
//         if err != nil {
//             log.Fatal("cannot create Reader: ", err)
//         }
//         defer func() {
//             if err := fr.Close(); err != nil {
//                 log.Fatal(err)
//             }
//         }()
//
//         for fr.Scan() {
//             datum, err := fr.Read()
//             if err != nil {
//                 log.Println("cannot read datum: ", err)
//                 continue
//             }
//             fmt.Println("RECORD: ", datum)
//         }
//     }
func NewReader(setters ...ReaderSetter) (*Reader, error) {
	var err error
	fr := &Reader{}
	for _, setter := range setters {
		err = setter(fr)
		if err != nil {
			return nil, &ErrReaderInit{Err: err}
		}
	}
	if fr.r == nil {
		return nil, &ErrReaderInit{Message: "must specify io.Reader"}
	}
	// read in header information and use it to initialize Reader
	magic := make([]byte, 4)
	_, err = fr.r.Read(magic)
	if err != nil {
		return nil, &ErrReaderInit{Message: "cannot read magic number", Err: err}
	}
	if bytes.Compare(magic, []byte(magicBytes)) != 0 {
		return nil, &ErrReaderInit{Message: "invalid magic number: " + string(magic)}
	}
	meta, err := decodeHeaderMetadata(fr.r)
	if err != nil {
		return nil, &ErrReaderInit{Message: "cannot read header metadata", Err: err}
	}
	fr.CompressionCodec, err = getHeaderString("avro.codec", meta)
	if err != nil {
		return nil, &ErrReaderInit{Message: "cannot read header metadata", Err: err}
	}
	if !IsCompressionCodecSupported(fr.CompressionCodec) {
		return nil, &ErrWriterInit{Message: fmt.Sprintf("unsupported codec: %s", fr.CompressionCodec)}
	}
	fr.DataSchema, err = getHeaderString("avro.schema", meta)
	if err != nil {
		return nil, &ErrReaderInit{Message: "cannot read header metadata", Err: err}
	}
	if fr.dataCodec, err = NewCodec(fr.DataSchema); err != nil {
		return nil, &ErrWriterInit{Message: "cannot compile schema", Err: err}
	}
	fr.Sync = make([]byte, syncLength)
	if _, err = fr.r.Read(fr.Sync); err != nil {
		return nil, &ErrReaderInit{Message: "cannot read sync marker", Err: err}
	}
	// setup reading pipeline
	toDecompress := make(chan *readerBlock)
	toDecode := make(chan *readerBlock)
	fr.deblocked = make(chan Datum)
	go read(fr, toDecompress)
	go decompress(fr, toDecompress, toDecode)
	go decode(fr, toDecode)
	return fr, nil
}

// Close releases resources and returns any Reader errors.
func (fr *Reader) Close() error {
	return fr.err
}

// Scan returns true if more data is ready to be read.
func (fr *Reader) Scan() bool {
	fr.datum = <-fr.deblocked
	return !fr.done
}

// Read returns the next element from the Reader.
func (fr *Reader) Read() (interface{}, error) {
	return fr.datum.Value, fr.datum.Err
}

func decodeHeaderMetadata(r io.Reader) (map[string]interface{}, error) {
	md, err := metadataCodec.Decode(r)
	if err != nil {
		return nil, err
	}
	return md.(map[string]interface{}), nil
}

func getHeaderString(someKey string, header map[string]interface{}) (string, error) {
	v, ok := header[someKey]
	if !ok {
		return "", fmt.Errorf("header ought to have %v key", someKey)
	}
	return string(v.([]byte)), nil
}

type readerBlock struct {
	data       []Datum
	datumCount int
	err        error
	r          io.Reader
}

func read(fr *Reader, toDecompress chan<- *readerBlock) {
	// NOTE: these variables created outside loop to eliminate churn
	var lr io.Reader
	var bits []byte
	sync := make([]byte, syncLength)

	blockCount, blockSize, err := readBlockCountAndSize(fr.r)
	if err != nil {
		blockCount = 0
	}
	for blockCount != 0 {
		lr = io.LimitReader(fr.r, int64(blockSize))
		if bits, err = ioutil.ReadAll(lr); err != nil {
			break
		}
		toDecompress <- &readerBlock{datumCount: blockCount, r: bytes.NewReader(bits)}
		if _, err = fr.r.Read(sync); err != nil {
			err = fmt.Errorf("cannot read sync marker: %v", err)
			break
		}
		if bytes.Compare(fr.Sync, sync) != 0 {
			err = fmt.Errorf("sync marker mismatch: %#v != %#v", sync, fr.Sync)
			break
		}
		if blockCount, blockSize, err = readBlockCountAndSize(fr.r); err != nil {
			break
		}
	}
	if err != nil {
		fr.err = fmt.Errorf("error reading: %v", err)
	}
	close(toDecompress)
}

func readBlockCountAndSize(r io.Reader) (blockCount, blockSize int, err error) {
	bc, err := longCodec.Decode(r)
	switch err {
	case io.EOF:
		// we're done
		return 0, 0, nil
	case nil:
		// not really an error: ignore
	default:
		// TODO: this could be optimized
		if strings.Contains(err.Error(), "EOF") {
			// we're done
			return 0, 0, nil
		}
		return 0, 0, &ErrReaderBlockCount{err}
	}
	bs, err := longCodec.Decode(r)
	if err != nil {
		return 0, 0, &ErrReaderBlockCount{err}
	}
	return int(bc.(int64)), int(bs.(int64)), nil
}

func decompress(fr *Reader, toDecompress <-chan *readerBlock, toDecode chan<- *readerBlock) {
	switch fr.CompressionCodec {
	case CompressionDeflate:
		var rc io.ReadCloser
		var bits []byte
		for block := range toDecompress {
			rc = flate.NewReader(block.r)
			bits, block.err = ioutil.ReadAll(rc)
			if block.err != nil {
				block.err = fmt.Errorf("cannot read from deflate: %v", block.err)
				toDecode <- block
				rc.Close() // ignore any close error
				continue
			}
			block.err = rc.Close()
			if block.err != nil {
				block.err = fmt.Errorf("cannot close deflate: %v", block.err)
				toDecode <- block
				continue
			}
			block.r = bytes.NewReader(bits)
			toDecode <- block
		}
	case CompressionNull:
		for block := range toDecompress {
			toDecode <- block
		}
	case CompressionSnappy:
		var src, dst []byte
		for block := range toDecompress {
			src, block.err = ioutil.ReadAll(block.r)
			if block.err != nil {
				block.err = fmt.Errorf("cannot read: %v", block.err)
				toDecode <- block
				continue
			}
			dst, block.err = snappy.Decode(dst, src)
			if block.err != nil {
				block.err = fmt.Errorf("cannot decompress: %v", block.err)
				toDecode <- block
				continue
			}
			block.r = bytes.NewReader(dst)
			toDecode <- block
		}
	}
	close(toDecode)
}

func decode(fr *Reader, toDecode <-chan *readerBlock) {
decodeLoop:
	for block := range toDecode {
		for i := 0; i < block.datumCount; i++ {
			var datum Datum
			datum.Value, datum.Err = fr.dataCodec.Decode(block.r)
			if datum.Value == nil && datum.Err == nil {
				break decodeLoop
			}
			fr.deblocked <- datum
		}
	}
	fr.done = true
	close(fr.deblocked)
}
