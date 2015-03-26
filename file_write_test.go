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
	"bytes"
	"io"
	"os"
	"testing"
)

var defaultSync []byte

func init() {
	defaultSync = []byte("\x21\x0f\xc7\xbb\x81\x86\x39\xac\x48\xa4\xc6\xaf\xa2\xf1\x58\x1a")
}

func TestNewFileWriterBailsUnsupportedCodec(t *testing.T) {
	var err error
	_, err = NewFileWriter(ToWriter(new(bytes.Buffer)), FileCompression(""))
	checkError(t, err, "unsupported codec")

	_, err = NewFileWriter(ToWriter(new(bytes.Buffer)), FileCompression("ficticious test codec name"))
	checkError(t, err, "unsupported codec")
}

func TestNewFileWriterBailsMissingSchema(t *testing.T) {
	var err error
	_, err = NewFileWriter(ToWriter(new(bytes.Buffer)))
	checkError(t, err, "missing schema")

	_, err = NewFileWriter(ToWriter(new(bytes.Buffer)), FileCompression(CompressionNull))
	checkError(t, err, "missing schema")

	_, err = NewFileWriter(ToWriter(new(bytes.Buffer)), FileCompression(CompressionDeflate))
	checkError(t, err, "missing schema")

	_, err = NewFileWriter(ToWriter(new(bytes.Buffer)), FileCompression(CompressionSnappy))
	checkError(t, err, "missing schema")
}

func TestNewFileWriterBailsInvalidSchema(t *testing.T) {
	_, err := NewFileWriter(FileSchema("this should not compile"))
	checkError(t, err, "compiling schema")
}

func TestNewFileWriterBailsBadSync(t *testing.T) {
	_, err := NewFileWriter(FileSchema(`"int"`), FileSync(make([]byte, 0)))
	checkError(t, err, "sync marker ought to be 16 bytes long")

	_, err = NewFileWriter(FileSchema(`"int"`), FileSync(make([]byte, syncLength-1)))
	checkError(t, err, "sync marker ought to be 16 bytes long")

	_, err = NewFileWriter(FileSchema(`"int"`), FileSync(make([]byte, syncLength+1)))
	checkError(t, err, "sync marker ought to be 16 bytes long")
}

func TestNewFileWriterCreatesRandomSync(t *testing.T) {
	bb := new(bytes.Buffer)
	func(w io.Writer) {
		fw, err := NewFileWriter(ToWriter(w), FileSchema(`"int"`))
		if err != nil {
			t.Fatalf("Actual: %#v; Expected: %#v", err, nil)
		}
		defer fw.Close()
	}(bb)

	notExpected := []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	actual := bb.Bytes()
	actual = actual[len(actual)-syncLength:]
	if bytes.Compare(actual, notExpected) == 0 {
		t.Errorf("Actual: %#v; Expected: some non-zero value bits", actual)
	}
}

func TestFileWriteHeaderCustomSync(t *testing.T) {
	bb := new(bytes.Buffer)
	func(w io.Writer) {
		fw, err := NewFileWriter(ToWriter(w), FileSchema(`"int"`), FileSync(defaultSync))
		if err != nil {
			t.Fatalf("Actual: %#v; Expected: %#v", err, nil)
		}
		fw.Close()
	}(bb)

	// NOTE: because key value pair ordering is indeterminate,
	// there are two valid possibilities for the encoded map:
	option1 := []byte("Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\x0a\x22int\x22\x00\x21\x0f\xc7\xbb\x81\x86\x39\xac\x48\xa4\xc6\xaf\xa2\xf1\x58\x1a\x00\x00")
	option2 := []byte("Obj\x01\x04\x16avro.schema\x0a\x22int\x22\x14avro.codec\x08null\x00\x21\x0f\xc7\xbb\x81\x86\x39\xac\x48\xa4\xc6\xaf\xa2\xf1\x58\x1a\x00\x00")

	actual := bb.Bytes()
	if (bytes.Compare(actual, option1) != 0) && (bytes.Compare(actual, option2) != 0) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, option1)
	}
}

func TestFileWriteWithNullCodec(t *testing.T) {
	bb := new(bytes.Buffer)
	func(w io.Writer) {
		fw, err := NewFileWriter(BufferToWriter(w), FileSchema(`"int"`), FileSync(defaultSync))
		if err != nil {
			t.Fatalf("Actual: %#v; Expected: %#v", err, nil)
		}
		defer fw.Close()
		fw.Enqueue(int32(13))
		fw.Enqueue(int32(42))
		fw.Enqueue(int32(54))
		fw.Enqueue(int32(99))
	}(bb)
	t.Logf("bb: %+v", bb.Bytes())

	// NOTE: because key value pair ordering is indeterminate,
	// there are two valid possibilities for the encoded map:
	option1 := []byte("Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\x0a\x22int\x22\x00" + string(defaultSync) + "\x08\x0a\x1a\x54\x6c\xc6\x01" + string(defaultSync) + "\x00\x00")
	option2 := []byte("Obj\x01\x04\x16avro.schema\x0a\x22int\x22\x14avro.codec\x08null\x00" + string(defaultSync) + "\x08\x0a\x1a\x54\x6c\xc6\x01" + string(defaultSync) + "\x00\x00")

	actual := bb.Bytes()
	if (bytes.Compare(actual, option1) != 0) && (bytes.Compare(actual, option2) != 0) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, option1)
	}
}

func _TestFileWriteWithDeflateCodec(t *testing.T) {
	bb := new(bytes.Buffer)
	func(w io.Writer) {
		fw, err := NewFileWriter(
			FileBlockSize(2),
			FileCompression(CompressionDeflate),
			FileSchema(`"int"`),
			FileSync(defaultSync),
			ToWriter(w))
		if err != nil {
			t.Fatalf("Actual: %#v; Expected: %#v", err, nil)
		}
		defer fw.Close()
		fw.Enqueue(int32(13))
		fw.Enqueue(int32(42))
		fw.Enqueue(int32(54))
		fw.Enqueue(int32(99))
	}(bb)

	// NOTE: because key value pair ordering is indeterminate,
	// there are two valid possibilities for the encoded map:
	option1 := []byte("Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\x0a\x22int\x22\x00" + string(defaultSync) + "\x08\x0a\x1a\x54\x6c\xc6\x01" + string(defaultSync) + "\x00\x00")
	option2 := []byte("Obj\x01\x04\x16avro.schema\x0a\x22int\x22\x14avro.codec\x08null\x00" + string(defaultSync) + "\x08\x0a\x1a\x54\x6c\xc6\x01" + string(defaultSync) + "\x00\x00")

	actual := bb.Bytes()
	if (bytes.Compare(actual, option1) != 0) && (bytes.Compare(actual, option2) != 0) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, option1)
	}
}

func TestFileWriteToDisk(t *testing.T) {
	fh, err := os.Create("test.avro")
	checkErrorFatal(t, err, nil)
	defer fh.Close()
	func(w io.Writer) {
		fw, err := NewFileWriter(
			FileBlockSize(2),
			// FileCompression(CompressionDeflate),
			FileCompression(CompressionSnappy),
			FileSchema(`"int"`),
			FileSync(defaultSync),
			ToWriter(w))
		if err != nil {
			t.Fatalf("Actual: %#v; Expected: %#v", err, nil)
		}
		defer fw.Close()
		fw.Enqueue(int32(13))
		fw.Enqueue(int32(42))
		fw.Enqueue(int32(54))
		fw.Enqueue(int32(99))
	}(fh)
}
