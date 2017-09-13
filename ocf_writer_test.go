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
	"io"
	"os"
	"testing"
)

// createTestFile is used to create a new test file fixture with provided data
func createTestFile(t *testing.T, pathname string, data []byte) {
	nf, err := os.Create(pathname)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = nf.Write(data); err != nil {
		t.Fatal(err)
	}
	if err = nf.Close(); err != nil {
		t.Fatal(err)
	}
}

// NOTE: already tested readOCFHeader

func TestNewOCFWriterWhenNotFileNewOCFHeader(t *testing.T) {
	// when config.W nil
	_, err := NewOCFWriter(OCFConfig{})
	ensureError(t, err, "cannot create OCFWriter", "when W is nil")

	// when config.CompressionName invalid
	_, err = NewOCFWriter(OCFConfig{W: new(bytes.Buffer), CompressionName: "*invalid*compression*algorithm*"})
	ensureError(t, err, "cannot create OCFWriter", "unrecognized compression algorithm")

	// when config.Schema doesn't compile
	_, err = NewOCFWriter(OCFConfig{W: new(bytes.Buffer), CompressionName: "null", Schema: "invalid-schema"})
	ensureError(t, err, "cannot create OCFWriter", "cannot unmarshal schema")

	_, err = NewOCFWriter(OCFConfig{W: new(bytes.Buffer), CompressionName: "null", Schema: `{}`})
	ensureError(t, err, "cannot create OCFWriter", "missing type")

	_, err = NewOCFWriter(OCFConfig{W: new(bytes.Buffer), CompressionName: "null"})
	ensureError(t, err, "cannot create OCFWriter", "without either Codec or Schema specified")
}

func TestNewOCFWriterWhenNotFileWriteOCFHeader(t *testing.T) {
	_, err := NewOCFWriter(OCFConfig{
		W:               ShortWriter(new(bytes.Buffer), 3),
		CompressionName: "null",
		Schema:          `{"type":"int"}`},
	)
	ensureError(t, err, "cannot write OCF header", "short write")
}

func TestNewOCFWriterWhenFileEmpty(t *testing.T) {
	// NOTE: When given an empty file, NewOCFWriter ought to behave exactly as
	// if it's merely given an non-file io.Writer.
	fh, err := os.OpenFile("fixtures/temp0.avro", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewOCFWriter(OCFConfig{
		W:               fh,
		CompressionName: "*invalid*",
		Schema:          `{"type":"int"}`},
	)
	ensureError(t, err, "cannot create OCFWriter", "unrecognized compression algorithm")
}

func TestNewOCFWriterWhenFileNotEmptyWhenCannotReadOCFHeader(t *testing.T) {
	fh, err := os.Open("fixtures/bad-header.avro")
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewOCFWriter(OCFConfig{
		W:               fh,
		CompressionName: "*invalid*",
		Schema:          `{"type":"int"}`},
	)
	ensureError(t, err, "cannot create OCFWriter", "cannot read OCF header")
}

func testNewOCFWriterWhenFile(t *testing.T, pathname string, expected ...string) {
	fh, err := os.Open(pathname)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fh.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	_, err = NewOCFWriter(OCFConfig{W: fh})
	ensureError(t, err, append([]string{"cannot create OCFWriter"}, expected...)...)
}

func TestNewOCFWriterWhenFileNotEmptyWhenCannotQuickScanToTail(t *testing.T) {
	testNewOCFWriterWhenFile(t, "fixtures/firstBlockCountNotGreaterThanZero.avro", "block count is not greater")
	testNewOCFWriterWhenFile(t, "fixtures/blockCountExceedsMaxBlockCount.avro", "block count exceeds")
	testNewOCFWriterWhenFile(t, "fixtures/cannotReadBlockSize.avro", "cannot read block size")
	testNewOCFWriterWhenFile(t, "fixtures/blockSizeNotGreaterThanZero.avro", "block size is not greater than 0")
	testNewOCFWriterWhenFile(t, "fixtures/blockSizeExceedsMaxBlockSize.avro", "block size exceeds")
	testNewOCFWriterWhenFile(t, "fixtures/cannotDiscardBlockBytes.avro", "cannot seek to next block", "EOF")
	testNewOCFWriterWhenFile(t, "fixtures/cannotReadSyncMarker.avro", "cannot read sync marker", "EOF")
	testNewOCFWriterWhenFile(t, "fixtures/syncMarkerMismatch.avro", "sync marker mismatch")
	testNewOCFWriterWhenFile(t, "fixtures/secondBlockCountZero.avro", "block count is not greater")
}

func TestNewOCFWriterWhenFileNotEmptyWhenProvidedDifferentCompressionAndSchema(t *testing.T) {
	createTestFile(t, "fixtures/temp1.avro", []byte("Obj\x01\x04\x14avro.codec\x0edeflate\x16avro.schema\x1e{\"type\":\"long\"}\x000123456789abcdef\x02\x04ab0123456789abcdef"))
	fh, err := os.Open("fixtures/temp1.avro")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fh.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	ocfw, err := NewOCFWriter(OCFConfig{
		W:               fh,
		Schema:          `{"type":"int"}`,
		CompressionName: "null",
	})
	if err != nil {
		t.Fatal(err)
	}

	if actual, expected := ocfw.Codec().Schema(), `{"type":"long"}`; actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}
	if actual, expected := ocfw.CompressionName(), CompressionDeflateLabel; actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}
}

func TestOCFWriterAppendWhenCannotWrite(t *testing.T) {
	testPathname := "fixtures/temp2.avro"
	createTestFile(t, testPathname, []byte("Obj\x01\x02\x16avro.schema\x1e{\"type\":\"long\"}\x000123456789abcdef"))
	appender, err := os.OpenFile(testPathname, os.O_RDONLY, 0666) // open for read only will cause expected error when attempt to append
	if err != nil {
		t.Fatal(err)
	}
	defer func(ioc io.Closer) {
		if err := ioc.Close(); err != nil {
			t.Fatal(err)
		}
	}(appender)

	ocfw, err := NewOCFWriter(OCFConfig{W: appender})
	if err != nil {
		t.Fatal(err)
	}

	err = ocfw.Append([]interface{}{13, 42})
	ensureError(t, err, "bad file descriptor")
}

func TestOCFWriterAppendSomeItemsToNothing(t *testing.T) {
	testPathname := "fixtures/temp3.avro"
	createTestFile(t, testPathname, []byte("Obj\x01\x02\x16avro.schema\x1e{\"type\":\"long\"}\x000123456789abcdef"))
	appender, err := os.OpenFile(testPathname, os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer func(ioc io.Closer) {
		if err := ioc.Close(); err != nil {
			t.Fatal(err)
		}
	}(appender)

	ocfw, err := NewOCFWriter(OCFConfig{W: appender})
	if err != nil {
		t.Fatal(err)
	}

	if err = ocfw.Append([]interface{}{13, 42}); err != nil {
		t.Fatal(err)
	}

	// let's make sure data is there
	reader, err := os.Open(testPathname)
	if err != nil {
		t.Fatal(err)
	}
	defer func(ioc io.Closer) {
		if err := ioc.Close(); err != nil {
			t.Fatal(err)
		}
	}(reader)

	ocfr, err := NewOCFReader(reader)
	if err != nil {
		t.Fatal(err)
	}

	var values []int64
	for ocfr.Scan() {
		value, err := ocfr.Read()
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, value.(int64))
	}
	if err := ocfr.Err(); err != nil {
		t.Fatal(err)
	}

	if actual, expected := len(values), 2; actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}
	if actual, expected := values[0], int64(13); actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}
	if actual, expected := values[1], int64(42); actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}
}

func TestOCFWriterAppendSomeItemsToSomeItems(t *testing.T) {
	testPathname := "fixtures/temp4.avro"
	createTestFile(t, testPathname, []byte("Obj\x01\x02\x16avro.schema\x1e{\"type\":\"long\"}\x000123456789abcdef\x04\x04\x1a\x540123456789abcdef"))
	appender, err := os.OpenFile(testPathname, os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer func(ioc io.Closer) {
		if err := ioc.Close(); err != nil {
			t.Fatal(err)
		}
	}(appender)

	ocfw, err := NewOCFWriter(OCFConfig{W: appender})
	if err != nil {
		t.Fatal(err)
	}

	if err = ocfw.Append([]interface{}{-10, -100}); err != nil {
		t.Fatal(err)
	}

	// let's make sure data is there
	reader, err := os.Open(testPathname)
	if err != nil {
		t.Fatal(err)
	}
	defer func(ioc io.Closer) {
		if err := ioc.Close(); err != nil {
			t.Fatal(err)
		}
	}(reader)

	ocfr, err := NewOCFReader(reader)
	if err != nil {
		t.Fatal(err)
	}

	var values []int64
	for ocfr.Scan() {
		value, err := ocfr.Read()
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, value.(int64))
	}
	if err := ocfr.Err(); err != nil {
		t.Fatal(err)
	}

	if actual, expected := len(values), 4; actual != expected {
		t.Fatalf("Actual: %v; Expected: %v", actual, expected)
	}
	if actual, expected := values[0], int64(13); actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}
	if actual, expected := values[1], int64(42); actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}
	if actual, expected := values[2], int64(-10); actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}
	if actual, expected := values[3], int64(-100); actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}
}
