// Copyright [2017] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro_test

import (
	"bytes"
	"testing"

	"github.com/linkedin/goavro"
)

// testOCFRoundTrip has OCFWriter write to a buffer using specified
// compression algorithm, then attempt to read it back
func testOCFRoundTrip(t *testing.T, compressionName string) {
	schema := `{"type":"long"}`

	bb := new(bytes.Buffer)
	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               bb,
		CompressionName: compressionName,
		Schema:          schema,
	})
	if err != nil {
		t.Fatal(err)
	}

	valuesToWrite := []int64{13, 42, -12, -1234}

	if err = ocfw.Append(valuesToWrite); err != nil {
		t.Fatal(err)
	}

	ocfr, err := goavro.NewOCFReader(bb)
	if err != nil {
		t.Fatal(err)
	}

	var valuesRead []int64
	for ocfr.Scan() {
		value, err := ocfr.Read()
		if err != nil {
			t.Fatal(err)
		}
		valuesRead = append(valuesRead, value.(int64))
	}

	if err = ocfr.Err(); err != nil {
		t.Fatal(err)
	}

	if actual, expected := len(valuesRead), len(valuesToWrite); actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}
	for i := 0; i < len(valuesRead); i++ {
		if actual, expected := valuesRead[i], valuesToWrite[i]; actual != expected {
			t.Errorf("Actual: %v; Expected: %v", actual, expected)
		}
	}
}

func TestOCFWriterCompressionNull(t *testing.T) {
	testOCFRoundTrip(t, goavro.CompressionNullLabel)
}

func TestOCFWriterCompressionDeflate(t *testing.T) {
	testOCFRoundTrip(t, goavro.CompressionDeflateLabel)
}

func TestOCFWriterCompressionSnappy(t *testing.T) {
	testOCFRoundTrip(t, goavro.CompressionSnappyLabel)
}
