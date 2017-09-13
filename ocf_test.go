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
	"fmt"
	"testing"
)

// testOCFRoundTripWithHeaders has OCFWriter write to a buffer using specified
// compression algorithm, then attempt to read it back
func testOCFRoundTrip(t *testing.T, compressionName string) {
	testOCFRoundTripWithHeaders(t, compressionName, nil)
}

// testOCFRoundTripWithHeaders has OCFWriter write to a buffer using specified
// compression algorithm and headers, then attempt to read it back
func testOCFRoundTripWithHeaders(t *testing.T, compressionName string, headers map[string][]byte) {
	schema := `{"type":"long"}`

	bb := new(bytes.Buffer)
	ocfw, err := NewOCFWriter(OCFConfig{
		W:               bb,
		CompressionName: compressionName,
		Schema:          schema,
		MetaData:        headers,
	})
	if err != nil {
		t.Fatal(err)
	}

	valuesToWrite := []int64{13, 42, -12, -1234}

	if err = ocfw.Append(valuesToWrite); err != nil {
		t.Fatal(err)
	}

	ocfr, err := NewOCFReader(bb)
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

	readMeta := ocfr.MetaData()
	for k, v := range headers {
		expected := fmt.Sprintf("%s", v)
		actual := fmt.Sprintf("%s", readMeta[k])
		if actual != expected {
			t.Errorf("Actual: %v; Expected: %v (%v)", actual, expected, k)
		}
	}
}

func TestOCFWriterCompressionNull(t *testing.T) {
	testOCFRoundTrip(t, CompressionNullLabel)
}

func TestOCFWriterCompressionDeflate(t *testing.T) {
	testOCFRoundTrip(t, CompressionDeflateLabel)
}

func TestOCFWriterCompressionSnappy(t *testing.T) {
	testOCFRoundTrip(t, CompressionSnappyLabel)
}

func TestOCFWriterWithApplicationMetaData(t *testing.T) {
	testOCFRoundTripWithHeaders(t, CompressionNullLabel, map[string][]byte{"foo": []byte("BOING"), "goo": []byte("zoo")})
}
