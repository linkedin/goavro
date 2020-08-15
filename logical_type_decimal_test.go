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
	"math/big"
	"testing"
)

func TestLogicalTypeBytesDecimal(t *testing.T) {
	testSchemaInvalid(t, `{"type": "bytes", "logicalType": "decimal"}`, "precision")
	testSchemaInvalid(t, `{"type": "fixed", "size": 16, "logicalType": "decimal"}`, "precision")

}

func TestDecimalBytesLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}`
	testBinaryCodecPass(t, schema, big.NewRat(617, 50), []byte("\x04\x04\xd2"))
	testBinaryCodecPass(t, schema, big.NewRat(-617, 50), []byte("\x04\xfb\x2e"))
	testBinaryCodecPass(t, schema, big.NewRat(0, 1), []byte("\x02\x00"))
}

func TestDecimalFixedLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "fixed", "size": 12, "logicalType": "decimal", "precision": 4, "scale": 2}`
	testBinaryCodecPass(t, schema, big.NewRat(617, 50), []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\xd2"))
	testBinaryCodecPass(t, schema, big.NewRat(-617, 50), []byte("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xfb\x2e"))
	testBinaryCodecPass(t, schema, big.NewRat(25, 4), []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x71"))
	testBinaryCodecPass(t, schema, big.NewRat(33, 100), []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x21"))
	schema0scale := `{"type": "fixed", "size": 12, "logicalType": "decimal", "precision": 4, "scale": 0}`
	// Encodes to 12 due to scale: 0
	testBinaryEncodePass(t, schema0scale, big.NewRat(617, 50), []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0c"))
	testBinaryDecodePass(t, schema0scale, big.NewRat(12, 1), []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0c"))
}

func TestDecimalBytesLogicalTypeInRecordEncode(t *testing.T) {
	schema := `{"type": "record", "name": "myrecord", "fields" : [
	       {"name": "mydecimal", "type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}]}`
	testBinaryCodecPass(t, schema, map[string]interface{}{"mydecimal": big.NewRat(617, 50)}, []byte("\x04\x04\xd2"))
}
