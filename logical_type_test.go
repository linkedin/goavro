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
	"fmt"
	"math/big"
	"testing"
	"time"
)

func TestSchemaLogicalType(t *testing.T) {
	testSchemaValid(t, `{"type": "long", "logicalType": "timestamp-millis"}`)
	testSchemaInvalid(t, `{"type": "bytes", "logicalType": "decimal"}`, "precision")
	testSchemaInvalid(t, `{"type": "fixed", "size": 16, "logicalType": "decimal"}`, "precision")
}

func TestStringLogicalTypeFallback(t *testing.T) {
	schema := `{"type": "string", "logicalType": "this_logical_type_does_not_exist"}`
	testSchemaValid(t, schema)
	testBinaryCodecPass(t, schema, "test string", []byte("\x16\x74\x65\x73\x74\x20\x73\x74\x72\x69\x6e\x67"))
}

func TestLongLogicalTypeFallback(t *testing.T) {
	schema := `{"type": "long", "logicalType": "this_logical_type_does_not_exist"}`
	testSchemaValid(t, schema)
	testBinaryCodecPass(t, schema, 12345, []byte("\xf2\xc0\x01"))
}

func TestTimeStampMillisLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "long", "logicalType": "timestamp-millis"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	t.Skip("this test is broken")
	testBinaryEncodeFail(t, schema, "test", "cannot transform binary timestamp-millis, expected time.Time")
	testBinaryCodecPass(t, schema, time.Date(2006, 1, 2, 15, 04, 05, 565000000, time.UTC), []byte("\xfa\x82\xac\xba\x91\x42"))
}

func TestTimeStampMillisLogicalTypeUnionEncode(t *testing.T) {
	schema := `{"type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]}`
	testBinaryEncodeFail(t, schema, Union("string", "test"), "cannot encode binary union: no member schema types support datum: allowed types: [null long.timestamp-millis]")
	testBinaryCodecPass(t, schema, Union("long.timestamp-millis", time.Date(2006, 1, 2, 15, 04, 05, 565000000, time.UTC)), []byte("\x02\xfa\x82\xac\xba\x91\x42"))
}

func TestTimeStampMicrosLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "long", "logicalType": "timestamp-micros"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	t.Skip("this test is broken")
	testBinaryEncodeFail(t, schema, "test", "cannot transform binary timestamp-micros, expected time.Time")
	testBinaryCodecPass(t, schema, time.Date(2006, 1, 2, 15, 04, 05, 565283000, time.UTC), []byte("\xc6\x8d\xf7\xe7\xaf\xd8\x84\x04"))
}

func TestTimeStampMicrosLogicalTypeUnionEncode(t *testing.T) {
	schema := `{"type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]}`
	testBinaryEncodeFail(t, schema, Union("string", "test"), "cannot encode binary union: no member schema types support datum: allowed types: [null long.timestamp-micros]")
	testBinaryCodecPass(t, schema, Union("long.timestamp-micros", time.Date(2006, 1, 2, 15, 04, 05, 565283000, time.UTC)), []byte("\x02\xc6\x8d\xf7\xe7\xaf\xd8\x84\x04"))
}

func TestTimeMillisLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "int", "logicalType": "time-millis"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary time-millis, expected time.Duration")
	testBinaryCodecPass(t, schema, 66904022*time.Millisecond, []byte("\xac\xff\xe6\x3f"))
}

func TestTimeMillisLogicalTypeUnionEncode(t *testing.T) {
	schema := `{"type": ["null", {"type": "int", "logicalType": "time-millis"}]}`
	testBinaryEncodeFail(t, schema, Union("string", "test"), "cannot encode binary union: no member schema types support datum: allowed types: [null int.time-millis]")
	testBinaryCodecPass(t, schema, Union("int.time-millis", 66904022*time.Millisecond), []byte("\x02\xac\xff\xe6\x3f"))
}

func TestTimeMicrosLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "long", "logicalType": "time-micros"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary time-micros, expected time.Duration")
	t.Skip("this test is broken")
	testBinaryCodecPass(t, schema, 66904022566*time.Microsecond, []byte("\xcc\xf8\xd2\xbc\xf2\x03"))
}

func TestTimeMicrosLogicalTypeUnionEncode(t *testing.T) {
	schema := `{"type": ["null", {"type": "long", "logicalType": "time-micros"}]}`
	testBinaryEncodeFail(t, schema, Union("string", "test"), "cannot encode binary union: no member schema types support datum: allowed types: [null long.time-micros]")
	t.Skip("this test is broken")
	testBinaryCodecPass(t, schema, Union("long.time-micros", 66904022566*time.Microsecond), []byte("\x02\xcc\xf8\xd2\xbc\xf2\x03"))
}
func TestDateLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "int", "logicalType": "date"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	t.Skip("this test is broken")
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary date, expected time.Time, received string")
	testBinaryCodecPass(t, schema, time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), []byte("\xbc\xcd\x01"))
}

func testGoZeroTime(t *testing.T, schema string, expected []byte) {
	t.Helper()
	testBinaryEncodePass(t, schema, time.Time{}, expected)

	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	value, remaining, err := codec.NativeFromBinary(expected)
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}

	// remaining ought to be empty because there is nothing remaining to be
	// decoded
	if actual, expected := len(remaining), 0; actual != expected {
		t.Errorf("schema: %s; Remaining; Actual: %#v; Expected: %#v", schema, actual, expected)
	}

	zeroTime, ok := value.(time.Time)
	if !ok {
		t.Fatalf("schema: %s, NativeFromBinary: expected time.Time, got %T", schema, value)
	}

	if !zeroTime.IsZero() {
		t.Fatalf("schema: %s, Check: time.Time{}.IsZero(), Actual: %t, Expected: true", schema, zeroTime.IsZero())
	}
}

func TestDateGoZero(t *testing.T) {
	testGoZeroTime(t, `{"type": "int", "logicalType": "date"}`, []byte{0xf3, 0xe4, 0x57})
}

func TestTimeStampMillisGoZero(t *testing.T) {
	testGoZeroTime(t, `{"type": "long", "logicalType": "timestamp-millis"}`, []byte{0xff, 0xdf, 0xe6, 0xa2, 0xe2, 0xa0, 0x1c})
}

func TestTimeStampMicrosGoZero(t *testing.T) {
	testGoZeroTime(t, `{"type": "long", "logicalType": "timestamp-micros"}`, []byte{0xff, 0xff, 0xdd, 0xf2, 0xdf, 0xff, 0xdf, 0xdc, 0x1})
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

func ExampleUnion_logicalType() {
	// Supported logical types and their native go types:
	// * timestamp-millis - time.Time
	// * timestamp-micros - time.Time
	// * time-millis      - time.Duration
	// * time-micros      - time.Duration
	// * date             - int
	// * decimal          - big.Rat
	codec, err := NewCodec(`["null", {"type": "long", "logicalType": "timestamp-millis"}]`)
	if err != nil {
		fmt.Println(err)
	}

	// Note the usage of type.logicalType i.e. `long.timestamp-millis` to denote the type in a union. This is due to the single string naming format
	// used by goavro. Decimal can be both bytes.decimal or fixed.decimal
	bytes, err := codec.BinaryFromNative(nil, map[string]interface{}{"long.timestamp-millis": time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC)})
	if err != nil {
		fmt.Println(err)
	}

	decoded, _, err := codec.NativeFromBinary(bytes)
	if err != nil {
		fmt.Println(err)
	}
	out := decoded.(map[string]interface{})
	fmt.Printf("%#v\n", out["long.timestamp-millis"].(time.Time).String())
	// Output: "2006-01-02 15:04:05 +0000 UTC"
}
