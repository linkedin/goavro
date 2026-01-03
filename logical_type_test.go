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
	"math"
	"math/big"
	"testing"
	"time"
)

const (
	precision = "precision"
	scale     = "scale"
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
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary timestamp-millis, expected time.Time or Go numeric")
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
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary timestamp-micros, expected time.Time or Go numeric")
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
	testBinaryCodecPass(t, schema, 66904022566*time.Microsecond, []byte("\xcc\xf8\xd2\xbc\xf2\x03"))
}

func TestTimeMicrosLogicalTypeUnionEncode(t *testing.T) {
	schema := `{"type": ["null", {"type": "long", "logicalType": "time-micros"}]}`
	testBinaryEncodeFail(t, schema, Union("string", "test"), "cannot encode binary union: no member schema types support datum: allowed types: [null long.time-micros]")
	testBinaryCodecPass(t, schema, Union("long.time-micros", 66904022566*time.Microsecond), []byte("\x02\xcc\xf8\xd2\xbc\xf2\x03"))
}
func TestDateLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "int", "logicalType": "date"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary date, expected time.Time or Go numeric, received string")
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
	// Test with a large decimal of precision 77 and scale 38
	largeDecimalSchema := `{"type": "bytes", "logicalType": "decimal", "precision": 77, "scale": 38}`
	n, _ := new(big.Int).SetString("12345678901234567890123456789012345678911111111111111111111111111111111111111", 10)
	d, _ := new(big.Int).SetString("100000000000000000000000000000000000000", 10)
	largeRat := new(big.Rat).SetFrac(n, d)
	testBinaryCodecPass(t, largeDecimalSchema, largeRat, []byte("\x40\x1b\x4b\x68\x19\x26\x11\xfa\xea\x20\x8f\xca\x21\x62\x7b\xe9\xda\xee\x32\x19\x83\x83\x95\x5d\xe8\x13\x1f\x4b\xf1\xc7\x1c\x71\xc7"))
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

	schemaPrecision1 := `{"type": "fixed", "size": 4, "logicalType": "decimal", "precision": 1, "scale": 1}`
	testBinaryCodecPass(t, schemaPrecision1, big.NewRat(163, 10), []byte("\x00\x00\x00\xa3"))
	testBinaryCodecPass(t, schemaPrecision1, big.NewRat(-130, 4), []byte("\xff\xff\xfe\xbb"))
	testBinaryCodecPass(t, schemaPrecision1, big.NewRat(25, 2), []byte("\x00\x00\x00\x7d"))
	testBinaryEncodeFail(t, schemaPrecision1, big.NewRat(math.MaxInt32, -1), "datum size ought to equal schema size")
}

func TestDecimalBytesLogicalTypeInRecordEncode(t *testing.T) {
	schema := `{"type": "record", "name": "myrecord", "fields" : [
	       {"name": "mydecimal", "type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}]}`
	testBinaryCodecPass(t, schema, map[string]interface{}{"mydecimal": big.NewRat(617, 50)}, []byte("\x04\x04\xd2"))
}

func TestDecimalBytesLogicalTypeInRecordDecodeWithDefault(t *testing.T) {
	schema := `{"type": "record", "name": "myrecord", "fields" : [
    {"name": "mydecimal", "type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2, "default":"\u0000"}]}`
	testBinaryCodecPass(t, schema, map[string]interface{}{"mydecimal": big.NewRat(617, 50)}, []byte("\x04\x04\xd2"))
}

func TestDecimalBytesTextualRoundTrip(t *testing.T) {
	schema := `{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}`
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		textual  string
		expected *big.Rat
	}{
		{`"40.20"`, big.NewRat(4020, 100)},
		{`"12.34"`, big.NewRat(1234, 100)},
		{`"-12.34"`, big.NewRat(-1234, 100)},
		{`"0.00"`, big.NewRat(0, 1)},
		{`"99.99"`, big.NewRat(9999, 100)},
	}

	for _, tc := range testCases {
		// Decode textual to native
		native, _, err := codec.NativeFromTextual([]byte(tc.textual))
		if err != nil {
			t.Fatalf("NativeFromTextual(%s): %v", tc.textual, err)
		}

		rat, ok := native.(*big.Rat)
		if !ok {
			t.Fatalf("NativeFromTextual(%s): expected *big.Rat, got %T", tc.textual, native)
		}

		if rat.Cmp(tc.expected) != 0 {
			t.Errorf("NativeFromTextual(%s): got %v, want %v", tc.textual, rat, tc.expected)
		}

		// Encode native to textual
		textual, err := codec.TextualFromNative(nil, rat)
		if err != nil {
			t.Fatalf("TextualFromNative(%v): %v", rat, err)
		}

		if string(textual) != tc.textual {
			t.Errorf("TextualFromNative(%v): got %s, want %s", rat, textual, tc.textual)
		}
	}
}

func TestDecimalFixedTextualRoundTrip(t *testing.T) {
	schema := `{"type": "fixed", "size": 12, "logicalType": "decimal", "precision": 4, "scale": 2}`
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		textual  string
		expected *big.Rat
	}{
		{`"40.20"`, big.NewRat(4020, 100)},
		{`"12.34"`, big.NewRat(1234, 100)},
		{`"-12.34"`, big.NewRat(-1234, 100)},
		{`"0.00"`, big.NewRat(0, 1)},
	}

	for _, tc := range testCases {
		// Decode textual to native
		native, _, err := codec.NativeFromTextual([]byte(tc.textual))
		if err != nil {
			t.Fatalf("NativeFromTextual(%s): %v", tc.textual, err)
		}

		rat, ok := native.(*big.Rat)
		if !ok {
			t.Fatalf("NativeFromTextual(%s): expected *big.Rat, got %T", tc.textual, native)
		}

		if rat.Cmp(tc.expected) != 0 {
			t.Errorf("NativeFromTextual(%s): got %v, want %v", tc.textual, rat, tc.expected)
		}

		// Encode native to textual
		textual, err := codec.TextualFromNative(nil, rat)
		if err != nil {
			t.Fatalf("TextualFromNative(%v): %v", rat, err)
		}

		if string(textual) != tc.textual {
			t.Errorf("TextualFromNative(%v): got %s, want %s", rat, textual, tc.textual)
		}
	}
}

func TestDecimalBytesBackwardsCompatibility(t *testing.T) {
	// Test that binary data incorrectly encoded as ASCII decimal strings
	// can still be decoded correctly when backwards compatibility is enabled
	schema := `{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}`

	// Create codec with backwards compatibility enabled
	opt := &CodecOption{EnableDecimalBinaryToTextualBackwardsCompatASCIIDecoding: true}
	codec, err := NewCodecWithOptions(schema, opt)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate incorrectly encoded data: "40.20" as ASCII bytes
	// Length prefix (10 = 0x0a in varint) + ASCII bytes for "40.20"
	incorrectlyEncodedBytes := append([]byte{0x0a}, []byte("40.20")...)

	native, _, err := codec.NativeFromBinary(incorrectlyEncodedBytes)
	if err != nil {
		t.Fatalf("NativeFromBinary (backwards compat): %v", err)
	}

	rat, ok := native.(*big.Rat)
	if !ok {
		t.Fatalf("NativeFromBinary: expected *big.Rat, got %T", native)
	}

	expected := big.NewRat(4020, 100)
	if rat.Cmp(expected) != 0 {
		t.Errorf("NativeFromBinary (backwards compat): got %v, want %v", rat, expected)
	}
}

func TestDecimalBytesCorrectBinaryEncoding(t *testing.T) {
	// Test that correctly encoded binary data (two's complement) still works
	schema := `{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}`
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	// 40.20 = 4020 with scale 2
	// 4020 in two's complement = 0x0FB4 (big-endian)
	// Avro bytes: length prefix (4 = 0x04) + 0x0F, 0xB4
	correctlyEncodedBytes := []byte{0x04, 0x0f, 0xb4}

	native, _, err := codec.NativeFromBinary(correctlyEncodedBytes)
	if err != nil {
		t.Fatalf("NativeFromBinary: %v", err)
	}

	rat, ok := native.(*big.Rat)
	if !ok {
		t.Fatalf("NativeFromBinary: expected *big.Rat, got %T", native)
	}

	expected := big.NewRat(4020, 100)
	if rat.Cmp(expected) != 0 {
		t.Errorf("NativeFromBinary: got %v, want %v", rat, expected)
	}
}

func TestDecimalTextualToBinaryRoundTrip(t *testing.T) {
	// Test the full flow: textual -> native -> binary -> native -> textual
	schema := `{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}`
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	originalTextual := []byte(`"40.20"`)

	// Step 1: Textual -> Native
	native1, _, err := codec.NativeFromTextual(originalTextual)
	if err != nil {
		t.Fatalf("NativeFromTextual: %v", err)
	}

	// Step 2: Native -> Binary
	binary, err := codec.BinaryFromNative(nil, native1)
	if err != nil {
		t.Fatalf("BinaryFromNative: %v", err)
	}

	// Verify binary is two's complement, not ASCII string
	// 4020 = 0x0FB4 in hex
	expectedBinary := []byte{0x04, 0x0f, 0xb4}
	if string(binary) != string(expectedBinary) {
		t.Errorf("BinaryFromNative: got %x, want %x", binary, expectedBinary)
	}

	// Step 3: Binary -> Native
	native2, _, err := codec.NativeFromBinary(binary)
	if err != nil {
		t.Fatalf("NativeFromBinary: %v", err)
	}

	// Step 4: Native -> Textual
	textual, err := codec.TextualFromNative(nil, native2)
	if err != nil {
		t.Fatalf("TextualFromNative: %v", err)
	}

	if string(textual) != string(originalTextual) {
		t.Errorf("Round-trip failed: got %s, want %s", textual, originalTextual)
	}
}

func TestLooksLikeASCIIDecimal(t *testing.T) {
	testCases := []struct {
		input    []byte
		expected bool
	}{
		{[]byte("40.20"), true},
		{[]byte("-40.20"), true},
		{[]byte("+40.20"), true},
		{[]byte(".5"), true},
		{[]byte("5."), true},
		{[]byte("0.0"), true},
		{[]byte("0"), false},        // no decimal point - could be valid two's complement
		{[]byte("123456"), false},   // no decimal point - could be valid two's complement
		{[]byte(""), false},         // empty
		{[]byte("-"), false},        // no digit
		{[]byte("."), false},        // no digit
		{[]byte("40.20.30"), false}, // multiple dots
		{[]byte("40-20"), false},    // sign not at start
		{[]byte("40a20"), false},    // non-decimal char
		{[]byte("\x0f\xb4"), false}, // binary data (two's complement)
		{[]byte{0x00}, false},       // null byte
		{[]byte{0xff, 0xff}, false}, // high bytes (negative two's complement)
		{[]byte("12.34e5"), false},  // scientific notation not supported
	}

	for _, tc := range testCases {
		result := looksLikeASCIIDecimal(tc.input)
		if result != tc.expected {
			t.Errorf("looksLikeASCIIDecimal(%q): got %v, want %v", tc.input, result, tc.expected)
		}
	}
}

func TestDecimalDefaultNoBackwardsCompat(t *testing.T) {
	// Test that by default, ASCII-looking bytes are NOT interpreted as ASCII
	// but rather as two's-complement (which is the correct spec behavior)
	schema := `{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}`

	// Create codec with default options (no backwards compat)
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	// "09" as ASCII bytes is [0x30, 0x39] which is 12345 in decimal
	// This could be misinterpreted as the string "09" if backwards compat were on
	// But with default settings, it should be decoded as 12345/100 = 123.45
	asciiLookingBytes := []byte{0x04, 0x30, 0x39} // length=2, bytes="09"

	native, _, err := codec.NativeFromBinary(asciiLookingBytes)
	if err != nil {
		t.Fatalf("NativeFromBinary: %v", err)
	}

	rat, ok := native.(*big.Rat)
	if !ok {
		t.Fatalf("NativeFromBinary: expected *big.Rat, got %T", native)
	}

	// 0x3039 = 12345, with scale 2 = 123.45 = 12345/100
	expected := big.NewRat(12345, 100)
	if rat.Cmp(expected) != 0 {
		t.Errorf("NativeFromBinary (default, no backwards compat): got %v, want %v", rat, expected)
	}
}

func TestDecimalNegativeBackwardsCompatibility(t *testing.T) {
	// Test backwards compatibility with negative numbers encoded as ASCII
	schema := `{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}`

	// Create codec with backwards compatibility enabled
	opt := &CodecOption{EnableDecimalBinaryToTextualBackwardsCompatASCIIDecoding: true}
	codec, err := NewCodecWithOptions(schema, opt)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate incorrectly encoded data: "-40.20" as ASCII bytes
	incorrectlyEncodedBytes := append([]byte{0x0c}, []byte("-40.20")...)

	native, _, err := codec.NativeFromBinary(incorrectlyEncodedBytes)
	if err != nil {
		t.Fatalf("NativeFromBinary (backwards compat): %v", err)
	}

	rat, ok := native.(*big.Rat)
	if !ok {
		t.Fatalf("NativeFromBinary: expected *big.Rat, got %T", native)
	}

	expected := big.NewRat(-4020, 100)
	if rat.Cmp(expected) != 0 {
		t.Errorf("NativeFromBinary (backwards compat): got %v, want %v", rat, expected)
	}
}

func TestValidatedStringLogicalTypeInRecordEncode(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "myrecord",
		"fields": [
			{
				"name": "number",
				"doc": "Phone number inside the national network. Length between 4-14",
				"type": {
					  "type": "string",
					  "logicalType": "validatedString",
					  "pattern": "^[\\d]{4,14}$"
				}
			}
		]
	  }`

	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	// NOTE: May omit fields when using default value
	textual := []byte(`{"number": "667777777"}`)

	// Convert textual Avro data (in Avro JSON format) to native Go form
	native, _, err := codec.NativeFromTextual(textual)
	if err != nil {
		t.Fatal(err)
	}

	// Convert native Go form to binary Avro data
	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		t.Fatal(err)
	}

	testSchemaValid(t, schema)
	testBinaryCodecPass(t, schema, map[string]interface{}{"number": "667777777"}, binary)

	// Convert binary Avro data back to native Go form
	native, _, err = codec.NativeFromBinary(binary)
	if err != nil {
		t.Fatal(err)
	}

	// Convert native Go form to textual Avro data
	textual, err = codec.TextualFromNative(nil, native)
	if err != nil {
		t.Fatal(err)
	}

	// NOTE: Textual encoding will show all fields, even those with values that
	// match their default values
	if got, want := string(textual), "{\"number\":\"667777777\"}"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
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

func TestPrecisionAndScaleFromSchemaMapValidation(t *testing.T) {
	testCasesInvalid := []struct {
		schemaMap map[string]interface{}
		errMsg    string
	}{
		{map[string]interface{}{}, "cannot create decimal logical type without precision"},
		{map[string]interface{}{
			precision: true,
		}, "wrong precision type"},
		{map[string]interface{}{
			precision: float64(0),
		}, "precision is less than one"},
		{map[string]interface{}{
			precision: float64(2),
			scale:     true,
		}, "wrong scale type"},
		{map[string]interface{}{
			precision: float64(2),
			scale:     float64(-1),
		}, "scale is less than zero"},
		{map[string]interface{}{
			precision: float64(2),
			scale:     float64(3),
		}, "scale is larger than precision"},
	}
	for _, tc := range testCasesInvalid {
		_, _, err := precisionAndScaleFromSchemaMap(tc.schemaMap)
		ensureError(t, err, tc.errMsg)
	}

	// validation passes
	p, s, err := precisionAndScaleFromSchemaMap(map[string]interface{}{
		precision: float64(1),
		scale:     float64(1),
	})
	if p != 1 || s != 1 || err != nil {
		t.Errorf("GOT: %v %v %v; WANT: 1 1 nil", p, s, err)
	}
}
