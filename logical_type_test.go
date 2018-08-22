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

func TestTimeStampMillisLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "long", "logicalType": "timestamp-millis"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	testBinaryEncodeFail(t, schema, "test", "cannot transform binary timestamp-millis, expected time.Time")
	testBinaryCodecPass(t, schema, time.Date(2006, 1, 2, 15, 04, 05, 0, time.UTC), []byte("\x90\xfa\xab\xba\x91\x42"))
}

func TestTimeStampMillisLogicalTypeUnionEncode(t *testing.T) {
	schema := `{"type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]}`
	testBinaryEncodeFail(t, schema, Union("string", "test"), "cannot encode binary union: no member schema types support datum: allowed types: [null long.timestamp-millis]")
	testBinaryCodecPass(t, schema, Union("long.timestamp-millis", time.Date(2006, 1, 2, 15, 04, 05, 0, time.UTC)), []byte("\x02\x90\xfa\xab\xba\x91\x42"))
}

func TestTimeStampMicrosLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "long", "logicalType": "timestamp-micros"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	testBinaryEncodeFail(t, schema, "test", "cannot transform binary timestamp-micros, expected time.Time")
	testBinaryCodecPass(t, schema, time.Date(2006, 1, 2, 15, 04, 05, 0, time.UTC), []byte("\x80\x8d\xb2\xe7\xaf\xd8\x84\x04"))
}

func TestTimeStampMicrosLogicalTypeUnionEncode(t *testing.T) {
	schema := `{"type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]}`
	testBinaryEncodeFail(t, schema, Union("string", "test"), "cannot encode binary union: no member schema types support datum: allowed types: [null long.timestamp-micros]")
	testBinaryCodecPass(t, schema, Union("long.timestamp-micros", time.Date(2006, 1, 2, 15, 04, 05, 0, time.UTC)), []byte("\x02\x80\x8d\xb2\xe7\xaf\xd8\x84\x04"))
}

func TestTimeMillisLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "int", "logicalType": "time-millis"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary time-millis, expected time.Duration")
	testBinaryCodecPass(t, schema, time.Duration(1*time.Second), []byte("\xd0\x0f"))
}

func TestTimeMillisLogicalTypeUnionEncode(t *testing.T) {
	schema := `{"type": ["null", {"type": "int", "logicalType": "time-millis"}]}`
	testBinaryEncodeFail(t, schema, Union("string", "test"), "cannot encode binary union: no member schema types support datum: allowed types: [null int.time-millis]")
	testBinaryCodecPass(t, schema, Union("int.time-millis", time.Duration(1*time.Second)), []byte("\x02\xd0\x0f"))
}

func TestTimeMicrosLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "long", "logicalType": "time-micros"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary time-micros, expected time.Duration")
	testBinaryCodecPass(t, schema, time.Duration(1*time.Second), []byte("\x80\x89\x7a"))
}

func TestTimeMicrosLogicalTypeUnionEncode(t *testing.T) {
	schema := `{"type": ["null", {"type": "long", "logicalType": "time-micros"}]}`
	testBinaryEncodeFail(t, schema, Union("string", "test"), "cannot encode binary union: no member schema types support datum: allowed types: [null long.time-micros]")
	testBinaryCodecPass(t, schema, Union("long.time-micros", time.Duration(1*time.Second)), []byte("\x02\x80\x89\x7a"))
}
func TestDateLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "int", "logicalType": "date"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary date, expected time.Time, received string")
	testBinaryCodecPass(t, schema, time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), []byte("\xbc\xcd\x01"))
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
