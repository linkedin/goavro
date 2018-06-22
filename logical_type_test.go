package goavro

import (
	"math/big"
	"testing"
	"time"
)

func TestSchemaLogicalType(t *testing.T) {
	testSchemaValid(t, `{"type": "long", "logicalType": "timestamp-millis"}`)
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
	testBinaryCodecPass(t, schema, big.NewRat(-617, 50), []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xfb\x2e"))
	testBinaryCodecPass(t, schema, big.NewRat(25, 4), []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x71"))
	testBinaryCodecPass(t, schema, big.NewRat(33, 100), []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x21"))
	schema0scale := `{"type": "fixed", "size": 12, "logicalType": "decimal", "precision": 4, "scale": 0}`
	// Encodes to 12 due to scale: 0
	testBinaryEncodePass(t, schema0scale, big.NewRat(617, 50), []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0c"))
	testBinaryDecodePass(t, schema0scale, big.NewRat(12, 1), []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0c"))
}
