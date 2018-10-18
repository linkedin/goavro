package goavro

import (
	"reflect"
	"testing"
)

func testPrimitiveRecoverNative(t *testing.T, schema string, value interface{}) {
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatalf("Schema: %s; %s", schema, err)
	}

	// native -> binary -> native
	binary, err := codec.BinaryFromNative(nil, value)
	if err != nil {
		t.Fatalf("Datum: %v; %s", value, err)
	}
	native, _, err := codec.NativeFromBinary(binary)
	if err != nil {
		t.Fatalf("Datum: %s; %s", binary, err)
	}
	if reflect.TypeOf(value) != reflect.TypeOf(native) {
		t.Fatalf("Datum: %v expected type %T but was value %v of type %T", value, value, native, native)
	}

	// native -> textual -> native
	textual, err := codec.TextualFromNative(nil, value)
	if err != nil {
		t.Fatalf("Datum: %v; %s", value, err)
	}
	native, _, err = codec.NativeFromTextual(textual)
	if err != nil {
		t.Fatalf("Datum: %s; %s", textual, err)
	}
	if reflect.TypeOf(value) != reflect.TypeOf(native) {
		t.Fatalf("Datum: %v expected type %T but was value %v of type %T", value, value, native, native)
	}
}

func TestPrimitiveRecoverInt(t *testing.T) {
	testPrimitiveRecoverNative(t, `"int"`, int32(1010))
}

func TestPrimitiveRecoverLong(t *testing.T) {
	testPrimitiveRecoverNative(t, `"long"`, int64(8128953))
}

func TestPrimitiveRecoverFloat(t *testing.T) {
	testPrimitiveRecoverNative(t, `"float"`, float32(-8.937134))
}

func TestPrimitiveRecoverDouble(t *testing.T) {
	testPrimitiveRecoverNative(t, `"double"`, float64(5.247290238727473))
}
