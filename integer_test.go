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
	"testing"
)

func TestSchemaPrimitiveCodecInt(t *testing.T) {
	testSchemaPrimativeCodec(t, "int")
}

func TestPrimitiveIntBinary(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, "int", "some string")
	testBinaryDecodeFailShortBuffer(t, "int", []byte{0xfd, 0xff, 0xff, 0xff})
	testBinaryCodecPass(t, "int", -1, []byte{0x01})
	testBinaryCodecPass(t, "int", -2147483647, []byte{0xfd, 0xff, 0xff, 0xff, 0xf})
	testBinaryCodecPass(t, "int", -3, []byte{0x05})
	testBinaryCodecPass(t, "int", -65, []byte("\x81\x01"))
	testBinaryCodecPass(t, "int", 0, []byte{0x00})
	testBinaryCodecPass(t, "int", 1, []byte{0x02})
	testBinaryCodecPass(t, "int", 1016, []byte("\xf0\x0f"))
	testBinaryCodecPass(t, "int", 1455301406, []byte{0xbc, 0x8c, 0xf1, 0xeb, 0xa})
	testBinaryCodecPass(t, "int", 2147483647, []byte{0xfe, 0xff, 0xff, 0xff, 0xf})
	testBinaryCodecPass(t, "int", 3, []byte("\x06"))
	testBinaryCodecPass(t, "int", 64, []byte("\x80\x01"))
	testBinaryCodecPass(t, "int", 66052, []byte("\x88\x88\x08"))
	testBinaryCodecPass(t, "int", 8454660, []byte("\x88\x88\x88\x08"))
}

func TestPrimitiveIntText(t *testing.T) {
	testTextDecodeFailShortBuffer(t, "int", []byte(""))
	testTextDecodeFailShortBuffer(t, "int", []byte("-"))

	testTextCodecPass(t, "int", -13, []byte("-13"))
	testTextCodecPass(t, "int", 0, []byte("0"))
	testTextCodecPass(t, "int", 13, []byte("13"))
	testTextDecodePass(t, "int", -0, []byte("-0"))
	testTextEncodePass(t, "int", -0, []byte("0")) // NOTE: -0 encodes as "0"
}

func TestSchemaPrimitiveCodecLong(t *testing.T) {
	testSchemaPrimativeCodec(t, "long")
}

func TestPrimitiveLongBinary(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, "long", "some string")
	testBinaryDecodeFailShortBuffer(t, "long", []byte("\xff\xff\xff\xff"))
	testBinaryCodecPass(t, "long", (1<<63)-1, []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1})
	testBinaryCodecPass(t, "long", -(1 << 63), []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1})
	testBinaryCodecPass(t, "long", -2147483648, []byte("\xff\xff\xff\xff\x0f"))
	testBinaryCodecPass(t, "long", -3, []byte("\x05"))
	testBinaryCodecPass(t, "long", -65, []byte("\x81\x01"))
	testBinaryCodecPass(t, "long", 0, []byte("\x00"))
	testBinaryCodecPass(t, "long", 1082196484, []byte("\x88\x88\x88\x88\x08"))
	testBinaryCodecPass(t, "long", 1359702038045356208, []byte{0xe0, 0xc2, 0x8b, 0xa1, 0x96, 0xf3, 0xd0, 0xde, 0x25})
	testBinaryCodecPass(t, "long", 138521149956, []byte("\x88\x88\x88\x88\x88\x08"))
	testBinaryCodecPass(t, "long", 17730707194372, []byte("\x88\x88\x88\x88\x88\x88\x08"))
	testBinaryCodecPass(t, "long", 2147483647, []byte("\xfe\xff\xff\xff\x0f"))
	testBinaryCodecPass(t, "long", 2269530520879620, []byte("\x88\x88\x88\x88\x88\x88\x88\x08"))
	testBinaryCodecPass(t, "long", 3, []byte("\x06"))
	testBinaryCodecPass(t, "long", 5959107741628848600, []byte{0xb0, 0xe7, 0x8a, 0xe1, 0xe2, 0xba, 0x80, 0xb3, 0xa5, 0x1})
	testBinaryCodecPass(t, "long", 64, []byte("\x80\x01"))

	// https://github.com/linkedin/goavro/issues/49
	testBinaryCodecPass(t, "long", -5513458701470791632, []byte("\x9f\xdf\x9f\x8f\xc7\xde\xde\x83\x99\x01"))
}

func TestPrimitiveLongText(t *testing.T) {
	testTextDecodeFailShortBuffer(t, "long", []byte(""))
	testTextDecodeFailShortBuffer(t, "long", []byte("-"))

	testTextCodecPass(t, "long", -13, []byte("-13"))
	testTextCodecPass(t, "long", 0, []byte("0"))
	testTextCodecPass(t, "long", 13, []byte("13"))
	testTextDecodePass(t, "long", -0, []byte("-0"))
	testTextEncodePass(t, "long", -0, []byte("0")) // NOTE: -0 encodes as "0"
}
