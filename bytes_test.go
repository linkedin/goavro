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
	"encoding/json"
	"strings"
	"testing"
)

func TestSchemaPrimitiveCodecBytes(t *testing.T) {
	testSchemaPrimativeCodec(t, `"bytes"`)
}

func TestPrimitiveBytesBinary(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, `"bytes"`, 13)
	testBinaryDecodeFailShortBuffer(t, `"bytes"`, nil)
	testBinaryDecodeFailShortBuffer(t, `"bytes"`, []byte{2})
	testBinaryCodecPass(t, `"bytes"`, []byte(""), []byte("\x00"))
	testBinaryCodecPass(t, `"bytes"`, []byte("some bytes"), []byte("\x14some bytes"))
}

func TestPrimitiveBytesText(t *testing.T) {
	testTextEncodeFailBadDatumType(t, `"bytes"`, 42)
	testTextDecodeFailShortBuffer(t, `"bytes"`, []byte(``))
	testTextDecodeFailShortBuffer(t, `"bytes"`, []byte(`"`))
	testTextDecodeFail(t, `"bytes"`, []byte(`..`), "expected initial \"")
	testTextDecodeFail(t, `"bytes"`, []byte(`".`), "expected final \"")

	testTextCodecPass(t, `"bytes"`, []byte(""), []byte("\"\""))
	testTextCodecPass(t, `"bytes"`, []byte("a"), []byte("\"a\""))
	testTextCodecPass(t, `"bytes"`, []byte("ab"), []byte("\"ab\""))
	testTextCodecPass(t, `"bytes"`, []byte("a\"b"), []byte("\"a\\\"b\""))
	testTextCodecPass(t, `"bytes"`, []byte("a\\b"), []byte("\"a\\\\b\""))
	testTextCodecPass(t, `"bytes"`, []byte("a/b"), []byte("\"a\\/b\""))

	testTextCodecPass(t, `"bytes"`, []byte("a\bb"), []byte(`"a\bb"`))
	testTextCodecPass(t, `"bytes"`, []byte("a\fb"), []byte(`"a\fb"`))
	testTextCodecPass(t, `"bytes"`, []byte("a\nb"), []byte(`"a\nb"`))
	testTextCodecPass(t, `"bytes"`, []byte("a\rb"), []byte(`"a\rb"`))
	testTextCodecPass(t, `"bytes"`, []byte("a\tb"), []byte(`"a\tb"`))
	testTextCodecPass(t, `"bytes"`, []byte("a	b"), []byte(`"a\tb"`)) // tab byte between a and b

	testTextDecodeFail(t, `"bytes"`, []byte("\"\\u\""), "short buffer")
	testTextDecodeFail(t, `"bytes"`, []byte("\"\\u.\""), "short buffer")
	testTextDecodeFail(t, `"bytes"`, []byte("\"\\u..\""), "short buffer")
	testTextDecodeFail(t, `"bytes"`, []byte("\"\\u...\""), "short buffer")

	testTextDecodeFail(t, `"bytes"`, []byte("\"\\u////\""), "invalid byte") // < '0'
	testTextDecodeFail(t, `"bytes"`, []byte("\"\\u::::\""), "invalid byte") // > '9'
	testTextDecodeFail(t, `"bytes"`, []byte("\"\\u@@@@\""), "invalid byte") // < 'A'
	testTextDecodeFail(t, `"bytes"`, []byte("\"\\uGGGG\""), "invalid byte") // > 'F'
	testTextDecodeFail(t, `"bytes"`, []byte("\"\\u````\""), "invalid byte") // < 'a'
	testTextDecodeFail(t, `"bytes"`, []byte("\"\\ugggg\""), "invalid byte") // > 'f'

	testTextCodecPass(t, `"bytes"`, []byte("⌘ "), []byte("\"\\u0001\\u00E2\\u008C\\u0098 \""))
	testTextCodecPass(t, `"bytes"`, []byte("😂"), []byte(`"\u00F0\u009F\u0098\u0082"`))
}

func TestSchemaPrimitiveStringCodec(t *testing.T) {
	testSchemaPrimativeCodec(t, `"string"`)
}

func TestPrimitiveStringBinary(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, `"string"`, 42)
	testBinaryDecodeFailShortBuffer(t, `"string"`, nil)
	testBinaryDecodeFailShortBuffer(t, `"string"`, []byte{2})
	testBinaryCodecPass(t, `"string"`, "", []byte("\x00"))
	testBinaryCodecPass(t, `"string"`, "some string", []byte("\x16some string"))
}

func TestPrimitiveStringText(t *testing.T) {
	testTextEncodeFailBadDatumType(t, `"string"`, 42)
	testTextDecodeFailShortBuffer(t, `"string"`, []byte(``))
	testTextDecodeFailShortBuffer(t, `"string"`, []byte(`"`))
	testTextDecodeFail(t, `"string"`, []byte(`..`), "expected initial \"")
	testTextDecodeFail(t, `"string"`, []byte(`".`), "expected final \"")

	testTextCodecPass(t, `"string"`, "", []byte("\"\""))
	testTextCodecPass(t, `"string"`, "a", []byte("\"a\""))
	testTextCodecPass(t, `"string"`, "ab", []byte("\"ab\""))
	testTextCodecPass(t, `"string"`, "a\"b", []byte("\"a\\\"b\""))
	testTextCodecPass(t, `"string"`, "a\\b", []byte("\"a\\\\b\""))
	testTextCodecPass(t, `"string"`, "a/b", []byte("\"a\\/b\""))

	testTextCodecPass(t, `"string"`, "a\bb", []byte(`"a\bb"`))
	testTextCodecPass(t, `"string"`, "a\fb", []byte(`"a\fb"`))
	testTextCodecPass(t, `"string"`, "a\nb", []byte(`"a\nb"`))
	testTextCodecPass(t, `"string"`, "a\rb", []byte(`"a\rb"`))
	testTextCodecPass(t, `"string"`, "a\tb", []byte(`"a\tb"`))
	testTextCodecPass(t, `"string"`, "a	b", []byte(`"a\tb"`)) // tab byte between a and b

	testTextDecodeFail(t, `"string"`, []byte("\"\\u\""), "short buffer")
	testTextDecodeFail(t, `"string"`, []byte("\"\\u.\""), "short buffer")
	testTextDecodeFail(t, `"string"`, []byte("\"\\u..\""), "short buffer")
	testTextDecodeFail(t, `"string"`, []byte("\"\\u...\""), "short buffer")

	testTextDecodeFail(t, `"string"`, []byte("\"\\u////\""), "invalid byte") // < '0'
	testTextDecodeFail(t, `"string"`, []byte("\"\\u::::\""), "invalid byte") // > '9'
	testTextDecodeFail(t, `"string"`, []byte("\"\\u@@@@\""), "invalid byte") // < 'A'
	testTextDecodeFail(t, `"string"`, []byte("\"\\uGGGG\""), "invalid byte") // > 'F'
	testTextDecodeFail(t, `"string"`, []byte("\"\\u````\""), "invalid byte") // < 'a'
	testTextDecodeFail(t, `"string"`, []byte("\"\\ugggg\""), "invalid byte") // > 'f'

	testTextCodecPass(t, `"string"`, "⌘ ", []byte("\"\\u0001\\u2318 \""))
	testTextCodecPass(t, `"string"`, "😂 ", []byte("\"\\u0001\\uD83D\\uDE02 \""))

	testTextDecodeFail(t, `"string"`, []byte("\"\\"), "short buffer")
	testTextDecodeFail(t, `"string"`, []byte("\"\\uD83D\""), "surrogate pair")
	testTextDecodeFail(t, `"string"`, []byte("\"\\uD83D\\u\""), "surrogate pair")
	testTextDecodeFail(t, `"string"`, []byte("\"\\uD83D\\uD\""), "surrogate pair")
	testTextDecodeFail(t, `"string"`, []byte("\"\\uD83D\\uDE\""), "surrogate pair")
	testTextDecodeFail(t, `"string"`, []byte("\"\\uD83D\\uDE0\""), "invalid byte")
}

func TestUnescapeUnicode(t *testing.T) {
	checkGood := func(t *testing.T, argument, want string) {
		got, err := unescapeUnicodeString(argument)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("GOT: %q; WANT: %q", got, want)
		}
	}

	checkBad := func(t *testing.T, argument, want string) {
		_, got := unescapeUnicodeString(argument)
		if got == nil || !strings.Contains(got.Error(), want) {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	}

	checkBad(t, "\\u0000", "short buffer")
	checkBad(t, "\\uinvalid", "invalid byte")
	checkBad(t, "\\ud83d\\ude0", "missing second half of surrogate pair")
	checkBad(t, "\\ud83d\\uinvalid", "invalid byte")
	checkBad(t, "\\", "short buffer")
	checkGood(t, "", "")
	checkGood(t, "\\\\", "\\")
	checkGood(t, "\u0041\u0062\u0063", "Abc")
	checkGood(t, "\u0001\\uD83D\\uDE02 ", "😂 ")
	checkGood(t, "Hello, \u0022World!\"", "Hello, \"World!\"")
	checkGood(t, "\u263a\ufe0f", "☺️")
	checkGood(t, "\u65e5\u672c\u8a9e", "日本語")
}

func TestJSONUnmarshalStrings(t *testing.T) {
	cases := []struct {
		arg  string
		want string
	}{
		{arg: `"A1"`, want: "A1"},
		{arg: `"\u0042\u0032"`, want: "B2"}, // backslashes have no meaning in back-tick string constant
	}

	for _, c := range cases {
		var raw interface{}
		if err := json.Unmarshal([]byte(c.arg), &raw); err != nil {
			t.Errorf("CASE: %s; ERROR: %s", c.arg, err)
			return
		}
		got, ok := raw.(string)
		if !ok {
			t.Errorf("CASE: %s; GOT: %T; WANT: string", c.arg, got)
			return
		}
		if got != c.want {
			t.Errorf("GOT: %s; WANT: %q", got, c.want)
		}
	}
}
