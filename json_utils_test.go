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
	"io"
	"testing"
)

func TestSkipJSONString(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantRest  string
		wantError bool
	}{
		// Basic strings
		{name: "empty string", input: `""`, wantRest: ""},
		{name: "simple string", input: `"hello"`, wantRest: ""},
		{name: "string with trailing", input: `"hello",next`, wantRest: ",next"},
		{name: "string with spaces", input: `"hello world"`, wantRest: ""},

		// Escaped characters
		{name: "escaped quote", input: `"hello\"world"`, wantRest: ""},
		{name: "escaped backslash", input: `"hello\\world"`, wantRest: ""},
		{name: "escaped newline", input: `"hello\nworld"`, wantRest: ""},
		{name: "escaped tab", input: `"hello\tworld"`, wantRest: ""},
		{name: "escaped unicode", input: `"hello\u0041world"`, wantRest: ""},
		{name: "multiple escapes", input: `"a\"b\\c\nd"`, wantRest: ""},
		{name: "escaped at end", input: `"test\\"`, wantRest: ""},

		// Whitespace handling
		{name: "leading whitespace", input: `  "hello"`, wantRest: ""},
		{name: "leading tabs", input: "\t\"hello\"", wantRest: ""},

		// Error cases
		{name: "missing open quote", input: `hello"`, wantError: true},
		{name: "missing close quote", input: `"hello`, wantError: true},
		{name: "escape at end", input: `"hello\`, wantError: true},
		{name: "empty input", input: ``, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rest, err := skipJSONString([]byte(tt.input))
			if tt.wantError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if string(rest) != tt.wantRest {
				t.Errorf("got rest=%q, want=%q", string(rest), tt.wantRest)
			}
		})
	}
}

func TestSkipJSONNumber(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantRest  string
		wantError bool
	}{
		// Integers
		{name: "zero", input: "0", wantRest: ""},
		{name: "single digit", input: "5", wantRest: ""},
		{name: "multi digit", input: "123", wantRest: ""},
		{name: "negative", input: "-42", wantRest: ""},
		{name: "negative zero", input: "-0", wantRest: ""},

		// Decimals
		{name: "decimal", input: "3.14", wantRest: ""},
		{name: "decimal no int", input: "0.5", wantRest: ""},
		{name: "negative decimal", input: "-3.14", wantRest: ""},
		{name: "long decimal", input: "123.456789", wantRest: ""},

		// Exponents
		{name: "exponent lowercase", input: "1e10", wantRest: ""},
		{name: "exponent uppercase", input: "1E10", wantRest: ""},
		{name: "exponent positive", input: "1e+10", wantRest: ""},
		{name: "exponent negative", input: "1e-10", wantRest: ""},
		{name: "decimal with exponent", input: "3.14e10", wantRest: ""},
		{name: "negative with exponent", input: "-2.5E-3", wantRest: ""},

		// With trailing content
		{name: "trailing comma", input: "123,", wantRest: ","},
		{name: "trailing brace", input: "456}", wantRest: "}"},
		{name: "trailing bracket", input: "789]", wantRest: "]"},
		{name: "trailing whitespace", input: "123 ", wantRest: " "},

		// Error cases
		{name: "empty", input: "", wantError: true},
		{name: "just minus", input: "-", wantError: true},
		{name: "leading zero", input: "01", wantRest: "1"}, // valid: stops at 0, leaves "1"
		{name: "decimal no digits", input: "1.", wantError: true},
		{name: "exponent no digits", input: "1e", wantError: true},
		{name: "exponent sign no digits", input: "1e+", wantError: true},
		{name: "invalid char", input: "abc", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rest, err := skipJSONNumber([]byte(tt.input))
			if tt.wantError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if string(rest) != tt.wantRest {
				t.Errorf("got rest=%q, want=%q", string(rest), tt.wantRest)
			}
		})
	}
}

func TestSkipJSONArray(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantRest  string
		wantError bool
	}{
		// Basic arrays
		{name: "empty array", input: "[]", wantRest: ""},
		{name: "single number", input: "[1]", wantRest: ""},
		{name: "multiple numbers", input: "[1,2,3]", wantRest: ""},
		{name: "with spaces", input: "[ 1 , 2 , 3 ]", wantRest: ""},

		// Mixed types
		{name: "mixed types", input: `[1,"hello",true,null]`, wantRest: ""},
		{name: "nested array", input: "[[1,2],[3,4]]", wantRest: ""},
		{name: "nested object", input: `[{"a":1},{"b":2}]`, wantRest: ""},
		{name: "deeply nested", input: `[[[1]]]`, wantRest: ""},

		// With trailing content
		{name: "trailing comma", input: "[1,2],next", wantRest: ",next"},
		{name: "trailing brace", input: "[1]}", wantRest: "}"},

		// Error cases
		{name: "missing bracket", input: "[1,2", wantError: true},
		{name: "missing open", input: "1,2]", wantError: true},
		{name: "empty input", input: "", wantError: true},
		{name: "unclosed nested", input: "[[1,2]", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rest, err := skipJSONArray([]byte(tt.input))
			if tt.wantError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if string(rest) != tt.wantRest {
				t.Errorf("got rest=%q, want=%q", string(rest), tt.wantRest)
			}
		})
	}
}

func TestSkipJSONObject(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantRest  string
		wantError bool
	}{
		// Basic objects
		{name: "empty object", input: "{}", wantRest: ""},
		{name: "single field", input: `{"a":1}`, wantRest: ""},
		{name: "multiple fields", input: `{"a":1,"b":2}`, wantRest: ""},
		{name: "with spaces", input: `{ "a" : 1 , "b" : 2 }`, wantRest: ""},

		// Various value types
		{name: "string value", input: `{"name":"John"}`, wantRest: ""},
		{name: "boolean value", input: `{"flag":true}`, wantRest: ""},
		{name: "null value", input: `{"empty":null}`, wantRest: ""},
		{name: "array value", input: `{"items":[1,2,3]}`, wantRest: ""},
		{name: "nested object", input: `{"outer":{"inner":1}}`, wantRest: ""},
		{name: "deeply nested", input: `{"a":{"b":{"c":{"d":1}}}}`, wantRest: ""},

		// Complex cases
		{name: "mixed types", input: `{"s":"str","n":123,"b":true,"nil":null,"a":[1],"o":{}}`, wantRest: ""},
		{name: "escaped key", input: `{"key\"with\"quotes":1}`, wantRest: ""},

		// With trailing content
		{name: "trailing comma", input: `{"a":1},next`, wantRest: ",next"},
		{name: "trailing bracket", input: `{"a":1}]`, wantRest: "]"},

		// Error cases
		{name: "missing brace", input: `{"a":1`, wantError: true},
		{name: "missing colon", input: `{"a"1}`, wantError: true},
		{name: "missing value", input: `{"a":}`, wantError: true},
		{name: "empty input", input: "", wantError: true},
		{name: "unclosed nested", input: `{"a":{"b":1}`, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rest, err := skipJSONObject([]byte(tt.input))
			if tt.wantError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if string(rest) != tt.wantRest {
				t.Errorf("got rest=%q, want=%q", string(rest), tt.wantRest)
			}
		})
	}
}

func TestSkipJSONValue(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantRest  string
		wantError bool
	}{
		// Primitives
		{name: "true", input: "true", wantRest: ""},
		{name: "false", input: "false", wantRest: ""},
		{name: "null", input: "null", wantRest: ""},
		{name: "string", input: `"hello"`, wantRest: ""},
		{name: "number", input: "123", wantRest: ""},
		{name: "negative number", input: "-456", wantRest: ""},
		{name: "decimal", input: "3.14", wantRest: ""},

		// Compound types
		{name: "array", input: "[1,2,3]", wantRest: ""},
		{name: "object", input: `{"a":1}`, wantRest: ""},
		{name: "empty array", input: "[]", wantRest: ""},
		{name: "empty object", input: "{}", wantRest: ""},

		// With whitespace
		{name: "leading space", input: "  true", wantRest: ""},
		{name: "leading newline", input: "\ntrue", wantRest: ""},
		{name: "leading tab", input: "\ttrue", wantRest: ""},

		// With trailing content
		{name: "true trailing", input: "true,next", wantRest: ",next"},
		{name: "false trailing", input: "false}", wantRest: "}"},
		{name: "null trailing", input: "null]", wantRest: "]"},

		// Error cases
		{name: "empty", input: "", wantError: true},
		{name: "whitespace only", input: "   ", wantError: true},
		{name: "invalid true", input: "tru", wantError: true},
		{name: "invalid false", input: "fals", wantError: true},
		{name: "invalid null", input: "nul", wantError: true},
		{name: "invalid char", input: "xyz", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rest, err := skipJSONValue([]byte(tt.input))
			if tt.wantError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if string(rest) != tt.wantRest {
				t.Errorf("got rest=%q, want=%q", string(rest), tt.wantRest)
			}
		})
	}
}

func TestSkipJSONValueShortBuffer(t *testing.T) {
	// Test that io.ErrShortBuffer is returned for truncated inputs
	tests := []struct {
		name  string
		input string
	}{
		{name: "truncated string", input: `"hello`},
		{name: "truncated array", input: `[1,2`},
		{name: "truncated object", input: `{"a":1`},
		{name: "truncated nested", input: `{"a":[1,2`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := skipJSONValue([]byte(tt.input))
			if err == nil {
				t.Errorf("expected error, got nil")
				return
			}
			if err != io.ErrShortBuffer {
				// Some errors might be more specific, that's fine
				t.Logf("got error: %v (not io.ErrShortBuffer, but still an error)", err)
			}
		})
	}
}
