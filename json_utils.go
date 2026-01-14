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
	"io"
)

// skipJSONValue advances the buffer past a single JSON value (object, array, string, number, bool, null).
// This is used when IgnoreExtraFieldsFromTextual is enabled to skip over values of unknown fields.
func skipJSONValue(buf []byte) ([]byte, error) {
	if buf, _ = advanceToNonWhitespace(buf); len(buf) == 0 {
		return nil, io.ErrShortBuffer
	}

	switch buf[0] {
	case '{':
		// Skip object: find matching closing brace
		return skipJSONObject(buf)
	case '[':
		// Skip array: find matching closing bracket
		return skipJSONArray(buf)
	case '"':
		// Skip string: find closing quote (handling escapes)
		return skipJSONString(buf)
	case 't':
		// true
		if len(buf) >= 4 && string(buf[:4]) == "true" {
			return buf[4:], nil
		}
		return nil, fmt.Errorf("cannot skip JSON value: invalid literal starting with 't'")
	case 'f':
		// false
		if len(buf) >= 5 && string(buf[:5]) == "false" {
			return buf[5:], nil
		}
		return nil, fmt.Errorf("cannot skip JSON value: invalid literal starting with 'f'")
	case 'n':
		// null
		if len(buf) >= 4 && string(buf[:4]) == "null" {
			return buf[4:], nil
		}
		return nil, fmt.Errorf("cannot skip JSON value: invalid literal starting with 'n'")
	default:
		// Must be a number (or invalid)
		if buf[0] == '-' || (buf[0] >= '0' && buf[0] <= '9') {
			return skipJSONNumber(buf)
		}
		return nil, fmt.Errorf("cannot skip JSON value: unexpected character: %q", buf[0])
	}
}

// skipJSONObject skips a JSON object starting with '{' and returns the buffer after the closing '}'.
func skipJSONObject(buf []byte) ([]byte, error) {
	if len(buf) == 0 || buf[0] != '{' {
		return nil, fmt.Errorf("cannot skip JSON object: expected '{'")
	}
	buf = buf[1:] // consume '{'
	var err error

	if buf, _ = advanceToNonWhitespace(buf); len(buf) == 0 {
		return nil, io.ErrShortBuffer
	}

	// Handle empty object
	if buf[0] == '}' {
		return buf[1:], nil
	}

	for len(buf) > 0 {
		// Skip key (string)
		if buf, err = skipJSONString(buf); err != nil {
			return nil, err
		}
		// Skip colon
		if buf, err = advanceAndConsume(buf, ':'); err != nil {
			return nil, err
		}
		// Skip value
		if buf, err = skipJSONValue(buf); err != nil {
			return nil, err
		}
		// Check for comma or closing brace
		if buf, _ = advanceToNonWhitespace(buf); len(buf) == 0 {
			return nil, io.ErrShortBuffer
		}
		switch buf[0] {
		case '}':
			return buf[1:], nil
		case ',':
			buf = buf[1:]
			if buf, _ = advanceToNonWhitespace(buf); len(buf) == 0 {
				return nil, io.ErrShortBuffer
			}
		default:
			return nil, fmt.Errorf("cannot skip JSON object: expected ',' or '}'; received: %q", buf[0])
		}
	}
	return nil, io.ErrShortBuffer
}

// skipJSONArray skips a JSON array starting with '[' and returns the buffer after the closing ']'.
func skipJSONArray(buf []byte) ([]byte, error) {
	if len(buf) == 0 || buf[0] != '[' {
		return nil, fmt.Errorf("cannot skip JSON array: expected '['")
	}
	buf = buf[1:] // consume '['
	var err error

	if buf, _ = advanceToNonWhitespace(buf); len(buf) == 0 {
		return nil, io.ErrShortBuffer
	}

	// Handle empty array
	if buf[0] == ']' {
		return buf[1:], nil
	}

	for len(buf) > 0 {
		// Skip value
		if buf, err = skipJSONValue(buf); err != nil {
			return nil, err
		}
		// Check for comma or closing bracket
		if buf, _ = advanceToNonWhitespace(buf); len(buf) == 0 {
			return nil, io.ErrShortBuffer
		}
		switch buf[0] {
		case ']':
			return buf[1:], nil
		case ',':
			buf = buf[1:]
			if buf, _ = advanceToNonWhitespace(buf); len(buf) == 0 {
				return nil, io.ErrShortBuffer
			}
		default:
			return nil, fmt.Errorf("cannot skip JSON array: expected ',' or ']'; received: %q", buf[0])
		}
	}
	return nil, io.ErrShortBuffer
}

// skipJSONString skips a JSON string starting with '"' and returns the buffer after the closing '"'.
func skipJSONString(buf []byte) ([]byte, error) {
	if buf, _ = advanceToNonWhitespace(buf); len(buf) == 0 {
		return nil, io.ErrShortBuffer
	}
	if buf[0] != '"' {
		return nil, fmt.Errorf("cannot skip JSON string: expected '\"'")
	}
	buf = buf[1:] // consume opening quote

	for i := 0; i < len(buf); i++ {
		switch buf[i] {
		case '\\':
			// Skip the next character (escaped)
			i++
		case '"':
			// Found closing quote
			return buf[i+1:], nil
		}
	}
	return nil, io.ErrShortBuffer
}

// skipJSONNumber skips a JSON number and returns the buffer after the number.
func skipJSONNumber(buf []byte) ([]byte, error) {
	if len(buf) == 0 {
		return nil, io.ErrShortBuffer
	}

	i := 0

	// Optional minus sign
	if buf[i] == '-' {
		i++
		if i >= len(buf) {
			return nil, io.ErrShortBuffer
		}
	}

	// Integer part
	switch {
	case buf[i] == '0':
		i++
	case buf[i] >= '1' && buf[i] <= '9':
		for i < len(buf) && buf[i] >= '0' && buf[i] <= '9' {
			i++
		}
	default:
		return nil, fmt.Errorf("cannot skip JSON number: invalid character: %q", buf[i])
	}

	// Optional fractional part
	if i < len(buf) && buf[i] == '.' {
		i++
		if i >= len(buf) || buf[i] < '0' || buf[i] > '9' {
			return nil, fmt.Errorf("cannot skip JSON number: expected digit after decimal point")
		}
		for i < len(buf) && buf[i] >= '0' && buf[i] <= '9' {
			i++
		}
	}

	// Optional exponent part
	if i < len(buf) && (buf[i] == 'e' || buf[i] == 'E') {
		i++
		if i >= len(buf) {
			return nil, io.ErrShortBuffer
		}
		if buf[i] == '+' || buf[i] == '-' {
			i++
			if i >= len(buf) {
				return nil, io.ErrShortBuffer
			}
		}
		if buf[i] < '0' || buf[i] > '9' {
			return nil, fmt.Errorf("cannot skip JSON number: expected digit in exponent")
		}
		for i < len(buf) && buf[i] >= '0' && buf[i] <= '9' {
			i++
		}
	}

	return buf[i:], nil
}
