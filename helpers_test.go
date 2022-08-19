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
)

// ShortWriter returns a structure that wraps an io.Writer, but returns
// io.ErrShortWrite when the number of bytes to write exceeds a preset limit.
//
// Copied with author's permission from https://github.com/karrick/gorill.
//
//	bb := NopCloseBuffer()
//	sw := ShortWriter(bb, 16)
//
//	n, err := sw.Write([]byte("short write"))
//	// n == 11, err == nil
//
//	n, err := sw.Write([]byte("a somewhat longer write"))
//	// n == 16, err == io.ErrShortWrite
func ShortWriter(w io.Writer, max int) io.Writer {
	return shortWriter{Writer: w, max: max}
}

func (s shortWriter) Write(data []byte) (int, error) {
	var short bool
	index := len(data)
	if index > s.max {
		index = s.max
		short = true
	}
	n, err := s.Writer.Write(data[:index])
	if short {
		return n, io.ErrShortWrite
	}
	return n, err
}

type shortWriter struct {
	io.Writer
	max int
}
