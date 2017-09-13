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
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func benchmarkLowAndHigh(b *testing.B, callback func()) {
	// Run test case in parallel at relative low concurrency
	b.Run("Low", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				callback()
			}
		})
	})

	// Run test case in parallel at relative high concurrency
	b.Run("High", func(b *testing.B) {
		concurrency := runtime.NumCPU() * 1000
		wg := new(sync.WaitGroup)
		wg.Add(concurrency)
		b.ResetTimer()

		for c := 0; c < concurrency; c++ {
			go func() {
				defer wg.Done()

				for n := 0; n < b.N; n++ {
					callback()
				}
			}()
		}

		wg.Wait()
	})
}

// ensure code under test returns error containing specified string
func ensureError(tb testing.TB, err error, contains ...string) {
	if err == nil {
		tb.Errorf("Actual: %v; Expected: %#v", err, contains)
		return
	}
	for _, stub := range contains {
		if !strings.Contains(err.Error(), stub) {
			tb.Errorf("Actual: %v; Expected: %#v", err, contains)
		}
	}
}

// ShortWriter returns a structure that wraps an io.Writer, but returns
// io.ErrShortWrite when the number of bytes to write exceeds a preset limit.
//
// Copied with author's permission from https://github.com/karrick/gorill.
//
//   bb := NopCloseBuffer()
//   sw := ShortWriter(bb, 16)
//
//   n, err := sw.Write([]byte("short write"))
//   // n == 11, err == nil
//
//   n, err := sw.Write([]byte("a somewhat longer write"))
//   // n == 16, err == io.ErrShortWrite
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
