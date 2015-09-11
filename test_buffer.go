// Copyright 2015 LinkedIn Corp. Licensed under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License.  You may obtain a copy of the License
// at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.Copyright [201X] LinkedIn Corp. Licensed under the Apache
// License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.

package goavro

import (
	"bytes"
	"io"
)

/* For testing purposes */
type TestBuffer interface {
	io.Writer
	io.Reader
	Bytes() []byte
}

/* A byte buffer for testing that fulfills io.Writer, but can't
   be upcast to ByteWriter or StringWriter */
type SimpleBuffer struct {
	buf bytes.Buffer
}

func (self *SimpleBuffer) Write(b []byte) (n int, err error) {
	return self.buf.Write(b)
}

func (self *SimpleBuffer) Bytes() []byte {
	return self.buf.Bytes()
}

func (self *SimpleBuffer) Read(p []byte) (n int, err error) {
	return self.buf.Read(p)
}
