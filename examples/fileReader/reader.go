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

package main

import (
	"fmt"
	"github.com/linkedin/goavro"
	"io"
	"log"
	"os"
)

func main() {
	if len(os.Args) > 1 {
		for i, arg := range os.Args {
			if i == 0 {
				continue
			}
			fh, err := os.Open(arg)
			if err != nil {
				log.Fatal(err)
			}
			dumpReader(fh)
			fh.Close()
		}
	} else {
		dumpReader(os.Stdin)
	}
}

func dumpReader(r io.Reader) {
	fr, err := goavro.NewReader(goavro.BufferFromReader(r))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := fr.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	for fr.Scan() {
		datum, err := fr.Read()
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println(datum)
	}
}
