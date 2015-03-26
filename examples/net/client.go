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
	"log"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		log.Fatal(err)
	}
	fr, err := goavro.NewReader(goavro.FromReader(conn))
	if err != nil {
		log.Fatal("cannot create Reader: ", err)
	}
	defer func() {
		if err := fr.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	for fr.Scan() {
		datum, err := fr.Read()
		if err != nil {
			log.Println("cannot read datum: ", err)
			continue
		}
		fmt.Println("RECORD: ", datum)
	}
}
