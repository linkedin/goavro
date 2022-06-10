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
	"encoding/json"
	"fmt"
	"testing"
)

func TestSchemaEnum(t *testing.T) {
	testSchemaValid(t, `{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`)
}

func TestEnumName(t *testing.T) {
	testSchemaInvalid(t, `{"type":"enum","symbols":["alpha","bravo"]}`, "Enum ought to have valid name: schema ought to have name key")
	testSchemaInvalid(t, `{"type":"enum","name":3}`, "Enum ought to have valid name: schema name ought to be non-empty string")
	testSchemaInvalid(t, `{"type":"enum","name":""}`, "Enum ought to have valid name: schema name ought to be non-empty string")
	testSchemaInvalid(t, `{"type":"enum","name":"&foo","symbols":["alpha","bravo"]}`, "Enum ought to have valid name: schema name ought to start with")
	testSchemaInvalid(t, `{"type":"enum","name":"foo&","symbols":["alpha","bravo"]}`, "Enum ought to have valid name: schema name ought to have second and remaining")
}

func TestEnumSymbols(t *testing.T) {
	testSchemaInvalid(t, `{"type":"enum","name":"e1"}`, `Enum "e1" ought to have symbols key`)
	testSchemaInvalid(t, `{"type":"enum","name":"e1","symbols":3}`, `Enum "e1" symbols ought to be non-empty array of strings`)
	testSchemaInvalid(t, `{"type":"enum","name":"e1","symbols":[]}`, `Enum "e1" symbols ought to be non-empty array of strings`)
}

func TestEnumSymbolInvalid(t *testing.T) {
	testSchemaInvalid(t, `{"type":"enum","name":"e1","symbols":[3]}`, `Enum "e1" symbol 1 ought to be non-empty string`)
	testSchemaInvalid(t, `{"type":"enum","name":"e1","symbols":[""]}`, `Enum "e1" symbol 1 ought to be non-empty string`)
	testSchemaInvalid(t, `{"type":"enum","name":"e1","symbols":["string-with-invalid-characters"]}`, `Enum "e1" symbol 1 ought to have second and remaining`)
}

func TestEnumDecodeError(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"enum","name":"e1","symbols":["alpha","bravo"]}`, nil, "short buffer")
	testBinaryDecodeFail(t, `{"type":"enum","name":"e1","symbols":["alpha","bravo"]}`, []byte("\x01"), `cannot decode binary enum "e1": index ought to be between 0 and 1`)
	testBinaryDecodeFail(t, `{"type":"enum","name":"e1","symbols":["alpha","bravo"]}`, []byte("\x04"), `cannot decode binary enum "e1": index ought to be between 0 and 1`)
}

func TestEnumEncodeError(t *testing.T) {
	testBinaryEncodeFail(t, `{"type":"enum","name":"e1","symbols":["alpha","bravo"]}`, 13, `cannot encode binary enum "e1": expected string or a type that implements avroEnum received: int`)
	testBinaryEncodeFail(t, `{"type":"enum","name":"e1","symbols":["alpha","bravo"]}`, "charlie", `cannot encode binary enum "e1": value ought to be member of symbols`)
}

func TestEnumEncode(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"enum","name":"e1","symbols":["alpha","bravo"]}`, "alpha", []byte("\x00"))
	testBinaryCodecPass(t, `{"type":"enum","name":"e1","symbols":["alpha","bravo"]}`, "bravo", []byte("\x02"))
}

func TestEnumTextCodec(t *testing.T) {
	testTextCodecPass(t, `{"type":"enum","name":"e1","symbols":["alpha","bravo"]}`, "alpha", []byte(`"alpha"`))
	testTextCodecPass(t, `{"type":"enum","name":"e1","symbols":["alpha","bravo"]}`, "bravo", []byte(`"bravo"`))
	testTextEncodeFail(t, `{"type":"enum","name":"e1","symbols":["alpha","bravo"]}`, "charlie", `cannot encode textual enum "e1": value ought to be member of symbols`)
	testTextDecodeFail(t, `{"type":"enum","name":"e1","symbols":["alpha","bravo"]}`, []byte(`"charlie"`), `cannot decode textual enum "e1": value ought to be member of symbols`)
}

func TestGH233(t *testing.T) {
	// here's the fail case
	// testTextCodecPass(t, `{"type":"record","name":"FooBar","namespace":"com.foo.bar","fields":[{"name":"event","type":["null",{"type":"enum","name":"FooBarEvent","symbols":["CREATED","UPDATED"]}]}]}`, map[string]interface{}{"event": Union("FooBarEvent", "CREATED")}, []byte(`{"event":{"FooBarEvent":"CREATED"}}`))
	// remove the namespace and it passes
	testTextCodecPass(t, `{"type":"record","name":"FooBar","fields":[{"name":"event","type":["null",{"type":"enum","name":"FooBarEvent","symbols":["CREATED","UPDATED"]}]}]}`, map[string]interface{}{"event": Union("FooBarEvent", "CREATED")}, []byte(`{"event":{"FooBarEvent":"CREATED"}}`))
	// experiments
	// the basic enum
	testTextCodecPass(t, `{"type":"enum","name":"FooBarEvent","symbols":["CREATED","UPDATED"]}`, "CREATED", []byte(`"CREATED"`))
	// the basic enum with namespace
	testTextCodecPass(t, `{"type":"enum","name":"FooBarEvent","namespace":"com.foo.bar","symbols":["CREATED","UPDATED"]}`, "CREATED", []byte(`"CREATED"`))
	// union with enum
	testTextCodecPass(t, `["null",{"type":"enum","name":"FooBarEvent","symbols":["CREATED","UPDATED"]}]`, Union("FooBarEvent", "CREATED"), []byte(`{"FooBarEvent":"CREATED"}`))
	// FAIL: union with enum with namespace: cannot determine codec: "FooBarEvent"
	// testTextCodecPass(t, `["null",{"type":"enum","name":"FooBarEvent","namespace":"com.foo.bar","symbols":["CREATED","UPDATED"]}]`, Union("FooBarEvent", "CREATED"), []byte(`{"FooBarEvent":"CREATED"}`))
	// conclusion, union is not handling namespaces correctly
	// try union with record instead of enum (records and enums both have namespaces)
	// get a basic record going
	testTextCodecPass(t, `{"type":"record","name":"LongList","fields":[{"name":"next","type":["null","LongList"],"default":null}]}`, map[string]interface{}{"next": Union("LongList", map[string]interface{}{"next": nil})}, []byte(`{"next":{"LongList":{"next":null}}}`))
	// add a namespace to the record
	// fails in the same way cannot determine codec: "LongList" for key: "next"
	// testTextCodecPass(t, `{"type":"record","name":"LongList","namespace":"com.foo.bar","fields":[{"name":"next","type":["null","LongList"],"default":null}]}`, map[string]interface{}{"next": Union("LongList", map[string]interface{}{"next": nil})}, []byte(`{"next":{"LongList":{"next":null}}}`))
	//
	// experiments on syntax solutions
	// testTextCodecPass(t, `["null",{"type":"enum","name":"com.foo.bar.FooBarEvent","symbols":["CREATED","UPDATED"]}]`, Union("com.foo.bar.FooBarEvent", "CREATED"), []byte(`{"FooBarEvent":"CREATED"}`))
	// thie TestUnionMapRecordFitsInRecord tests binary from Native, but not native from textual
	// that's where the error is happening
	// if the namespace is specified in the incoming name it works
	testTextCodecPass(t, `{"type":"record","name":"ns1.LongList","fields":[{"name":"next","type":["null","LongList"],"default":null}]}`, map[string]interface{}{"next": Union("ns1.LongList", map[string]interface{}{"next": nil})}, []byte(`{"next":{"ns1.LongList":{"next":null}}}`))

	// try the failcase with the namespace specified on the input
	testTextCodecPass(t, `{"type":"record","name":"FooBar","namespace":"com.foo.bar","fields":[{"name":"event","type":["null",{"type":"enum","name":"FooBarEvent","symbols":["CREATED","UPDATED"]}]}]}`, map[string]interface{}{"event": Union("com.foo.bar.FooBarEvent", "CREATED")}, []byte(`{"event":{"com.foo.bar.FooBarEvent":"CREATED"}}`))

}

func ExampleCheckSolutionGH233() {
	const avroSchema = `
	{
		"type": "record",
		"name": "FooBar",
		"namespace": "com.foo.bar",
		"fields": [
		  {
				"name": "event",
				"type": [
					"null",
					{
						"type": "enum",
						"name": "FooBarEvent",
						"symbols": ["CREATED", "UPDATED"]
					}
				]
			}
		]
	}
	`
	codec, _ := NewCodec(avroSchema)

	const avroJson = `{"event":{"com.foo.bar.FooBarEvent":"CREATED"}}`

	native, _, err := codec.NativeFromTextual([]byte(avroJson))
	if err != nil {
		panic(err)
	}

	blob, err := json.Marshal(native)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(blob))
	// Output: {"event":{"com.foo.bar.FooBarEvent":"CREATED"}}

}
