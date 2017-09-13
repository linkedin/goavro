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
	testBinaryEncodeFail(t, `{"type":"enum","name":"e1","symbols":["alpha","bravo"]}`, 13, `cannot encode binary enum "e1": expected string; received: int`)
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
