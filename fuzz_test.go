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
	"bytes"
	"strings"
	"testing"
)

func TestCrashers_OCFReader(t *testing.T) {
	var crashers = map[string]string{
		"scan: negative block sizes": "Obj\x01\x04\x16avro.schema\x96\x05{" +
			"\"type\":\"record\",\"nam" +
			"e\":\"c0000000\",\"00000" +
			"0000\":\"00000000000\"," +
			"\"fields\":[{\"name\":\"u" +
			"0000000\",\"type\":\"str" +
			"ing\",\"000\":\"00000000" +
			"0000\"},{\"name\":\"c000" +
			"000\",\"type\":\"string\"" +
			",\"000\":\"000000000000" +
			"00000000000000000000" +
			"0\"},{\"name\":\"t000000" +
			"00\",\"type\":\"long\",\"0" +
			"00\":\"000000000000000" +
			"0000000000000000\"}]," +
			"\"0000\":\"000000000000" +
			"00000000000000000000" +
			"00000000\"}\x14000000000" +
			"0\b0000\x000000000000000" +
			"0000\xd90",
	}

	for testName, f := range crashers {
		t.Logf("Testing: %s", testName)
		_, _ = NewOCFReader(strings.NewReader(f)) // looking for panic rather than an error
	}
}

func TestCrashers_OCF_e2e(t *testing.T) {
	var crashers = map[string]string{
		"map: initialSize overflow": "Obj\x01\x04\x14avro.codec\bnul" +
			"l\x16avro.schema\xa2\x0e{\"typ" +
			"e\":\"record\",\"name\":\"" +
			"test_schema\",\"fields" +
			"\":[{\"name\":\"string\"," +
			"\"type\":\"string\",\"doc" +
			"\":\"Meaningless strin" +
			"g of characters\"},{\"" +
			"name\":\"simple_map\",\"" +
			"type\":{\"type\":\"map\"," +
			"\"values\":\"int\"}},{\"n" +
			"ame\":\"complex_map\",\"" +
			"type\":{\"type\":\"map\"," +
			"\"values\":{\"type\":\"ma" +
			"p\",\"values\":\"string\"" +
			"}}},{\"name\":\"union_s" +
			"tring_null\",\"type\":[" +
			"\"null\",\"string\"]},{\"" +
			"name\":\"union_int_lon" +
			"g_null\",\"type\":[\"int" +
			"\",\"long\",\"null\"]},{\"" +
			"name\":\"union_float_d" +
			"ouble\",\"type\":[\"floa" +
			"t\",\"double\"]},{\"name" +
			"\":\"fixed3\",\"type\":{\"" +
			"type\":\"fixed\",\"name\"" +
			":\"fixed3\",\"size\":3}}" +
			",{\"name\":\"fixed2\",\"t" +
			"ype\":{\"type\":\"fixed\"" +
			",\"name\":\"fixed2\",\"si" +
			"ze\":2}},{\"name\":\"enu" +
			"m\",\"type\":{\"type\":\"e" +
			"num\",\"name\":\"Suit\",\"" +
			"symbols\":[\"SPADES\",\"" +
			"HEARTS\",\"DIAMONDS\",\"" +
			"CLUBS\"]}},{\"name\":\"r" +
			"ecord\",\"type\":{\"type" +
			"\":\"record\",\"name\":\"r" +
			"ecord\",\"fields\":[{\"n" +
			"ame\":\"value_field\",\"" +
			"type\":\"string\"}],\"al" +
			"iases\":[\"Reco\x9adAlias" +
			"\"]}},{\"name\":\"array_" +
			"of_boolean\",\"type\":{" +
			"\"type\":\"array\",\"item" +
			"s\":\"boolean\"}},{\"nam" +
			"e\":\"bytes\",\"type\":\"b" +
			"ytes\"}]}\x00\xfeJ\x17\u007f\xb4r\x11\x0e\x96&\x0e" +
			"\xda<\xed\x86\xf6\x06\xfa\x05(OMG SPARK I" +
			"S AWESOME\x04\x06abc\x02\x06bcd\x0e" +
			"\x00\x02\x06key\x03\x80\x00\x02d\x02a\x02b\x00\x00\x01\x00\x00" +
			"\x00\x00\x00\x04\x00\xdb\x0fI@\x02\x03\x04\x11\x12\x00\xb6\x01Two" +
			" things are infinite" +
			": the universe and h" +
			"uman stupidity; and " +
			"I'm not sure about " +
			"universe.\x06\x01\x00\x00\x00\x06ABCT\x00e" +
			"rran is IMBA!\x04\x06qqq\x84\x01" +
			"\x06mmm\x00\x00\x02\x06key\x04\x023\x024\x021\x02K" +
			"��~\x02\x84\x01\x02`\xaa\xaa\xaa\xaa\xaa\x1a@\a" +
			"\a\a\x01\x02\x06\x9e\x01Life did no\xef\xbf" +
			"\xbd\ttend to make us pe" +
			"rfect. Whoever is pe" +
			"rfect `elongs in a m" +
			"useum.\x00\x00$The cake is" +
			" a LIE!\x00\x02\x06key\x00\x00\x00\x04\x02\x00\x00" +
			"\x00\x00\x00\x00\x00\x00\x11\"\t\x10\x90\x04\x16TEST_ST" +
			"R123\x00\x04\x00\x02S\xfeJ\x17\u007f\xb4r\x11\x0e\x96&\x0e" +
			"\xda<\xed\x86\xf6",
		"map: initialSize overflow-2": "Obj\x01\xff\xff\xff\xff\xff\xff\xff\xff\xff\x010",
		"array: initialSize overflow": "Obj\x01\x04\x14avro.codec\bnul" +
			"l\x16avro.schema\xa2\x0e{\"typ" +
			"e\":\"record\",\"name\":\"" +
			"test_schema\",\"fields" +
			"\":[{\"name\":\"string\"," +
			"\"type\":\"string\",\"doc" +
			"\":\"Meaningless strin" +
			"g of characters\"},{\"" +
			"name\":\"simple_map\",\"" +
			"type\":{\"type\":\"map\"," +
			"\"values\":\"int\"}},{\"n" +
			"ame\":\"complex_map\",\"" +
			"type\":{\"type\":\"map\"," +
			"\"values\":{\"type\":\"ma" +
			"p\",\"values\":\"string\"" +
			"}}},{\"name\":\"union_s" +
			"tring_null\",\"type\":[" +
			"\"null\",\"string\"]},{\"" +
			"name\":\"union_int_lon" +
			"g_null\",\"type\":[\"int" +
			"\",\"long\",\"null\"]},{\"" +
			"name\":\"union_float_d" +
			"ouble\",\"type\":[\"floa" +
			"t\",\"double\"]},{\"name" +
			"\":\"fixed3\",\"type\":{\"" +
			"type\":\"fixed\",\"name\"" +
			":\"fixed3\",\"size\":3}}" +
			",{\"name\":\"fixed2\",\"t" +
			"ype\":{\"type\":\"fixed\"" +
			",\"name\":\"fixed2\",\"si" +
			"ze\":2}},{\"name\":\"enu" +
			"m\",\"type\":{\"type\":\"e" +
			"num\",\"name\":\"Suit\",\"" +
			"symbols\":[\"SPADES\",\"" +
			"HEARTS\",\"DIAMONDS\",\"" +
			"CLUBS\"]}},{\"name\":\"r" +
			"ecord\",\"type\":{\"type" +
			"\":\"record\",\"name\":\"r" +
			"ecord\",\"fields\":[{\"n" +
			"ame\":\"value_field\",\"" +
			"type\":\"string\"}],\"al" +
			"iases\":[\"Reco\x9adAlias" +
			"\"]}},{\"name\":\"array_" +
			"of_boolean\",\"type\":{" +
			"\"type\":\"array\",\"item" +
			"s\":\"boolean\"}},{\"nam" +
			"e\":\"bytes\",\"type\":\"b" +
			"ytes\"}]}\x00\xfeJ\x17\u007f\xb4r\x11\x0e\x96&\x0e" +
			"\xda<\xed\x86\xf6\x06\xfa\x05(OMG SPARK I" +
			"S AWESOME\x04\x06abc\x02\x06bcd\x0e" +
			"\x00\x02\x06key\x03\x80\x00\x02d\x02a\x02b\x00\x00\x01\x00\x00" +
			"\x00\x00\x00\x04\x00\xdb\x0fI@\x02\x03\x04\x11\x12\x00\xb6\x01Two" +
			" things are infinite" +
			": the universe and h" +
			"uman stupidity; and " +
			"I'm not sure about u" +
			"n������\xef" +
			"\xbf\xbd�is IMBA!\x04\x06qqq\x84\x01" +
			"\x06mmm\x00\x00\x02\x06key\x04\x023\x024\x021\x022" +
			"\x00\x00\x02\x06123\x02\x84\x01\x02`\xaa\xaa\xaa\xaa\xaa\x1a@\a" +
			"\a\a\x01\x02\x06\x9e\x01Life did no\xef\xbf" +
			"\xbd\ttend to make us pe" +
			"rfect. Whoever is pe" +
			"rfect `elongs in a m" +
			"useum.\x00\x00$The cake is" +
			" a LIE!\x00\x02\x06key\x00\x00\x00\x04\x02\x00\x00" +
			"\x00\x00\x00\x00\x00\x00\x11\"\t\x10\x90\x04\x16TEST_ST" +
			"R123\x00\x04\x00\x02S\xfeJ\x17\u007f\xb4r\x11\x0e\x96&\x0e" +
			"\xda<\xed\x86\xf6",
		"scan: blockSize overflow": "Obj\x01\x04\x14avro.codec\fsna" +
			"ppy\x16avro.schema\xf2\x05{\"t" +
			"ype\":\"record\",\"name\"" +
			":\"twitter_schema\",\"n" +
			"amespace\":\"com.migun" +
			"o.avro\",\"fields\":[{\"" +
			"name\":\"username\",\"ty" +
			"pe\":\"string\",\"doc\":\"" +
			"Name of the user acc" +
			"ount on Twitter.com\"" +
			"},{\"name\":\"tweet\",\"t" +
			"ype\":\"string\",\"doc\":" +
			"\"The content of the " +
			"user's Twitter messa" +
			"ge\"},{\"name\":\"timest" +
			"amp\",\"type\":\"long\",\"" +
			"doc\":\"Unix epoch tim" +
			"e in milliseconds\"}]" +
			",\"doc:\":\"A basic sch" +
			"ema for storing Twit" +
			"ter messages\"}\x00.\xe2\xf3\xee\x96" +
			"\nw2\xc3*5\\\x951\xa4\xae~\xa2\x8f\xdc\xf8\xa3H",
		"fixed: size conversion: positive float64 -> negative int": "Obj\x01\x04\x14avro.codec\x0edef" +
			"late\x16avro.schema\xa2\x0e{\"" +
			"type\":\"record\",\"name" +
			"\":\"test_schema\",\"fie" +
			"lds\":[{\"name\":\"strin" +
			"g\",\"type\":\"string\",\"" +
			"doc\":\"Meaningless st" +
			"ring of characters\"}" +
			",{\"name\":\"simple_map" +
			"\",\"type\":{\"type\":\"ma" +
			"p\",\"values\":\"int\"}}," +
			"{\"name\":\"complex_map" +
			"\",\"type\":{\"type\":\"ma" +
			"p\",\"values\":{\"type\":" +
			"\"map\",\"values\":\"stri" +
			"ng\"}}},{\"name\":\"unio" +
			"n_string_null\",\"type" +
			"\":[\"null\",\"string\"]}" +
			",{\"name\":\"union_int_" +
			"long_null\",\"type\":[\"" +
			"int\",\"long\",\"null\"]}" +
			",{\"name\":\"union_floa" +
			"t_double\",\"type\":[\"f" +
			"loat\",\"double\"]},{\"n" +
			"ame\":\"fixed3\",\"type\"" +
			":{\"type\":\"fixed\",\"na" +
			"me\":\"fixed3\",\"size\":" +
			"3}},{\"name\":\"fixed2\"" +
			",\"type\":{\"type\":\"fix" +
			"ed\",\"name\":\"fixed2\"," +
			"\"size\":6938893903907" +
			"22837764769792556762" +
			"6953125,\"name\":\"Suit" +
			"\",\"symbols\":[\"SPADES" +
			"\",\"HEARTS\",\"DIAMONDS" +
			"\",\"CLUBS\"]}},{\"name\"" +
			":\"record\",\"type\":{\"t" +
			"ype\":\"record\",\"name\"" +
			":\"record\",\"fields\":[" +
			"{\"name\":\"value_field" +
			"\",\"type\":\"string\"}]," +
			"\"aliases\":[\"RecordAl" +
			"ias\"]}},{\"name\":\"arr" +
			"ay_of_boolean\",\"type" +
			"\":{\"type\":\"array\",\"i" +
			"tems\":\"boolean\"}},{\"" +
			"name\":\"bytes\",\"type\"" +
			":\"bytes\"}]}\x00\x90\xfb\x1eO%\x06%B" +
			"\x03\x00s\x0f(\x89\x02\a\x06\x82&\x1d\x97K\xa8\x9dg\x15\x86\xf7" +
			"9l\x8f\xc74\x84\x10B\x88\xa1\x04\x04\x11\x1d8\x14g\xa9\"8" +
			"R\xe7N\x84\xef~[\xdf\xfd>\xac\xa0\x93Nl5\x95B\xb5\xa8" +
			"\x88U\x10\x8a\x04\xa1H\xb5\xb6b\x89ZL\xa5\x88x\x03+T\x87" +
			"\x8a\x11\xec@p\x1d\x87g\xef\xff\xfc\xff\xf7\xaf\xf5\xbe\xcf\xfb\xee+" +
			"~4\x10#]:Z\xfa\xbd\xbbO\xdc=\xbb=\x1cp#\xa6\x02" +
			"\xae\xa73\xf0\xc2\xd3\x0f\xff\xf6\x9eۓ/jK\xe4\x1dx\xb5}" +
			"\xbe\xf1\xab?<x\xd75\xbb\xa5\x9e\x10\xf3\xbe\xf7\xe0?\xbfy\xe4" +
			"rW\xce\xd1\xf1γO\xff\xfa\x91å\xcbY\xc8U\xfc\xf1\x96" +
			"\x8da\xf5\x91\x8b\\\x12ҍH\xd2\xd4N\x93\x92\xe3\x15\xe5\x8cT" +
			"\xb4\x1cM9܀\xc6\x19\xd4\xd8ݠ\xa7\x97dK\xc0/>>" +
			"F{z[\xcdY\x14w$\xec \x17U\x97]\x16\xc5\xcf\xc3\r" +
			"_j\xdeV\x05\xd6\xe7\xd9ᛧk\x04\xaaX\xf76ڡ\f" +
			"\xaf1\x10N\x94\xe4֮E\x88Rswi\x19\x95s\x1a\x06\xa5" +
			"\xab\xb0{a$E]!\x15\xba<[\x9bQ\xed\xc6ܤ$\x11" +
			"\xd9\fi$\xcdx\x9c\xa6\x89-\xbc\xe1m\xa4\xdcz0\x91t%" +
			"\x8b\xd9C딽YMFݕ\xee\xcd\bG|\x9c\xccX\x1a" +
			"\xf2\xbaI\x197V\x96\xd8i\xcc\u007f<ѪLKt\xcf|\xd9" +
			"U\x05\xe9:I\xaf5\xad\xc6U!\x16Tּe\xab\x8b\xech" +
			"\x8c\xb4N\xbb\x82j\x9a\U000f16a2I\x98\x96U\xb1l\x8e\x81\xe3" +
			"\x91\xaa0\xba#\x03\xce\xe3.rf\xa8D\x12\br\xa4ڬ\xe5" +
			"\xf2\xe6\xdaմ\x99\xe2\xe8kݶ\xb9\x84\xb5ea>\x88\xd8\xe6" +
			"\xe1l\xf4tv%\x17\xef\xa8\xf6\xc7=\x0e\xd7,\xde\xd1\xe7\\\xd9" +
			"MS\r\x1f\xa9Ԩ\xb6?\xe6~8\x9c^\xd3\x1bD\xc2\xe1\xf2" +
			"ӧ\xfe\xf1\xc6+\xf7^{\xe5\xbb\xee\xf0\xf2/\x9f\xbb\xf3ٯ" +
			"\xfc\xe8\x1b\xe3\xecH\xec\xd9\xc9\xc9\xc9\xe1\xfc\v\xcf\u007f❣\xab" +
			"\xe7g\xf8\x90\xb7\xfe\xf5\xcf\xfb'7\xadć\xf6\xb4D\x9a\xe5\xd5" +
			"\xef\xdf\u007f\xf6\xe4F\xee\x13\xd4\x02\xb9\x9a\xff\xebK\u007fzx\xf9\n" +
			"d¶\xe5o\xff\xe4\x9e>\x9c\xdfҶw\xbb\xd4\xecc\xcc~" +
			"\xfc\xfdI\xa7\x15\x88\b\xd3\x105\x95\x94c\xaf\x0eL\xe8,\xea\"" +
			",\x87\x94V\x91\xbay\xb6\xb3\x8d\xb1z.Zu4\xb5\xc5}r" +
			"R\x93\xb1q>A\xdbA\xbdl(f\xd6w\xa9[T]\x92\x1d" +
			"\xa6\x8fX<\xb1Z҉\x1b\x1e\xeb\nU\x9b\xd74\xafֱD" +
			"H[\\\x87\xa0\x19\xdb;,w8\xed\x87\xf3\x046\x1c\x1fE)" +
			"\x051d\x11i\xd4(n\xf2\x94S\x18\x8aɲ\xf8uR\xacK" +
			"\x8c̦\x8eU\x1c.i\xbb\x05\x1cp\x84/\xbf\xf5\xbb\xfbw\xef" +
			"?x\xf3\xe1\xc9\xe1\xeb\x1f\xfb\xe0c\x8f}\xe0\xa3\x1foǫ|" +
			"\x1aU\xa9!\x97\x0e8\xbe\xc3\xe1\xfa\v/\xfe\xf6\x87\x9f\xb9\xf9\xe9" +
			"\xe7\xee\xfd<\x90\xdd͊R&C,\xeb$\x89\x11l\t*\xcf" +
			"\xeew\xad\xbcj\xa1k\xf7<\x13CV\x81\x14\x8bV@}?\u007f" +
			"\x14J\x90\xad\xb7>[\xb6:\xbe\xf8\xe4\xcf^?\xb9E)\x10t" +
			"\x8av[/\xf3\xfc\xab\x8f\xbft\xf2\xf9\x13[T\xe4\xa2*\x87j" +
			"\xcfd\x1b\xb0\x8d\x9a\xccRw~\x84\xcaJ\x94\x1dŶ)\x17\xd6" +
			"\xa0s\x84MΓ\x19\xba\xdc\xc3\xe5\xe0\n\xbc\xf9\xfa\x8f\xbf\x86\x1a" +
			"\x12B\xa1r\x9ey\xe6\xdf\u007f\u007f\xf7\xe1\xfcp\xbcbkk3\xaf" +
			"/\xbd\x8f\xe4\x1d3I}4\xcbJ\x92\xf8:\xccJ_@\xa9m" +
			"\xdcf\xe07\U000dc520/^ĵ\x9a\x80\f>\xebڭ\xf6" +
			"J\x18s{Q>\u009e=\x0e\x1dL%=\nt\xac\x97\xb2\xe3" +
			"\x1eX\x1a\xb2\xe2\xd7(\xef4\xc1\xe7\xe5\xe8f\x9b\xaf\x8a#\xc5]" +
			"\x90@\xf5,n[\x1aѶ%+\xbf\xc0̵\xe2\xacj\xd1T" +
			"\xb7\xe2\t\xa6\x8f\xb1\xc7\xdaFm\x8cWe2ߑ\xec\xca\xdb\xd4" +
			"\x8dТ&ϛ\xc6]\xd1\xd8\xd5\xe2\xc5\x1d5\x99\x03哢" +
			"\x8b\xa3\"\x12mhhp\xb2\xd5ac\xaa]\xc5\\\xecFY\xd2" +
			"\\\xf0DEr\x1c\xf8J*\x89\xac\x8c\xf3\xbeg\nx\xd6Zq" +
			"\xabi\x0f\xc9s!E\x90I\nE\xd5\xf7&\xa8\xb5\xa5\x00\x99\\" +
			"1$\x95\x03\xd9v\x0f\x99\xd20\x96\x1ec\xf5\xa9Bɣm\x8f" +
			"7\a\x03\\\t&ypF7i\xbc\v\xb58'i\xed\x14\x04" +
			"*S\xf3\xa9z\x8dMVE\x8av\"w^\x96\x12V\x80\x8a\x1e" +
			"\x8f\x88\xfa\xa5\xc8\xe1\xad\x13mr\x03\xcc\n\x13ᬤ\x8e\x85!" +
			"\r\r\x05\x96Z\xdbh\xb2MF\xd0[\x8fiM\xa1\x95(o\xd8" +
			"^\x8eۤ\xc4\xcaVHɍ\x12\x938\xbe\xa0k\xa9\x9a1\x11" +
			"'m\x9d\xa0F\x94\xe8XZ\xc98njj\xa5\xf5\x98\xe8\x14\xba" +
			"\x05\xa1UU\x11\x97l\x93&\xacY\xcc\x11B\xaa3y\xc1ސ" +
			"\x9bp\xa98\x1f\x17\xa2?\xf2\x9d\xcb.)$\x06D\x87\x8b\xa0\x01" +
			"ӕ\xb7\xd9L\x9fC\x17\xccM%\xd4\bLv\x1f\xad@\n\xe3" +
			"9\xb5\x1a\xb4)i\xf6\xa4\x90CF\xa0\"l\xc0\x94\x84\xd9f\x99" +
			"\xb2|l\v\x16e\x01Zp\xe2\x06\xe9aC\x15\xdeɴ\r\x89" +
			"k\xe1\x149\x84\x9e\xe5\xd0\xd4V\x88(ֆSǜ\xa0\x83\xa9" +
			"a0{X[qX\xcf\xdd\x14\t\x9d\a[N\xa16\xb50U" +
			"\xa9\xba\x06\x92\xb5+K\xcb\x0e\r\xff\x12\xa3\xc4U\xac\u0381\xa9D" +
			" \x1a\x87\xdc\xf7ɚ\xa0T/\xcb\bc\xccT\xcaY)-_" +
			"\xdbV\xbd\xfd\xb4i4\xbc\xd0\r\xbeZ@B\a\vqr\x89d" +
			"\xd1*64\xa6\x1cuՅ\xccJ\x1d\xa2\xb5\xb1F\xe7{\x8c\"" +
			"\x91\xec5\n\x88K\xa8nr\x9ey\xef\xbew\xb0\x9b\x05 \t\xdd" +
			"\xbcCA\x05\xa5\xac\x88\x02\xef!ɍ\x91\xb13\xa3\xae//B" +
			"\b\xac\xf2\xc4\xf2\xaeR($\x8d\x9c{T<\xa86\xcc\v@\x8f" +
			"\x06҅oy3\xd4\\7\xcaB\xb0\x9d\x13Fg\xf2\x937-" +
			"\x97\n\x12\x93\x105d\xdd\xee\"\x19@\x9aY\xbd2\x0e\xc0\xc4\xc5" +
			"\xa8\x0fzb^\x13L\u007f\f\x9f\xb6\xa8fjE\xe6\x02>\x8d\xd7" +
			"ͽ\xee.-\u0099Wc\x99\xa2\xd1*\x99\xb6Ѧ\xac$\x88" +
			"\x8a\x99T\x94\x99\x06P\xab\x05C\xc5X\xccJN\fzp\xa4\xc6" +
			"}'9\xf1\x1e\x97\xa2\xc2\x10\xe8\xbc\xf35\xf3\u0604s\x99b`" +
			"\x94+̧\xd8W\x11\xe8\xbe\x1dr\xa3\xca\v\xac:\x8393\x16" +
			"\x93\x11\x9cs\r\xf7ra\xe42GV\b\xcdRݖ\x11'\xe6" +
			"|\xb1F\x916\xa1\x10\x8f\xfb\xf7\xc9`5Bs\xb6=\xb7\x16A" +
			"RG\x84O\x1e!\xed\x12_\xcbi\x96UfX\xa4\xaa^\xd8Z" +
			"B\xe3\xdcAх3\x1a\xca\x0e\xae\x8d9\x18\x1e\x86)\xe1\x01\xab" +
			"\xc1,x\a+h\xa6\xb3S\x1d7\xbaN\x8d\vZ\x1a\x14A\xc5" +
			"q7\"\x91\xfa\x1b\xabJ\xc7\xf4\xe7\xd9\x06\x9f\x94\xf2\xd4Թ\xc4" +
			"\xb6\xab\x04\xac\x17\x02\xef\xb8\xc3\xde\x18\x90\xdbM\x17\xb3j\xa2\xba\xff" +
			"\v\x80\x8b8V\xa8\xce\xd5.\xd3D\x82\xf6b\x03\xfa\xa5\xc3d\xb3" +
			"\x106X\x91{\xbb\xac8~\xe0\x15\xce\x18\x10jDg\x9b*F" +
			"h\xc3V\xe1\t2pV\xba}߹F\x83%f\xba\xa5s\xea" +
			"8\xf9\xc58>\xb8l\xa60\x06\x88\x1eVC\xc5\xe640\xab\x98" +
			"\u09a9\x99\x90\x15\xa0\xb0y\x18ΆD\x1e\xf6\x94u\xda|N" +
			"<+\x86\x1et\xa0\x9a\xd4\xed\x05\xabsD\x14\x06%\x94v2\xbc" +
			"\xcf2g\xea\x16\x06(vK+pa\x9a\xed\xb6cپ4X" +
			"\xb8\xb8@ad&\x91?i1b\x86]\x94\xf5\xc0\x1a\xa9!-" +
			"\xb43\b\x94\x11\x84\vʹ\x8c\x95\r\xf3'\x99^\xb9\x1d\x80\xed" +
			"'\xcdޖ\a\x13/\x8a\x96p3\xa6\x84\xcd̊\x14\xb3\x17\xaa" +
			"ٞQ\xe5\x80U\xb0\xe6ū\xaf\r\x92\x95c\xb8с \xeb" +
			"\xe6t\xaeT\xc4DG\xc4\x12$\xe8T\x83\x10dt\xbe\xf8Ve" +
			"\xbai\b\x820\x83\x1d \x91\x80\xee\xc9\x16\xf914\n?\xe2\x00" +
			"\xb4\x1e\x18\xf3\x8eZ\x02t\xf8fUČ\xa1\xa0\x86\x94\x91\xb3\xcd" +
			"td\xb6.\x89\uf06e\x8e\x1a\x99\x17\xa8\x8c\xf8z8\x15_\x86" +
			"\xc0J ;.\r!1\xb0i\n\xe8yH\xf06Av\x9c\x02" +
			"*R\x00Ŕ\xf4d\xa3D\x9b\xf4 ;\x8a\x9c\xfb&Y\xa9\xa1" +
			"\xf8\xe00\x8a\xab\x05<\x9e\xc3\xe8\xc3y\xf2:5q\x80\xa46\v" +
			"\xb3\xd4U\n\xc9_\xb4`\xca45<:\xa7\xbd\xc5lĘ\x14" +
			"\x8ee\x0e\bn\x9c\x8c\xf4}HTD\n\xb8\xfaX\xf3(\x03\xcf" +
			"\xe5\xb2\xf5\xc3b1\x97\x805\xdd#([\xf0\f\x96&\xb8\x16$" +
			"J\xb4<\a\xf0\xe9\x1c<o7\xe2Ʒ\x8f\x1e\xcb<=\xdc\x06" +
			"\x8f\xf1և\x99\x93D\xb7\xf8\xf1\xdc\x15\x17nD=\x12\f\x8d\xa6" +
			"*\xd7\x10\xd6|\xd0\xdd\xd4U=\xe2\xc4\xda&\x0e\xd7:\xb7\xadZ" +
			"$\xec\xe1,P\x83\xa5\xec2\xfeN([\x9d~\xfb\xb5\xa7\xfe\xfb" +
			"\xd3\x1f\xfc\xf9\xf1O\x1e\xbe\xf8\x97'\xef|轟z\xff\x87\x8f" +
			"\xbf8\xdd\x18<\xa9,\xd6\xdc\x1a\xe4b钚ͻ\x94R\x83" +
			"\xeb\xa1\"\x1eI)X\x8d\xfbH\xab:\t\x8d.\\\xca@\xc7n" +
			"\xc4\x16\xee.\x10d\xf2\xe2\xb9\xe1eяDH*\x968o:" +
			"F\x83L*;\xbb2\xfew\xdb\x1a\x06\xc11/\x05\x13#?\xee" +
			"f\xc8\xf6\xc0\x06\x8cV\" }\xfa&\x14[qYX\xae\xeaP" +
			"\"\xa0 $\xf2\xe6\xec\xa27^\xfbܷ\xde\xfe\xc8\x13߹\xf3" +
			"\xd5/\xff\x0f\x90\xfb\x1eO%\x06%B\x03\x00s\x0f(\x89\x02\a",
	}

	for testName, f := range crashers {
		t.Logf("Testing: %s", testName)

		// TODO: replace this with a call out to the e2e Fuzz function
		ocfr, err := NewOCFReader(strings.NewReader(f))
		if err != nil {
			continue
		}

		var datums []interface{}
		for ocfr.Scan() {
			if datum, err := ocfr.Read(); err == nil {
				datums = append(datums, datum)
			}
		}

		b := new(bytes.Buffer)
		ocfw, err := NewOCFWriter(
			OCFConfig{
				W:      b,
				Schema: ocfr.Codec().Schema(),
			})
		if err != nil {
			panic(err)
		}
		if err := ocfw.Append(datums); err != nil {
			panic(err)
		}
	}
}
