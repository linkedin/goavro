package goavro

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func testFuzz(crasher string) error {
	fr, err := NewReader(FromReader(strings.NewReader(crasher)))
	if err != nil {
		return err
	}

	var datums []interface{}

	for fr.Scan() {
		datum, err := fr.Read()
		if err != nil {
			return err
		}
		datums = append(datums, datum)
	}

	codec, err := NewCodec(fr.DataSchema)
	if err != nil {
		panic(err)
	}

	for _, datum := range datums {
		bb := new(bytes.Buffer)
		err := codec.Encode(bb, datum)
		if err != nil {
			panic(fmt.Sprintf("%+v :: %s", datum, err))
		}
	}

	return nil
}

/*
// This is where we put anything that just caused a panic and wasn't solved by returning an error
func TestFuzz_Panics(t *testing.T) {
	var crashers = []string{
		"Obj\x01\x04\x14avro.codec\fsna" +
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
			"amp\",\"type\":\"null\",\"" +
			"doc\":\"Unix epoch tim" +
			"e in milliseconds\"}]" +
			",\"doc:\":\"A basic sch" +
			"ema for storing Twit" +
			"ter messages\"}\x005\\\x951\xa4" +
			"\xae~\xa2\x8f\xdc\xf8\xa3H\x87\x83\x80\x04\xd6\x01d\xf0c\fmi" +
			"gunoFRock: Nerf pape" +
			"r, scissors is fine." +
			"\xb2\xb8\xee\x96\n\x14BlizzardCSFWor" +
			"ks as intended.  Ter" +
			"ran is IMBA.\xe2\xf3\xee\x96\n",
	}

	for n, data := range crashers {
		if err := testFuzz(data); err != nil {
			t.Errorf("Error returned during fuzzing crasher[%v]: %v\n", n, err)
		}
	}
}
*/

func TestFuzz_UnboundedAllocation(t *testing.T) {
	var crashers = []string{
		"Obj\x01\x04\x14avro.codec\fsna" +
			"ppy\x16avro.schema\xf2\x05{\"t" +
			"ype\":\"record\",\"name\"" +
			":\"twitter_schema\",\"t" +
			"ype\":\"com.miguno.avr" +
			"o\",\"fields\":[{\"name\"" +
			":\"username\",\"type\":\"" +
			"string\",\"doc\":\"Name " +
			"of the user account " +
			"on Twitter.com\"},{\"n" +
			"ame\":\"tweet\",\"type\":" +
			"\"string\",\"doc\":\"The " +
			"content of the user'" +
			"s Twitter message\"}," +
			"{\"name\":\"timestamp\"," +
			"\"type\":\"long\",\"doc\":" +
			"\"Unix epoch time in " +
			"milliseconds\"}],\"doc" +
			":\":\"A basic schema f" +
			"or storing Twitter m" +
			"essages\"}\x0003\x951\xa4\xae~\xa2\x8f\xdc" +
			"\xf8\xa3H\x87\x83\x80\x04\xd6\x01d\xf0c\fmigunoF" +
			"Rock: Nerf paper, sc" +
			"issors is fine.\xb2\xb8\xee\x96\n" +
			"\x14BlizzardCSFseconds\"" +
			"}]Works as intended." +
			"  Terran is IM",
		"Obj\x010\xa2\x8f\xdc\xf8\xa30",
		"Obj\x010ƕ�\b",
	}

	for n, data := range crashers {
		if err := testFuzz(data); err != nil {
			if strings.Contains(err.Error(), "greater than the max currently set with MaxDecodeSize") {
				// Then we're handling the error properly
				continue
			}

			t.Errorf("Error returned during fuzzing crasher[%v]: %v\n", n, err)
		}
	}
}
