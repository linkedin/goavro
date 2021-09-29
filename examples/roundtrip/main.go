package main

import (
	"bufio"
	bin "encoding/binary"
	hex "encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/linkedin/goavro/v2"
)

// roundtrip is a tool for checking avro
//
// incoming data is assumed to be standard json
// incoming json is required to be one json object per line
// use `jq -c .` if you need to.  get it into one line
//
// you can write out your avro in binary form and stop there
// which is useful for cases where you might want to send it off into other tools
//
// you can also do a roundtrip of decode/encode
// which allows you to see if your avro schema matches your expectations
//
// If you want to use an encoded schemaid then specify a schemid with -sid
// it will be encoded per a common standard (one null byte, 16 bytes of schemaid)
// Its NOT the standard SOE
// SOE should be added
// Probably OCF should be added too
//
// EXAMPLE
//
// kubectl get events -w -o json | jq -c .  | ./roundtrip -sid aa6b1ca0e1ee2d885bfbc747f4a4011b -avsc event-schema.json ) -rt

func MakeAvroHeader(schemaid string) (header []byte, err error) {
	dst, err := hex.DecodeString(schemaid)
	if err != nil {
		return
	}
	header = append(header, byte(0))
	header = append(header, dst...)
	return
}
func main() {

	var avsc = flag.String("avsc", "", "the avro schema")
	var data = flag.String("data", "-", "(default stdin) the data that corresponds to the avro schema or error - ONE LINE PER DATA ITEM")
	var schemaid = flag.String("sid", "", "the schemaid which is normally the md5hash of rht schema itself")
	var roundtrip = flag.Bool("rt", false, "do full round trip to try to rebuild the original data string")
	var xxd = flag.String("bin", "", "write out the binary data to this file - look at it with xxd if you want to")
	var appendBin = flag.Bool("append", false, "append to the output binary file instead of trunc")

	flag.Parse()

	_avsc, err := ioutil.ReadFile(*avsc)
	if err != nil {
		panic(fmt.Sprintf("Failed to read avsc file:%s:error:%v:", *avsc, err))
	}

	codec, err := goavro.NewCodecForStandardJSON(string(_avsc))
	if err != nil {
		panic(err)
	}

	var _data io.Reader
	if *data == "-" {
		_data = os.Stdin
	} else {
		file, err := os.Open(*data)
		if err != nil {
			panic(fmt.Sprintf("Failed to open data file:%s:error:%v:", *data, err))
		}
		_data = bufio.NewReader(file)
		defer file.Close()
	}

	binOut := struct {
		file *os.File
		do   bool
	}{}
	if len(*xxd) > 0 {
		bits := os.O_WRONLY | os.O_CREATE
		if *appendBin {
			bits |= os.O_APPEND
		} else {
			bits |= os.O_TRUNC
		}

		binOut.file, err = os.OpenFile(*xxd, bits, 0600)
		if err != nil {
			panic(err)
		}
		defer binOut.file.Close()
		binOut.do = true
	}

	scanner := bufio.NewScanner(_data)

	for scanner.Scan() {

		dat := scanner.Text()
		if len(dat) == 0 {
			fmt.Println("skipping empty line")
			continue
		}

		fmt.Println("RT in")
		fmt.Println(dat)

		textual := []byte(dat)

		fmt.Printf("encoding for schemaid:%s:\n", *schemaid)
		avroNative, _, err := codec.NativeFromTextual(textual)

		if err != nil {
			fmt.Println(dat)
			panic(err)
		}

		header, err := MakeAvroHeader(*schemaid)
		if err != nil {
			fmt.Println(string(textual))
			panic(err)
		}

		avrobin, err := codec.BinaryFromNative(nil, avroNative)
		if err != nil {
			fmt.Println(dat)
			panic(err)
		}

		// trying to minimize operations within the loop
		// so do only a quick boolean check here
		if binOut.do {
			for _, buf := range [][]byte{header, avrobin} {
				err = bin.Write(binOut.file, bin.LittleEndian, buf)
				if err != nil {
					fmt.Println(dat)
					panic(err)
				}
			}
		}

		if *roundtrip {
			// this will scramble the order
			// since it makes new go maps
			// when it takes the binary into native
			rtnativeval, _, err := codec.NativeFromBinary(avrobin)
			if err != nil {
				fmt.Println(dat)
				panic(err)
			}

			// Convert native Go form to textual Avro data
			textual, err = codec.TextualFromNative(nil, rtnativeval)
			if err != nil {
				fmt.Println(dat)
				panic(err)
			}

			fmt.Println("RT out")
			fmt.Println(string(textual))
		}

	}
	if err := scanner.Err(); err != nil {
		fmt.Println("scanner error")
		panic(err)
	}

	fmt.Println("Done with loop - no more data")

}
