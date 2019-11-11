package goavro

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
)

func codecFromFile(schemaPath string) (*Codec, error) {
	schemaBinary, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("could not read schema binary: %s", err.Error())
	}

	codec, err := NewCodec(string(schemaBinary))
	if err != nil {
		return nil, fmt.Errorf("could not create avro schema: %s", err.Error())
	}

	return codec, nil
}

func ValidateJSONFromFile(dataPath, schemaPath string) (bool, string, error) {
	codec, err := codecFromFile(schemaPath)
	if err != nil {
		return false, "", err
	}

	textual, err := ioutil.ReadFile(dataPath)
	if err != nil {
		return false, "", fmt.Errorf("could not read from data path: %s", err.Error())
	}

	_, _, err = codec.NativeFromTextual(textual)
	if err != nil {
		return false, err.Error(), nil
	}

	return true, "", nil
}

func ValidateAVROFromFile(dataPath, schemaPath string) (bool, string, error) {
	codec, err := codecFromFile(schemaPath)
	if err != nil {
		return false, "", err
	}

	fh, err := os.Open(dataPath)
	if err != nil {
		return false, "", fmt.Errorf("could not open data path: %s", err.Error())
	}
	defer fh.Close()

	ocf, err := NewOCFReader(bufio.NewReader(fh))
	if err != nil {
		return false, "", fmt.Errorf("could not create oc reader for data file: %s", err.Error())
	}

	for ocf.Scan() {
		datum, err := ocf.Read()
		if err != nil {
			return false, "", fmt.Errorf("could not read from the data file: %s", err.Error())
		}
		_, err = codec.TextualFromNative(nil, datum)
		if err != nil {
			return false, err.Error(), nil
		}
	}

	if ocf.Err() != nil {
		return false, ocf.Err().Error(), nil
	}

	return true, "", nil
}
