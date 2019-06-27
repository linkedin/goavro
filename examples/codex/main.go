package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/linkedin/goavro"
)

var progname = filepath.Base(os.Args[0])

func main() {
	if err := cmdMain(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s.\n", progname, err)
		os.Exit(1)
	}
}

func cmdMain() error {
	flag.Parse()
	if flag.NArg() == 0 {
		return errors.New("which files?")
	}

	codex := goavro.NewCodex()

	for _, arg := range flag.Args() {
		if err := decodeFromFile(arg, codex); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s.\n", progname, arg, err)
		}
	}
	return nil
}

func decodeFromFile(pathname string, codex *goavro.Codex) error {
	fh, err := os.Open(flag.Args()[0])
	if err != nil {
		return err
	}
	err = decodeFromReader(fh, codex)
	if err2 := fh.Close(); err == nil {
		err = err2
	}
	return err
}

func decodeFromReader(r io.Reader, codex *goavro.Codex) error {
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	fingerprint, err := goavro.FingerprintFromSOE(buf)
	if err != nil {
		return err
	}

	codec, ok := codex.Load(fingerprint)
	if !ok {
		codec, err = codecFromFingerprint(fingerprint)
		if err != nil {
			return err
		}
		codex.Store(codec)
	}

	datum, _, err := codec.NativeFromSingle(buf)
	if err != nil {
		return err
	}
	_, err = fmt.Printf("%#v\n", datum)
	return err
}

func codecFromFingerprint(fingerprint uint64) (*goavro.Codec, error) {
	schema, err := schemaFromFingerprint(fingerprint)
	if err != nil {
		return nil, err
	}
	return goavro.NewCodec(string(schema))
}

// schemaFromFingerprint calls a fictitious web service that returns response
// bodies that match the specified schema fingerprint.
func schemaFromFingerprint(fingerprint uint64) ([]byte, error) {
	client := &http.Client{Timeout: time.Duration(3 * time.Second)}
	response, err := client.Get("http://schema-registry.example.com/schemas/rabin=" + strconv.FormatUint(fingerprint, 10))
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		var message string
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			message = fmt.Sprintf(": cannot read response body: %s", err)
		} else if len(body) > 0 {
			message = ": " + string(body)
		}
		return nil, fmt.Errorf("schema registry status code: %d%s", response.StatusCode, message)
	}

	return ioutil.ReadAll(response.Body)
}
