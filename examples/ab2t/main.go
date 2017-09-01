package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/linkedin/goavro"
)

func usage() {
	executable, err := os.Executable()
	if err != nil {
		executable = os.Args[0]
	}
	base := filepath.Base(executable)
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", base)
	fmt.Fprintf(os.Stderr, "\t%s [file1.avro [file2.avro [file3.avro]]]\n", base)
	fmt.Fprintf(os.Stderr, "\tWhen filename is hyphen, %s will read from its standard input.\n", base)
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
	}
	for _, arg := range args {
		if arg == "-" {
			stat, err := os.Stdin.Stat()
			if err != nil {
				bail(err)
			}
			if (stat.Mode() & os.ModeCharDevice) != 0 {
				usage()
			}
			if err = dumpFromReader(os.Stdin); err != nil {
				bail(err)
			}
			if err = os.Stdin.Close(); err != nil {
				bail(err)
			}
			continue
		}
		fh, err := os.Open(arg)
		if err != nil {
			bail(err)
		}
		if err := dumpFromReader(bufio.NewReader(fh)); err != nil {
			bail(err)
		}
		if err := fh.Close(); err != nil {
			bail(err)
		}
	}
}

func dumpFromReader(ior io.Reader) error {
	ocf, err := goavro.NewOCFReader(ior)
	if err != nil {
		return err
	}

	codec := ocf.Codec()
	data := make(chan interface{}, 100)
	finishedOutput := new(sync.WaitGroup)
	finishedOutput.Add(1)

	go textualFromNative(codec, data, finishedOutput)

	for ocf.Scan() {
		datum, err := ocf.Read()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			continue
		}
		data <- datum
	}
	close(data)
	finishedOutput.Wait()

	return ocf.Err()
}

func textualFromNative(codec *goavro.Codec, data <-chan interface{}, finishedOutput *sync.WaitGroup) {
	for datum := range data {
		buf, err := codec.TextualFromNative(nil, datum)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			continue
		}
		fmt.Println(string(buf))
	}
	finishedOutput.Done()
}

func bail(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err)
	os.Exit(1)
}
