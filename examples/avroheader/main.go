package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/linkedin/goavro"
)

var (
	showCount  = flag.Bool("count", false, "show count of data items")
	showSchema = flag.Bool("schema", false, "show data schema")
)

func usage() {
	executable, err := os.Executable()
	if err != nil {
		executable = os.Args[0]
	}
	base := filepath.Base(executable)
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", base)
	fmt.Fprintf(os.Stderr, "\t%s [-count] [-schema] [file1.avro...]\n", base)
	fmt.Fprintf(os.Stderr, "\tAs a special case, when there are no filename arguments, %s will read\n", base)
	fmt.Fprintf(os.Stderr, "\tfrom its standard input.\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	flag.Parse()

	args := flag.Args()

	if len(args) == 0 {
		stat, err := os.Stdin.Stat()
		if err != nil {
			bail(err)
		}
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			usage()
		}
		if err := headerFromReader(os.Stdin, ""); err != nil {
			bail(err)
		}
	}

	for _, arg := range args {
		fh, err := os.Open(arg)
		if err != nil {
			bail(err)
		}
		if len(args) > 1 {
			arg += ": "
		} else {
			arg = ""
		}
		if err := headerFromReader(fh, arg); err != nil {
			bail(err)
		}
		if err := fh.Close(); err != nil {
			bail(err)
		}
	}
}

func headerFromReader(ior io.Reader, prefix string) error {
	ocfr, err := goavro.NewOCFReader(ior)
	if err != nil {
		return err
	}

	fmt.Printf("%sCompression Algorithm (avro.codec): %q\n", prefix, ocfr.CompressionName())

	if *showSchema {
		fmt.Printf("%sSchema (avro.schema):\n%s\n", prefix, ocfr.Codec().Schema())
	}

	if !*showCount {
		return nil
	}

	var decoded, errors int

	for ocfr.Scan() {
		_, err := ocfr.Read()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			errors++
			continue
		}
		decoded++
	}

	if decoded > 0 {
		fmt.Printf("%sSuccessfully decoded: %d\n", prefix, decoded)
	}
	if errors > 0 {
		fmt.Printf("%sCannot decode: %d\n", prefix, errors)
	}

	return ocfr.Err()
}

func bail(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err)
	os.Exit(1)
}
