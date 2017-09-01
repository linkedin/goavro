package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/linkedin/goavro"
)

func bail(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err)
	os.Exit(1)
}

func usage(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
	}
	executable, err := os.Executable()
	if err != nil {
		executable = os.Args[0]
	}
	base := filepath.Base(executable)
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", base)
	fmt.Fprintf(os.Stderr, "\t%s [-v] [-summary] [-bc N] [-compression null|deflate|snappy] [-schema new-schema.avsc] source.avro destination.avro\n", base)
	fmt.Fprintf(os.Stderr, "\tWhen source.avro pathname is hyphen, %s will read from its standard input.\n", base)
	fmt.Fprintf(os.Stderr, "\tWhen destination.avro pathname is hyphen, %s will write to its standard output.\n", base)
	flag.PrintDefaults()
	os.Exit(2)
}

var (
	blockCount                      *int
	compressionName, schemaPathname *string
	summary, verbose                *bool
)

func init() {
	compressionName = flag.String("compression", "", "compression codec ('null', 'deflate', 'snappy'; default: use source compression)")
	blockCount = flag.Int("bc", 0, "max count of items in each block (default: use source block boundaries)")
	schemaPathname = flag.String("schema", "", "pathname to new schema (default: use source schema)")
	summary = flag.Bool("summary", false, "print summary information to stderr")
	verbose = flag.Bool("v", false, "print verbose information to stderr (implies: -summary)")
}

func main() {
	flag.Parse()

	if count := len(flag.Args()); count != 2 {
		usage(fmt.Errorf("wrong number of arguments: %d", count))
	}

	if *blockCount < 0 {
		usage(fmt.Errorf("count must be greater or equal to 0: %d", *blockCount))
	}

	if *verbose {
		*summary = true
	}

	var err error
	var fromF io.ReadCloser
	var toF io.WriteCloser

	if srcPathname := flag.Arg(0); srcPathname == "-" {
		stat, err := os.Stdin.Stat()
		if err != nil {
			bail(err)
		}
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			usage(errors.New("cannot read from standard input when connected to terminal"))
		}
		fromF = os.Stdin
		if *summary {
			fmt.Fprintf(os.Stderr, "reading from stdin\n")
		}
	} else {
		fromF, err = os.Open(srcPathname)
		if err != nil {
			bail(err)
		}
		defer func(ioc io.Closer) {
			if err := ioc.Close(); err != nil {
				bail(err)
			}
		}(fromF)
		if *summary {
			fmt.Fprintf(os.Stderr, "reading from %s\n", flag.Arg(0))
		}
	}

	if destPathname := flag.Arg(1); destPathname == "-" {
		stat, err := os.Stdout.Stat()
		if err != nil {
			bail(err)
		}
		// if *verbose { // DEBUG
		// 	fmt.Fprintf(os.Stderr, "standard output mode: %v\n", stat.Mode())
		// }
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			usage(errors.New("cannot send to standard output when connected to terminal"))
		}
		toF = os.Stdout
		if *summary {
			fmt.Fprintf(os.Stderr, "writing to stdout\n")
		}
	} else {
		toF, err = os.Create(destPathname)
		if err != nil {
			bail(err)
		}
		defer func(ioc io.Closer) {
			if err := ioc.Close(); err != nil {
				bail(err)
			}
		}(toF)
		if *summary {
			fmt.Fprintf(os.Stderr, "writing to %s\n", flag.Arg(1))
		}
	}

	// NOTE: Convert fromF to OCFReader
	ocfr, err := goavro.NewOCFReader(fromF)
	if err != nil {
		bail(err)
	}

	inputCompressionName := ocfr.CompressionName()
	outputCompressionName := inputCompressionName
	if *compressionName != "" {
		outputCompressionName = *compressionName
	}

	if *summary {
		fmt.Fprintf(os.Stderr, "input compression algorithm: %s\n", inputCompressionName)
		fmt.Fprintf(os.Stderr, "output compression algorithm: %s\n", outputCompressionName)
	}

	// NOTE: Either use schema from reader, or attempt to use new schema
	var outputSchema string
	if *schemaPathname == "" {
		outputSchema = ocfr.Codec().Schema()
	} else {
		schemaBytes, err := ioutil.ReadFile(*schemaPathname)
		if err != nil {
			bail(err)
		}
		outputSchema = string(schemaBytes)
	}

	// NOTE: Convert toF to OCFWriter
	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               toF,
		CompressionName: outputCompressionName,
		Schema:          outputSchema,
	})
	if err != nil {
		bail(err)
	}

	if err := transcode(ocfr, ocfw); err != nil {
		bail(err)
	}
}

func transcode(from *goavro.OCFReader, to *goavro.OCFWriter) error {
	var blocksRead, blocksWritten, itemsRead int

	var block []interface{}
	if *blockCount > 0 {
		block = make([]interface{}, 0, *blockCount)
	}

	for from.Scan() {
		datum, err := from.Read()
		if err != nil {
			break
		}

		itemsRead++
		block = append(block, datum)

		endOfBlock := from.RemainingBlockItems() == 0
		if endOfBlock {
			blocksRead++
			if *verbose {
				fmt.Fprintf(os.Stderr, "read block with %d items\n", len(block))
			}
		}

		// NOTE: When blockCount is 0, user wants each destination block to have
		// the same number of items as its corresponding source block. However,
		// when blockCount is greater than 0, user wants specified block count
		// sizes.
		if (*blockCount == 0 && endOfBlock) || (*blockCount > 0 && len(block) == *blockCount) {
			if err := writeBlock(to, block); err != nil {
				return err
			}
			blocksWritten++
			block = block[:0] // set slice length to 0 in order to re-use allocated underlying array
		}
	}

	var err error

	// append all remaining items (condition can only be true used when *blockCount > 0)
	if len(block) > 0 {
		if err = writeBlock(to, block); err == nil {
			blocksWritten++
		}
	}

	// if no write error, then return any read error encountered
	if err == nil {
		err = from.Err()
	}

	if *summary {
		fmt.Fprintf(os.Stderr, "read %d items\n", itemsRead)
		fmt.Fprintf(os.Stderr, "wrote %d blocks\n", blocksWritten)
	}

	return err
}

func writeBlock(to *goavro.OCFWriter, block []interface{}) error {
	if *verbose {
		fmt.Fprintf(os.Stderr, "writing block with %d items\n", len(block))
	}
	return to.Append(block)
}
