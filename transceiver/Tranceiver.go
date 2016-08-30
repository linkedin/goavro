package transceiver

import (
	"bytes"
	"io"
)

type Transceiver interface {
	Transceive(request []bytes.Buffer) ([]io.Reader, error)

}