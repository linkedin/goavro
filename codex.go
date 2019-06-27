package goavro

import (
	"strconv"
	"sync"
)

// Codex is a structure for dealing with zero or more Codec instances.
type Codex struct {
	m map[uint64]*Codec
	l sync.RWMutex
}

// NewCodex returns a new Codex instance.
func NewCodex() *Codex {
	return &Codex{m: make(map[uint64]*Codec)}
}

// Load returns the Codec instance stored in the Codex associated with signed
// 64-bit Rabin fingerprint.
func (x *Codex) Load(fingerprint uint64) (*Codec, bool) {
	x.l.RLock()
	c, ok := x.m[fingerprint]
	x.l.RUnlock()
	return c, ok
}

// Store saves codec in the Codex for future use.  The signed 64-bit Rabin
// fingerprint for codec is not a required parameter because its value is a
// Codec field.
func (x *Codex) Store(codec *Codec) {
	x.l.Lock()
	x.m[codec.Rabin] = codec
	x.l.Unlock()
}

// NativeFromSingle looks up the required Codec fingerprint in the Codex and
// uses it to decode buf.  When buf is too short or does not begin with the
// magic Single-Object Encoding prefix it returns ErrNotSingleObjectEncoded.
// When buf requires a Codec that has not been registered in the Codex it
// returns ErrUnknownCodec.  In all other cases, it returns the result of
// attempting to decode the single-object encoded datum from buf.
func (x *Codex) NativeFromSingle(buf []byte) (interface{}, []byte, error) {
	fingerprint, newBuf, err := FingerprintFromSOE(buf)
	if err != nil {
		return nil, buf, err
	}

	x.l.RLock()
	c, ok := x.m[fingerprint]
	x.l.RUnlock()

	if !ok {
		return nil, nil, ErrUnknownCodec(fingerprint)
	}

	value, newBuf, err := c.nativeFromBinary(newBuf)
	if err != nil {
		return nil, buf, err // if error, return original byte slice
	}
	return value, newBuf, nil
}

// ErrUnknownCodec is returned when the required Codec for decoding the buffer
// is not yet registered in the Codex.
type ErrUnknownCodec uint64

func (e ErrUnknownCodec) Error() string { return "unknown codec: " + strconv.FormatUint(uint64(e), 10) }
