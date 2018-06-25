package goavro

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"
)

type toNativeFn func([]byte) (interface{}, []byte, error)
type fromNativeFn func([]byte, interface{}) ([]byte, error)

//////////////////////////////////////////////////////////////////////////////////////////////
// date logical type - to/from time.Time, time.UTC location
//////////////////////////////////////////////////////////////////////////////////////////////
func dateToNative(fn toNativeFn) toNativeFn {
	return func(b []byte) (interface{}, []byte, error) {
		l, b, err := fn(b)
		if err != nil {
			return l, b, err
		}
		i, ok := l.(int32)
		if !ok {
			return l, b, fmt.Errorf("cannot transform to native date, expected int, received %t", l)
		}
		t := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(i)).UTC()
		return t, b, nil
	}
}

func dateFromNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		t, ok := d.(time.Time)
		if !ok {
			return nil, fmt.Errorf("cannot transform to binary date, expected time.Time, received %T", d)
		}
		// The number of days calculation is incredibly naive we take the time.Duration
		// between the given time and unix epoch and divide that by (24 * time.Hour)
		// This accuracy seems acceptable given the relation to unix epoch for now
		// TODO: replace with a better method
		numDays := t.Sub(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)).Nanoseconds() / (24 * time.Hour.Nanoseconds())
		return fn(b, numDays)
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////
// time-millis logical type - to/from time.Time, time.UTC location
//////////////////////////////////////////////////////////////////////////////////////////////
func timeMillisToNative(fn toNativeFn) toNativeFn {
	return func(b []byte) (interface{}, []byte, error) {
		l, b, err := fn(b)
		if err != nil {
			return l, b, err
		}
		i, ok := l.(int32)
		if !ok {
			return l, b, fmt.Errorf("cannot transform to native time.Duration, expected int, received %t", l)
		}
		t := time.Duration(i) * time.Millisecond
		return t, b, nil
	}
}

func timeMillisFromNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		t, ok := d.(time.Duration)
		if !ok {
			return nil, fmt.Errorf("cannot transform to binary time-millis, expected time.Duration, received %T", d)
		}
		duration := int32(t.Nanoseconds() / 1e6)
		return fn(b, duration)
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////
// time-micros logical type - to/from time.Time, time.UTC location
//////////////////////////////////////////////////////////////////////////////////////////////
func timeMicrosToNative(fn toNativeFn) toNativeFn {
	return func(b []byte) (interface{}, []byte, error) {
		l, b, err := fn(b)
		if err != nil {
			return l, b, err
		}
		i, ok := l.(int64)
		if !ok {
			return l, b, fmt.Errorf("cannot transform to native time.Duration, expected long, received %t", l)
		}
		t := time.Duration(i) * time.Microsecond
		return t, b, nil
	}
}

func timeMicrosFromNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		t, ok := d.(time.Duration)
		if !ok {
			return nil, fmt.Errorf("cannot transform to binary time-micros, expected time.Duration, received %T", d)
		}
		duration := t.Nanoseconds() / 1e3
		return fn(b, duration)
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////
// timestamp-millis logical type - to/from time.Time, time.UTC location
//////////////////////////////////////////////////////////////////////////////////////////////
func timeStampMillisToNative(fn toNativeFn) toNativeFn {
	return func(b []byte) (interface{}, []byte, error) {
		l, b, err := fn(b)
		if err != nil {
			return l, b, err
		}
		i, ok := l.(int64)
		if !ok {
			return l, b, fmt.Errorf("cannot transform native timestamp-millis, expected int64, received %t", l)
		}
		secs := i / 1e3
		nanosecs := i - (secs * 1e3)
		return time.Unix(secs, nanosecs).UTC(), b, nil
	}
}

func timeStampMillisFromNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		t, ok := d.(time.Time)
		if !ok {
			return nil, fmt.Errorf("cannot transform binary timestamp-millis, expected time.Time, received %T", d)
		}
		millisecs := t.UnixNano() / 1e6
		return fn(b, millisecs)
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////
// timestamp-micros logical type - to/from time.Time, time.UTC location
//////////////////////////////////////////////////////////////////////////////////////////////
func timeStampMicrosToNative(fn toNativeFn) toNativeFn {
	return func(b []byte) (interface{}, []byte, error) {
		l, b, err := fn(b)
		if err != nil {
			return l, b, err
		}
		i, ok := l.(int64)
		if !ok {
			return l, b, fmt.Errorf("cannot transform native timestamp-micros, expected int64, received %t", l)
		}
		secs := i / 1e6
		nanosecs := i - (secs * 1e6)
		return time.Unix(secs, nanosecs).UTC(), b, nil
	}
}

func timeStampMicrosFromNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		t, ok := d.(time.Time)
		if !ok {
			return nil, fmt.Errorf("cannot transform binary timestamp-micros, expected time.Time, received %T", d)
		}
		microsecs := t.UnixNano() / 1e3
		return fn(b, microsecs)
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////
// decimal logical-type - byte/fixed - to/from math/big.Rat
// two's complement algorithm taken from:
// https://groups.google.com/d/msg/golang-nuts/TV4bRVrHZUw/UcQt7S4IYlcJ by rog
/////////////////////////////////////////////////////////////////////////////////////////////
type makeCodecFn func(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error)

var one = big.NewInt(1)

func makeDecimalBytesCodec(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	schemaMap["name"] = "bytes.decimal"
	c, err := registerNewCodec(st, schemaMap, enclosingNamespace)
	if err != nil {
		return nil, fmt.Errorf("Bytes ought to have valid name: %s", err)
	}
	precision := schemaMap["precision"]
	scale := schemaMap["scale"]
	p := int(precision.(float64))
	s := int(scale.(float64))
	c.binaryFromNative = decimalBytesFromNative(bytesBinaryFromNative, p, s)
	c.textualFromNative = decimalBytesFromNative(bytesTextualFromNative, p, s)
	c.nativeFromBinary = decimalBytesToNative(bytesNativeFromBinary, p, s)
	c.nativeFromTextual = decimalBytesToNative(bytesNativeFromTextual, p, s)
	return c, nil
}

func decimalBytesToNative(fn toNativeFn, precision, scale int) toNativeFn {
	return func(b []byte) (interface{}, []byte, error) {
		d, o, err := fn(b)
		if err != nil {
			return d, o, err
		}
		bs, ok := d.([]byte)
		if !ok {
			return nil, b, fmt.Errorf("cannot transform to native decimal, expected []byte, received %T", d)
		}
		i := big.NewInt(0)
		fromSignedBytes(i, bs)
		if i.BitLen() > 64 {
			return nil, b, fmt.Errorf("cannot transform to native decimal, max value is 64bit but received: %dbit", i.BitLen())
		}
		r := big.NewRat(i.Int64(), int64(math.Pow10(scale)))
		return r, o, nil
	}
}

func decimalBytesFromNative(fn fromNativeFn, precision, scale int) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		r, ok := d.(*big.Rat)
		if !ok {
			return nil, fmt.Errorf("cannot transform to bytes, expected *big.Rat, received %T", d)
		}
		if precision < 0 {
			return nil, fmt.Errorf("cannot transform to bytes, expected precision to be greater than 0")
		}
		if scale < 0 || scale > precision {
			return nil, fmt.Errorf("cannot transform to bytes, expected scale to be 0 or scale to be greater than precision")
		}
		// we reduce accuracy to precision by dividing and multiplying by digit length
		num := big.NewInt(0).Set(r.Num())
		denom := big.NewInt(0).Set(r.Denom())

		// we get the scaled decimal representation
		i := new(big.Int).Mul(num, big.NewInt(int64(math.Pow10(scale))))
		// divide that by the denominator
		precnum := new(big.Int).Div(i, denom)
		bout, err := toSignedBytes(precnum)
		if err != nil {
			return nil, err
		}
		return fn(b, bout)
	}
}

func makeDecimalFixedCodec(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	schemaMap["name"] = "fixed.decimal"
	c, err := makeFixedCodec(st, enclosingNamespace, schemaMap)
	if err != nil {
		return nil, err
	}
	precision := schemaMap["precision"]
	scale := schemaMap["scale"]
	size, err := getSize(c.typeName, schemaMap)
	if err != nil {
		return nil, err
	}
	p := int(precision.(float64))
	s := int(scale.(float64))
	c.binaryFromNative = decimalBytesFromNative(padBytes(c.binaryFromNative, int(size)), p, s)
	c.textualFromNative = decimalBytesFromNative(padBytes(c.textualFromNative, int(size)), p, s)
	c.nativeFromBinary = decimalBytesToNative(unpadBytes(c.nativeFromBinary), p, s)
	c.nativeFromTextual = decimalBytesToNative(unpadBytes(c.nativeFromTextual), p, s)
	return c, nil
}

func getSize(typeName *name, schemaMap map[string]interface{}) (uint, error) {
	// Fixed type must have size
	sizeRaw, ok := schemaMap["size"]
	if !ok {
		return 0, fmt.Errorf("Fixed %q ought to have size key", typeName)
	}
	var size uint
	switch val := sizeRaw.(type) {
	case string:
		s, err := strconv.ParseUint(val, 10, 0)
		if err != nil {
			return 0, fmt.Errorf("Fixed %q size ought to be number greater than zero: %v", typeName, sizeRaw)
		}
		size = uint(s)
	case float64:
		if val <= 0 {
			return 0, fmt.Errorf("Fixed %q size ought to be number greater than zero: %v", typeName, sizeRaw)
		}
		size = uint(val)
	default:
		return 0, fmt.Errorf("Fixed %q size ought to be number greater than zero: %v", typeName, sizeRaw)
	}
	return size, nil
}

func padBytes(fn fromNativeFn, size int) fromNativeFn {
	return func(bytes []byte, datum interface{}) ([]byte, error) {
		padded := make([]byte, size, size)
		dbytes := datum.([]byte)
		copy(padded[size-len(dbytes):], dbytes)

		return fn(bytes, padded)
	}
}

func unpadBytes(fn toNativeFn) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		d, b, err := fn(bytes)
		if err != nil {
			return nil, b, err
		}
		unpadded := d.([]byte)
		pos := -1
		for idx, ib := range unpadded {
			if ib != []byte("\x00")[0] {
				pos = idx
				break
			}
		}
		return unpadded[pos:], b, nil
	}
}

// fromSignedBytes sets the value of n to the big-endian two's complement
// value stored in the given data. If data[0]&80 != 0, the number
// is negative. If data is empty, the result will be 0.
func fromSignedBytes(n *big.Int, data []byte) {
	n.SetBytes(data)
	if len(data) > 0 && data[0]&0x80 > 0 {
		n.Sub(n, new(big.Int).Lsh(one, uint(len(data))*8))
	}
}

// toSignedBytes returns the big-endian two's complement
// form of n.
func toSignedBytes(n *big.Int) ([]byte, error) {
	switch n.Sign() {
	case 0:
		return []byte{0}, nil
	case 1:
		b := n.Bytes()
		if b[0]&0x80 > 0 {
			b = append([]byte{0}, b...)
		}
		return b, nil
	case -1:
		length := uint(n.BitLen()/8+1) * 8
		b := new(big.Int).Add(n, new(big.Int).Lsh(one, length)).Bytes()
		// When the most significant bit is on a byte
		// boundary, we can get some extra significant
		// bits, so strip them off when that happens.
		if len(b) >= 2 && b[0] == 0xff && b[1]&0x80 != 0 {
			b = b[1:]
		}
		return b, nil
	}
	return nil, fmt.Errorf("toSignedBytes: error big.Int.Sign() returned unexpected value")
}
