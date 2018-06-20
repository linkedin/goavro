package goavro

import (
	"fmt"
	"time"
)

type toNativeFn func([]byte) (interface{}, []byte, error)
type fromNativeFn func([]byte, interface{}) ([]byte, error)

///////////////////////////////////////////////
// date logical type - to/from UTC
///////////////////////////////////////////////
func dateToNative(fn toNativeFn) toNativeFn {
	return func(b []byte) (interface{}, []byte, error) {
		l, b, err := fn(b)
		if err != nil {
			return l, b, err
		}
		i, ok := l.(int)
		if !ok {
			// This error condition will not trigger with the existing longX functions as they only error on short buffer error
			return l, b, fmt.Errorf("cannot transform native timestamp-millis, expected int64, received %t", l)
		}
		t := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i).UTC()
		return t, b, nil
	}
}

func dateFromNative(fn fromNativeFn) fromNativeFn {
	return nil
}

///////////////////////////////////////////////
// timestamp-millis logical type - to/from UTC
///////////////////////////////////////////////
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
