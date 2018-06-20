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
