// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"fmt"
	"time"
)

// Timestamp (millisecond precision)
//
// The timestamp-millis logical type represents an instant on the global
// timeline, independent of a particular time zone or calendar, with a precision
// of one millisecond. Please note that time zone information gets lost in this
// process. Upon reading a value back, we can only reconstruct the instant, but
// not the original representation. In practice, such timestamps are typically
// displayed to users in their local time zones, therefore they may be displayed
// differently depending on the execution environment.
//
// A timestamp-millis logical type annotates an Avro long, where the long stores
// the number of milliseconds from the unix epoch, 1 January 1970 00:00:00.000
// UTC.

func timeNativeFromLogicalTypeTimestampMillis(fn toNativeFn) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		l, b, err := fn(bytes)
		if err != nil {
			return l, b, err
		}
		milliseconds, ok := l.(int64)
		if !ok {
			return l, b, fmt.Errorf("cannot transform native timestamp-millis, expected int64, received %T", l)
		}
		seconds := milliseconds / 1e3
		nanoseconds := (milliseconds - (seconds * 1e3)) * 1e6
		return time.Unix(seconds, nanoseconds).UTC(), b, nil
	}
}

func logicalTypeTimestampMillisFromTimeNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		switch val := d.(type) {
		case int, int32, int64, float32, float64:
			// "Language implementations may choose to represent logical types with an appropriate native type, although this is not required."
			// especially permitted default values depend on the field's schema type and goavro encodes default values using the field schema
			return fn(b, val)

		case time.Time:
			// While this code performs a few more steps than seem required, it is
			// written this way to allow the best time resolution without overflowing the int64 value.
			return fn(b, val.Unix()*1e3+int64(val.Nanosecond()/1e6))

		default:
			return nil, fmt.Errorf("cannot transform to binary timestamp-millis, expected time.Time or Go numeric, received %T", d)
		}
	}
}

// Timestamp (microsecond precision)
//
// The timestamp-micros logical type represents an instant on the global
// timeline, independent of a particular time zone or calendar, with a precision
// of one microsecond. Please note that time zone information gets lost in this
// process. Upon reading a value back, we can only reconstruct the instant, but
// not the original representation. In practice, such timestamps are typically
// displayed to users in their local time zones, therefore they may be displayed
// differently depending on the execution environment.
//
// A timestamp-micros logical type annotates an Avro long, where the long stores
// the number of microseconds from the unix epoch, 1 January 1970
// 00:00:00.000000 UTC.

func timeNativeFromLogicalTypeTimestampMicros(fn toNativeFn) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		l, b, err := fn(bytes)
		if err != nil {
			return l, b, err
		}
		microseconds, ok := l.(int64)
		if !ok {
			return l, b, fmt.Errorf("cannot transform native timestamp-micros, expected int64, received %T", l)
		}
		// While this code performs a few more steps than seem required, it is
		// written this way to allow the best time resolution on UNIX and
		// Windows without overflowing the int64 value.  Windows has a zero-time
		// value of 1601-01-01 UTC, and the number of nanoseconds since that
		// zero-time overflows 64-bit integers.
		seconds := microseconds / 1e6
		nanoseconds := (microseconds - (seconds * 1e6)) * 1e3
		return time.Unix(seconds, nanoseconds).UTC(), b, nil
	}
}

func logicalTypeTimestampMicrosFromTimeNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		switch val := d.(type) {
		case int, int32, int64, float32, float64:
			// "Language implementations may choose to represent logical types with an appropriate native type, although this is not required."
			// especially permitted default values depend on the field's schema type and goavro encodes default values using the field schema
			return fn(b, val)

		case time.Time:
			// While this code performs a few more steps than seem required, it is
			// written this way to allow the best time resolution on UNIX and
			// Windows without overflowing the int64 value.  Windows has a zero-time
			// value of 1601-01-01 UTC, and the number of nanoseconds since that
			// zero-time overflows 64-bit integers.
			return fn(b, val.Unix()*1e6+int64(val.Nanosecond()/1e3))

		default:
			return nil, fmt.Errorf("cannot transform to binary timestamp-micros, expected time.Time or Go numeric, received %T", d)
		}
	}
}
