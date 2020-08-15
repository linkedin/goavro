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

// date
//
// The date logical type represents a date within the calendar, with no
// reference to a particular time zone or time of day.
//
// A date logical type annotates an Avro int, where the int stores the number of
// days from the unix epoch, 1 January 1970 (ISO calendar).
//
// The following schema represents a date:
//
// {
//   "type": "int",
//   "logicalType": "date"
// }

func nativeFromDate(fn toNativeFn) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		l, b, err := fn(bytes)
		if err != nil {
			return l, b, err
		}
		i, ok := l.(int32)
		if !ok {
			return l, b, fmt.Errorf("cannot transform to native date, expected int, received %T", l)
		}
		t := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(i)).UTC()
		return t, b, nil
	}
}

func dateFromNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		switch val := d.(type) {
		case int, int32, int64, float32, float64:
			// "Language implementations may choose to represent logical types with an appropriate native type, although this is not required."
			// especially permitted default values depend on the field's schema type and goavro encodes default values using the field schema
			return fn(b, val)

		case time.Time:
			// rephrasing the avro 1.9.2 spec a date is actually stored as the duration since unix epoch in days
			// time.Unix() returns this duration in seconds and time.UnixNano() in nanoseconds
			// reviewing the source code, both functions are based on the internal function unixSec()
			// unixSec() returns the seconds since unix epoch as int64, whereby Unix() provides the greater range and UnixNano() the higher precision
			// As a date requires a precision of days Unix() provides more then enough precision and a greater range, including the go zero time
			numDays := val.Unix() / 86400
			return fn(b, numDays)

		default:
			return nil, fmt.Errorf("cannot transform to binary date, expected time.Time or Go numeric, received %T", d)
		}
	}
}
