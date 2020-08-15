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

// Time (millisecond precision)
//
// The time-millis logical type represents a time of day, with no reference to a
// particular calendar, time zone or date, with a precision of one millisecond.
//
// A time-millis logical type annotates an Avro int, where the int stores the
// number of milliseconds after midnight, 00:00:00.000.

func timeDurationNativeFromLogicalTypeTimeMillis(fn toNativeFn) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		l, b, err := fn(bytes)
		if err != nil {
			return l, b, err
		}
		i, ok := l.(int32)
		if !ok {
			return l, b, fmt.Errorf("cannot transform to native time.Duration, expected int, received %T", l)
		}
		t := time.Duration(i) * time.Millisecond
		return t, b, nil
	}
}

func logicalTypeTimeMillisFromNativeTimeDuration(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		switch val := d.(type) {
		case int, int32, int64, float32, float64:
			// "Language implementations may choose to represent logical types with an appropriate native type, although this is not required."
			// especially permitted default values depend on the field's schema type and goavro encodes default values using the field schema
			return fn(b, val)

		case time.Duration:
			duration := int32(val.Nanoseconds() / int64(time.Millisecond))
			return fn(b, duration)

		default:
			return nil, fmt.Errorf("cannot transform to binary time-millis, expected time.Duration or Go numeric, received %T", d)
		}
	}
}

// Time (microsecond precision)
//
// The time-micros logical type represents a time of day, with no reference to a
// particular calendar, time zone or date, with a precision of one microsecond.
//
// A time-micros logical type annotates an Avro long, where the long stores the
// number of microseconds after midnight, 00:00:00.000000.

func timeDurationNativeFromLogicalTypeTimeMicros(fn toNativeFn) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		l, b, err := fn(bytes)
		if err != nil {
			return l, b, err
		}
		i, ok := l.(int64)
		if !ok {
			return l, b, fmt.Errorf("cannot transform to native time.Duration, expected long, received %T", l)
		}
		t := time.Duration(i) * time.Microsecond
		return t, b, nil
	}
}

func logicalTypeTimeMicrosFromNativeTimeDuration(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		switch val := d.(type) {
		case int, int32, int64, float32, float64:
			// "Language implementations may choose to represent logical types with an appropriate native type, although this is not required."
			// especially permitted default values depend on the field's schema type and goavro encodes default values using the field schema
			return fn(b, val)

		case time.Duration:
			duration := int32(val.Nanoseconds() / int64(time.Microsecond))
			return fn(b, duration)

		default:
			return nil, fmt.Errorf("cannot transform to binary time-micros, expected time.Duration or Go numeric, received %T", d)
		}
	}
}
