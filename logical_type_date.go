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
//     {
//       "type": "int",
//       "logicalType": "date"
//     }

func timeDateNativeFromLogicalTypeDate(fn toNativeFn) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		l, b, err := fn(bytes)
		if err != nil {
			return l, b, err
		}
		i, ok := l.(int32)
		if !ok {
			return l, b, fmt.Errorf("cannot decode int.date; expected int, received %T", l)
		}
		t := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(i)).UTC()
		return t, b, nil
	}
}

func logicalTypeDateFromTimeDateNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		switch val := d.(type) {
		case int, int32, int64, float32, float64:
			// "Language implementations may choose to represent logical types with an appropriate native type, although this is not required."
			// especially permitted default values depend on the field's schema type and goavro encodes default values using the field schema
			return fn(b, val)

		case time.Time:
			numberOfDays := val.Unix() / 86400 // ignore fractions of a day, which would represent time of that day.
			return fn(b, numberOfDays)

		default:
			return nil, fmt.Errorf("cannot encode int.date: expected time.Time or Go numeric; received %T", d)
		}
	}
}
