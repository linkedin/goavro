// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

// Duration
//
// The duration logical type represents an amount of time defined by a number of
// months, days and milliseconds. This is not equivalent to a number of
// milliseconds, because, depending on the moment in time from which the
// duration is measured, the number of days in the month and number of
// milliseconds in a day may differ. Other standard periods such as years,
// quarters, hours and minutes can be expressed through these basic periods.
//
// A duration logical type annotates Avro fixed type of size 12, which stores
// three little-endian unsigned integers that represent durations at different
// granularities of time. The first stores a number in months, the second stores
// a number in days, and the third stores a number in milliseconds.
//
// TODO
