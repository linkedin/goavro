// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Modified by Datastax Inc. 7/4/2022

package goavro

import (
	"fmt"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

var someTime = time.Unix(123, 0)
var answer int64 = 42

type (
	userDefined       float64
	userDefinedSlice  []int
	userDefinedString string
)

type conversionTest struct {
	s, d interface{} // source and destination

	// following are used if they're non-zero
	wantint     int64
	wantuint    uint64
	wantBigInt  *big.Int
	wantstr     string
	wantbytes   []byte
	wantf32     float32
	wantf64     float64
	wantDecimal *decimal.Decimal
	wanttime    time.Time
	wantUUID    uuid.UUID
	wantbool    bool // used if d is of type *bool
	wanterr     string
	wantiface   interface{}
	wantmap     map[string]int
	wantptr     *int64 // if non-nil, *d's pointed value must be equal to *wantptr
	wantnil     bool   // if true, *d must be *int64(nil)
	wantusrdef  userDefined
	wantusrstr  userDefinedString
}

// Target variables for scanning into.
var (
	scanstr     string
	scanbytes   []byte
	scanint     int
	scanuint8   uint8
	scanuint16  uint16
	scanBigInt  *big.Int
	scanbool    bool
	scanf32     float32
	scanf64     float64
	scanDecimal *decimal.Decimal
	scantime    time.Time
	scanUUID    uuid.UUID
	scanptr     *int64
	scaniface   interface{}
	scanmap     map[string]int
)

func conversionTests() []conversionTest {
	id := uuid.MustParse("12345678-1234-5678-1234-567812345678")
	dec, _ := decimal.NewFromString("1.23456789")

	// Return a fresh instance to test so "go test -count 2" works correctly.
	return []conversionTest{
		// Exact conversions (destination pointer type matches source type)
		{s: "foo", d: &scanstr, wantstr: "foo"},
		{s: 123, d: &scanint, wantint: 123},
		{s: someTime, d: &scantime, wanttime: someTime},
		{s: dec, d: &scanDecimal, wantDecimal: &dec},
		{s: big.NewInt(123), d: &scanBigInt, wantBigInt: big.NewInt(123)},
		{s: id, d: &scanUUID, wantUUID: id},

		// To strings
		{s: "string", d: &scanstr, wantstr: "string"},
		{s: []byte("byteslice"), d: &scanstr, wantstr: "byteslice"},
		{s: 123, d: &scanstr, wantstr: "123"},
		{s: int8(123), d: &scanstr, wantstr: "123"},
		{s: int64(123), d: &scanstr, wantstr: "123"},
		{s: uint8(123), d: &scanstr, wantstr: "123"},
		{s: uint16(123), d: &scanstr, wantstr: "123"},
		{s: uint32(123), d: &scanstr, wantstr: "123"},
		{s: uint64(123), d: &scanstr, wantstr: "123"},
		{s: 1.5, d: &scanstr, wantstr: "1.5"},
		{s: id, d: &scanstr, wantstr: "12345678-1234-5678-1234-567812345678"},
		{s: nil, d: &scanstr, wantstr: ""},

		// From time.Time:
		{s: time.Unix(1, 0).UTC(), d: &scanstr, wantstr: "1970-01-01T00:00:01Z"},
		{s: time.Unix(1453874597, 0).In(time.FixedZone("here", -3600*8)), d: &scanstr, wantstr: "2016-01-26T22:03:17-08:00"},
		{s: time.Unix(1, 2).UTC(), d: &scanstr, wantstr: "1970-01-01T00:00:01.000000002Z"},
		{s: time.Time{}, d: &scanstr, wantstr: "0001-01-01T00:00:00Z"},
		{s: time.Unix(1, 2).UTC(), d: &scanbytes, wantbytes: []byte("1970-01-01T00:00:01.000000002Z")},
		{s: time.Unix(1, 2).UTC(), d: &scaniface, wantiface: time.Unix(1, 2).UTC()},

		// To uuid.UUID
		{s: id, d: &scanUUID, wantUUID: id},

		// To []byte
		{s: nil, d: &scanbytes, wantbytes: nil},
		{s: "string", d: &scanbytes, wantbytes: []byte("string")},
		{s: []byte("byteslice"), d: &scanbytes, wantbytes: []byte("byteslice")},
		{s: 123, d: &scanbytes, wantbytes: []byte("123")},
		{s: int8(123), d: &scanbytes, wantbytes: []byte("123")},
		{s: int64(123), d: &scanbytes, wantbytes: []byte("123")},
		{s: uint8(123), d: &scanbytes, wantbytes: []byte("123")},
		{s: uint16(123), d: &scanbytes, wantbytes: []byte("123")},
		{s: uint32(123), d: &scanbytes, wantbytes: []byte("123")},
		{s: uint64(123), d: &scanbytes, wantbytes: []byte("123")},
		{s: 1.5, d: &scanbytes, wantbytes: []byte("1.5")},
		{s: id, d: &scanbytes, wantbytes: id[:]},

		// Strings to integers
		{s: "255", d: &scanuint8, wantuint: 255},
		{s: "256", d: &scanuint8, wanterr: "converting driver.Value type string (\"256\") to a uint8: value out of range"},
		{s: "256", d: &scanuint16, wantuint: 256},
		{s: "-1", d: &scanint, wantint: -1},
		{s: "foo", d: &scanint, wanterr: "converting driver.Value type string (\"foo\") to a int: invalid syntax"},

		// int64 to smaller integers
		{s: int64(5), d: &scanuint8, wantuint: 5},
		{s: int64(256), d: &scanuint8, wanterr: "converting driver.Value type int64 (\"256\") to a uint8: value out of range"},
		{s: int64(256), d: &scanuint16, wantuint: 256},
		{s: int64(65536), d: &scanuint16, wanterr: "converting driver.Value type int64 (\"65536\") to a uint16: value out of range"},

		// True bools
		{s: true, d: &scanbool, wantbool: true},

		// False bools
		{s: false, d: &scanbool, wantbool: false},

		// Not bools
		{s: "yup", d: &scanbool, wanterr: "unsupported Scan, storing driver.Value type string into type *bool"},
		{s: 2, d: &scanbool, wanterr: "unsupported Scan, storing driver.Value type int into type *bool"},

		// Floats
		{s: 1.5, d: &scanf64, wantf64: 1.5},
		{s: int64(1), d: &scanf64, wantf64: float64(1)},
		{s: 1.5, d: &scanf32, wantf32: float32(1.5)},
		{s: "1.5", d: &scanf32, wantf32: float32(1.5)},
		{s: "1.5", d: &scanf64, wantf64: 1.5},

		// Pointers
		{s: interface{}(nil), d: &scanptr, wantnil: true},
		{s: int64(42), d: &scanptr, wantptr: &answer},

		// To interface{}
		{s: 1.5, d: &scaniface, wantiface: 1.5},
		{s: int64(1), d: &scaniface, wantiface: int64(1)},
		{s: "str", d: &scaniface, wantiface: "str"},
		{s: []byte("byteslice"), d: &scaniface, wantiface: []byte("byteslice")},
		{s: true, d: &scaniface, wantiface: true},
		{s: nil, d: &scaniface},
		{s: []byte(nil), d: &scaniface, wantiface: []byte(nil)},

		// Maps
		{s: map[string]int{"a": 1}, d: &scanmap, wantmap: map[string]int{"a": 1}},
		{s: nil, d: &scanmap, wantmap: nil},

		// To a user-defined type
		{s: 1.5, d: new(userDefined), wantusrdef: 1.5},
		{s: int64(123), d: new(userDefined), wantusrdef: 123},
		{s: "1.5", d: new(userDefined), wantusrdef: 1.5},
		{s: []byte{1, 2, 3}, d: new(userDefinedSlice), wanterr: `unsupported Scan, storing driver.Value type []uint8 into type *goavro.userDefinedSlice`},
		{s: "str", d: new(userDefinedString), wantusrstr: "str"},

		// Other errors
		{s: complex(1, 2), d: &scanstr, wanterr: `unsupported Scan, storing driver.Value type complex128 into type *string`},
	}
}

func intPtrValue(intptr interface{}) interface{} {
	return reflect.Indirect(reflect.Indirect(reflect.ValueOf(intptr))).Int()
}

func intValue(intptr interface{}) int64 {
	return reflect.Indirect(reflect.ValueOf(intptr)).Int()
}

func uintValue(intptr interface{}) uint64 {
	return reflect.Indirect(reflect.ValueOf(intptr)).Uint()
}

func float64Value(ptr interface{}) float64 {
	return *(ptr.(*float64))
}

func float32Value(ptr interface{}) float32 {
	return *(ptr.(*float32))
}

func timeValue(ptr interface{}) time.Time {
	return *(ptr.(*time.Time))
}

func TestConversions(t *testing.T) {
	for n, ct := range conversionTests() {
		err := convertAssign(ct.d, ct.s)
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		errf := func(format string, args ...interface{}) {
			base := fmt.Sprintf("convertAssign #%d: for %v (%T) -> %T, ", n, ct.s, ct.s, ct.d)
			t.Errorf(base+format, args...)
		}
		if errstr != ct.wanterr {
			errf("got error %q, want error %q", errstr, ct.wanterr)
		}
		if ct.wantstr != "" && ct.wantstr != scanstr {
			errf("want string %q, got %q", ct.wantstr, scanstr)
		}
		if ct.wantbytes != nil && string(ct.wantbytes) != string(scanbytes) {
			errf("want byte %q, got %q", ct.wantbytes, scanbytes)
		}
		if ct.wantint != 0 && ct.wantint != intValue(ct.d) {
			errf("want int %d, got %d", ct.wantint, intValue(ct.d))
		}
		if ct.wantuint != 0 && ct.wantuint != uintValue(ct.d) {
			errf("want uint %d, got %d", ct.wantuint, uintValue(ct.d))
		}
		if ct.wantf32 != 0 && ct.wantf32 != float32Value(ct.d) {
			errf("want float32 %v, got %v", ct.wantf32, float32Value(ct.d))
		}
		if ct.wantf64 != 0 && ct.wantf64 != float64Value(ct.d) {
			errf("want float32 %v, got %v", ct.wantf64, float64Value(ct.d))
		}
		if bp, boolTest := ct.d.(*bool); boolTest && *bp != ct.wantbool && ct.wanterr == "" {
			errf("want bool %v, got %v", ct.wantbool, *bp)
		}
		if ct.wantUUID.String() != "00000000-0000-0000-0000-000000000000" && ct.wantUUID != scanUUID {
			errf("want UUID %q, got %q", ct.wantUUID, scanUUID)
		}
		if !ct.wanttime.IsZero() && !ct.wanttime.Equal(timeValue(ct.d)) {
			errf("want time %v, got %v", ct.wanttime, timeValue(ct.d))
		}
		if ct.wantnil && *ct.d.(**int64) != nil {
			errf("want nil, got %v", intPtrValue(ct.d))
		}
		if ct.wantptr != nil {
			if *ct.d.(**int64) == nil {
				errf("want pointer to %v, got nil", *ct.wantptr)
			} else if *ct.wantptr != intPtrValue(ct.d) {
				errf("want pointer to %v, got %v", *ct.wantptr, intPtrValue(ct.d))
			}
		}
		if ifptr, ok := ct.d.(*interface{}); ok {
			if !reflect.DeepEqual(ct.wantiface, scaniface) {
				errf("want interface %#v, got %#v", ct.wantiface, scaniface)
				continue
			}
			if srcBytes, ok := ct.s.([]byte); ok {
				dstBytes := (*ifptr).([]byte)
				if len(srcBytes) > 0 && &dstBytes[0] == &srcBytes[0] {
					errf("copy into interface{} didn't copy []byte data")
				}
			}
		}
		if ct.wantusrdef != 0 && ct.wantusrdef != *ct.d.(*userDefined) {
			errf("want userDefined %f, got %f", ct.wantusrdef, *ct.d.(*userDefined))
		}
		if len(ct.wantusrstr) != 0 && ct.wantusrstr != *ct.d.(*userDefinedString) {
			errf("want userDefined %q, got %q", ct.wantusrstr, *ct.d.(*userDefinedString))
		}
	}
}

// https://golang.org/issues/13905
func TestUserDefinedBytes(t *testing.T) {
	type userDefinedBytes []byte
	var u userDefinedBytes
	v := []byte("foo")

	err := convertAssign(&u, v)
	if err != nil {
		t.Fatalf("convertAssign(%v, %v) unexpected error: %v", u, v, err)
	}
	if &u[0] == &v[0] {
		t.Fatal("userDefinedBytes got potentially dirty driver memory")
	}
}
