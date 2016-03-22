// Copyright 2015 LinkedIn Corp. Licensed under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License.  You may obtain a copy of the License
// at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.Copyright [201X] LinkedIn Corp. Licensed under the Apache
// License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.

package goavro

import (
	"testing"
)

func TestNameEnforcesNameRequirements(t *testing.T) {
	n := &name{}
	err := nameName("")(n)
	checkError(t, err, "not be empty")

	err = nameName("0")(n)
	checkError(t, err, "start with [A-Za-z_]")

	err = nameName("_|")(n)
	checkError(t, err, "remaining characters contain only [A-Za-z0-9_]")

	err = nameName("_0aZ")(n)
	checkError(t, err, nil)
}

func TestNameAndNamespaceBothSpecified(t *testing.T) {
	a, err := newName(
		nameName("X"),
		nameNamespace("org.foo"),
		nameEnclosingNamespace("enclosing.namespace"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	expected := &name{n: "org.foo.X"}
	if !a.equals(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", a, expected)
	}
}

func TestNameWithDots(t *testing.T) {
	a, err := newName(
		nameName("org.foo.X"),
		nameNamespace("namespace"),
		nameEnclosingNamespace("enclosing.namespace"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	expected := &name{n: "org.foo.X"}
	if !a.equals(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", a, expected)
	}
}

func TestNameWithoutDots(t *testing.T) {
	a, err := newName(nameName("X"), nameEnclosingNamespace("enclosing.namespace"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	expected := &name{n: "enclosing.namespace.X"}
	if !a.equals(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", a, expected)
	}

	a, err = newName(nameName("X"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	expected = &name{n: "X"}
	if !a.equals(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", a, expected)
	}
}

func TestNamePrimitiveTypesHaveNoDotPrefix(t *testing.T) {
	// null
	a, err := newName(nameName("null"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	expected := &name{n: "null"}
	if !a.equals(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", a, expected)
	}
	// bool
	a, err = newName(nameName("bool"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	expected = &name{n: "bool"}
	if !a.equals(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", a, expected)
	}
	// int
	a, err = newName(nameName("int"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	expected = &name{n: "int"}
	if !a.equals(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", a, expected)
	}
	// long
	a, err = newName(nameName("long"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	expected = &name{n: "long"}
	if !a.equals(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", a, expected)
	}
	// float
	a, err = newName(nameName("float"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	expected = &name{n: "float"}
	if !a.equals(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", a, expected)
	}
	// double
	a, err = newName(nameName("double"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	expected = &name{n: "double"}
	if !a.equals(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", a, expected)
	}
	// bytes
	a, err = newName(nameName("bytes"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	expected = &name{n: "bytes"}
	if !a.equals(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", a, expected)
	}
	// string
	a, err = newName(nameName("string"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	expected = &name{n: "string"}
	if !a.equals(expected) {
		t.Errorf("Actual: %#v; Expected: %#v", a, expected)
	}
}

func TestNameNamespaceWithNamespace(t *testing.T) {
	someName := &name{n: "org.foo.X"}
	someNamespace := someName.namespace()
	if someNamespace != "org.foo" {
		t.Errorf("Actual: %#v; Expected: %#v", someNamespace, "org.foo")
	}
}

func TestNameNamespaceWithoutNamespace(t *testing.T) {
	someName := &name{n: "X"}
	someNamespace := someName.namespace()
	if someNamespace != "" {
		t.Errorf("Actual: %#v; Expected: %#v", someNamespace, "")
	}
}
