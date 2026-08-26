// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

// NOTE: part of goavro package because it tests private functionality

import (
	"fmt"
	"testing"
)

func TestNameStartsInvalidCharacter(t *testing.T) {
	_, err := newName("&X", "org.foo", nullNamespace)
	if _, ok := err.(ErrInvalidName); err == nil && !ok {
		t.Errorf("GOT: %#v, WANT: %#v", err, ErrInvalidName{"start with [A-Za-z_]"})
	}
}

func TestNameContainsInvalidCharacter(t *testing.T) {
	_, err := newName("X&", "org.foo.bar", nullNamespace)
	if _, ok := err.(ErrInvalidName); err == nil && !ok {
		t.Errorf("GOT: %#v, WANT: %#v", err, ErrInvalidName{"start with [A-Za-z_]"})
	}
}

func TestNamespaceContainsInvalidCharacter(t *testing.T) {
	defer func() { RelaxedNameValidation = false }()
	RelaxedNameValidation = true
	n, err := newName("X", ".org.foo", nullNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := n.fullName, ".org.foo.X"; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
	if actual, expected := n.namespace, ".org.foo"; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
}

func TestRelaxedNameValidationAllowsDollarSign(t *testing.T) {
	defer func() { RelaxedNameValidation = false }()

	// A nested Java class name is rejected by default.
	if _, err := newName("CreateEvent", "org.apache.hadoop.hdfs.inotify.Event$", nullNamespace); err == nil {
		t.Fatal("GOT: nil; WANT: error for '$' without RelaxedNameValidation")
	}

	RelaxedNameValidation = true
	n, err := newName("CreateEvent", "org.apache.hadoop.hdfs.inotify.Event$", nullNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := n.fullName, "org.apache.hadoop.hdfs.inotify.Event$.CreateEvent"; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}

	// '$' is still not valid as the first character of a component.
	if _, err := newName("$Event", "org.foo", nullNamespace); err == nil {
		t.Error("GOT: nil; WANT: error for '$' as first character")
	}
}

func TestNameAndNamespaceProvided(t *testing.T) {
	n, err := newName("X", "org.foo", nullNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := n.fullName, "org.foo.X"; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
	if actual, expected := n.namespace, "org.foo"; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
}

func TestNameWithDotIgnoresNamespace(t *testing.T) {
	n, err := newName("org.bar.X", "some.ignored.namespace", nullNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := n.fullName, "org.bar.X"; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
	if actual, expected := n.namespace, "org.bar"; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
}

func TestNameWithoutDotsButWithEmptyNamespaceAndEnclosingName(t *testing.T) {
	n, err := newName("X", nullNamespace, "org.foo")
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := n.fullName, "org.foo.X"; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
	if actual, expected := n.namespace, "org.foo"; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
}

func TestNewNameFromSchemaMap(t *testing.T) {
	n, err := newNameFromSchemaMap(nullNamespace, map[string]interface{}{
		"name":      "foo",
		"namespace": "",
		"type":      map[string]interface{}{},
	})
	ensureError(t, err)

	if got, want := n.fullName, "foo"; got != want {
		t.Errorf("GOT: %q; WANT: %q", got, want)
	}
	if got, want := n.namespace, ""; got != want {
		t.Errorf("GOT: %q; WANT: %q", got, want)
	}
}

func TestShortName(t *testing.T) {
	cases := []struct {
		name      string
		namespace string
		want      string
	}{
		{"bar", "", "bar"},
		{"foo", "org.bar", "foo"},
		{"bar.foo", "org", "foo"},
	}

	for _, c := range cases {
		n, err := newName(c.name, c.namespace, nullNamespace)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("n: %#v fullName: %v shortName: %v\n", n, n.String(), n.ShortName())
		if actual, expected := n.ShortName(), c.want; actual != expected {
			t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
		}
	}
}
