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
	"fmt"
	"strings"
)

const (
	nullNamespace = ""
)

type name struct {
	n   string
	ns  string
	ens string
}

type nameSetter func(*name) error

func newName(setters ...nameSetter) (*name, error) {
	n := &name{}
	for _, setter := range setters {
		err := setter(n)
		if err != nil {
			return nil, err
		}
	}
	// if name contains dot, then ignore namespace and enclosing namespace
	if !strings.ContainsRune(n.n, '.') {
		if n.ns != "" {
			n.n = n.ns + "." + n.n
		} else if n.ens != "" {
			n.n = n.ens + "." + n.n
		}
	}
	return n, nil
}

func nameSchema(schema map[string]interface{}) nameSetter {
	return func(n *name) error {
		val, ok := schema["name"]
		if !ok {
			return fmt.Errorf("ought to have name key")
		}
		n.n, ok = val.(string)
		if !ok || len(n.n) == 0 {
			return fmt.Errorf("name ought to be non-empty string: %T", n)
		}
		if val, ok := schema["namespace"]; ok {
			n.ns, ok = val.(string)
			if !ok {
				return fmt.Errorf("namespace ought to be a string: %T", n)
			}
		}
		return nil
	}
}

func nameName(someName string) nameSetter {
	return func(n *name) error {
		n.n = someName
		return nil
	}
}

func nameEnclosingNamespace(someNamespace string) nameSetter {
	return func(n *name) error {
		n.ens = someNamespace
		return nil
	}
}

func nameNamespace(someNamespace string) nameSetter {
	return func(n *name) error {
		n.ns = someNamespace
		return nil
	}
}

func (n *name) equals(b *name) bool {
	if n.n == b.n {
		return true
	}
	return false
}

func (n name) namespace() string {
	li := strings.LastIndex(n.n, ".")
	if li == -1 {
		return ""
	}
	return n.n[:li]
}

func (n name) GoString() string {
	return n.n
}

func (n name) String() string {
	return n.n
}
