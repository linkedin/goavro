package goavro

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// pcfProcessor is a function type that given a parsed JSON object, returns its
// Parsing Canonical Form accroding to Avro specification.
type pcfProcessor func(s interface{}) string

// parsingCanonialForm returns the "Parsing Canonical Form" (pcf) for a parsed
// JSON structure of a *valid* Avro schema.
func parsingCanonicalForm(schema interface{}) string {
	var proc pcfProcessor

	proc = func(s interface{}) string {
		switch val := s.(type) {
		case map[string]interface{}: // JSON object
			return pcfMap(val, proc)
		case []interface{}: // JSON array
			return pcfArray(val, proc)
		case string: // Standalone string
			return pcfString(val, proc)
		case float64:
			return pcfFloat64(val, proc)
		default: // Invalid JSON element within the schema; ignore
			return ""
		}
	}

	return proc(schema)
}

// pcfFloat64 returns the parsing canonical form for a float64 value
func pcfFloat64(val float64, proc pcfProcessor) string {
	return strconv.FormatFloat(val, 'g', -1, 64)
}

// pcfString returns the parsing canonical form for a string value
func pcfString(val string, proc pcfProcessor) string {
	return `"` + val + `"`
}

// pcfArray returns the parsing canonical form for a JSON array
func pcfArray(val []interface{}, proc pcfProcessor) string {
	var elements = make([]string, len(val))
	for i, el := range val {
		elements[i] = proc(el)
	}
	return "[" + strings.Join(elements, ",") + "]"
}

// pcfMap returns the parsing canonical form for a JSON map
func pcfMap(jsonMap map[string]interface{}, proc pcfProcessor) string {
	var els = make(stringPairs, 0, len(jsonMap))

	namespace := ""
	//Remember the namespace to fully qualify names later
	if namespaceJSON, ok := jsonMap["namespace"]; ok {
		if namespaceStr, ok := namespaceJSON.(string); ok {
			// and it's value is string (otherwise invalid schema)
			namespace = namespaceStr
		}
	}

	for k, v := range jsonMap {

		// Reduce primitive schemas to their simple form.
		if len(jsonMap) == 1 && k == "type" {
			if t, ok := v.(string); ok {
				return "\"" + t + "\""
			}
		}

		// Only keep relevant attributes (strip 'doc', 'alias' or 'namespace')
		if _, ok := fieldOrder[k]; !ok {
			continue
		}

		// Add namespace to a non-qualified name.
		if k == "name" && namespace != "" {
			// Check if the name isn't already qualified.
			if t, ok := v.(string); ok && !strings.Contains(t, ".") {
				v = namespace + "." + t
			}
		}

		// Only fixed type allows size, and we must convert a string size to a
		// float.
		if k == "size" {
			if s, ok := v.(string); ok {
				s, err := strconv.Atoi(s)
				if err != nil {
					panic(fmt.Errorf("Fixed size ought to be number greater than zero: %v", s))
				}
				v = float64(s)
			}
		}

		els = append(els, stringPair{k, proc(k) + ":" + proc(v)})
	}

	// Sort keys by their order in specification.
	sort.Sort(byAvroFieldOrder(els))
	return "{" + strings.Join(els.Bs(), ",") + "}"
}

// stringPair represents a pair of string values.
type stringPair struct {
	A string
	B string
}

// stringPairs is a sortable array of pair of strings.
type stringPairs []stringPair

// Bs returns an array of second values of an array of pairs.
func (sp *stringPairs) Bs() []string {
	out := make([]string, len(*sp))
	for i, el := range *sp {
		out[i] = el.B
	}
	return out
}

// fieldOrder defines fields that show up in canonical schema and specifies
// their precedence.
var fieldOrder = map[string]int{
	"name":    1,
	"type":    2,
	"fields":  3,
	"symbols": 4,
	"items":   5,
	"values":  6,
	"size":    7,
}

// byAvroFieldOrder is equipped with a sort order of fields according to the
// specification.
type byAvroFieldOrder []stringPair

func (s byAvroFieldOrder) Len() int {
	return len(s)
}

func (s byAvroFieldOrder) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s byAvroFieldOrder) Less(i, j int) bool {
	return fieldOrder[s[i].A] < fieldOrder[s[j].A]
}
