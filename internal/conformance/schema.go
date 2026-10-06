package conformance

import (
	"fmt"

	"github.com/dlclark/regexp2"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// ecmaRegexp adapts regexp2 in ECMAScript mode to the regular expression engine of the schema
// compiler. JSON Schema patterns use ECMA-262 syntax (for example, negative lookahead in
// manifest.schema.json), which the standard regexp package cannot handle.
type ecmaRegexp regexp2.Regexp

func compileECMARegexp(s string) (jsonschema.Regexp, error) {
	re, err := regexp2.Compile(s, regexp2.ECMAScript)
	if err != nil {
		return nil, err
	}
	return (*ecmaRegexp)(re), nil
}

func (r *ecmaRegexp) MatchString(s string) bool {
	ok, err := (*regexp2.Regexp)(r).MatchString(s)
	return err == nil && ok
}

func (r *ecmaRegexp) String() string {
	return (*regexp2.Regexp)(r).String()
}

// schemaSet holds the compiled schemas of conformance/schema, keyed by data format.
type schemaSet struct {
	compiler *jsonschema.Compiler
	byFormat map[string]*jsonschema.Schema
}

// newSchemaSet registers every schema document by its $id, so $ref is resolved offline,
// and compiles the schema of each format. docs maps a path under schema/ to the decoded document.
func newSchemaSet(docs map[string]any, formats []string) (*schemaSet, error) {
	c := jsonschema.NewCompiler()
	c.UseRegexpEngine(compileECMARegexp)
	for rel, doc := range docs {
		obj, ok := doc.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s: top level is not an object", rel)
		}
		id, ok := obj["$id"].(string)
		if !ok || id == "" {
			return nil, fmt.Errorf("%s: schema has no $id", rel)
		}
		if err := c.AddResource(id, doc); err != nil {
			return nil, fmt.Errorf("%s: %w", rel, err)
		}
	}
	set := &schemaSet{compiler: c, byFormat: map[string]*jsonschema.Schema{}}
	for _, f := range formats {
		rel := "schema/" + f + ".schema.json"
		obj, ok := docs[rel].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s: schema file is missing", rel)
		}
		s, err := c.Compile(obj["$id"].(string))
		if err != nil {
			return nil, fmt.Errorf("%s: compile: %w", rel, err)
		}
		set.byFormat[f] = s
	}
	return set, nil
}

// validate checks doc against the schema of format.
func (s *schemaSet) validate(format string, doc any) error {
	sch, ok := s.byFormat[format]
	if !ok {
		return fmt.Errorf("no schema for format %q", format)
	}
	return sch.Validate(doc)
}
