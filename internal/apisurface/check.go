package apisurface

import (
	"fmt"
	"go/types"
	"slices"
	"sort"
	"strings"
)

// Problem is one difference between the surface and the Go package.
type Problem struct {
	// Path locates the surface item, for example "event_envelope.occurred_at".
	Path    string
	Message string
}

func (p Problem) String() string { return p.Path + ": " + p.Message }

// Check compares pkg with the surface using the mapping and returns every
// difference found. A nil result means the package matches. Items that the
// surface defines but the mapping does not cover are reported as problems
// instead of being skipped.
func Check(pkg *types.Package, s *Surface, m *Mapping) []Problem {
	c := &checker{pkg: pkg, s: s, m: m}
	c.checkTypes()
	c.checkStore()
	c.checkErrors()
	return c.problems
}

type checker struct {
	pkg      *types.Package
	s        *Surface
	m        *Mapping
	problems []Problem
}

func (c *checker) report(path, format string, args ...any) {
	c.problems = append(c.problems, Problem{Path: path, Message: fmt.Sprintf(format, args...)})
}

func (c *checker) typeString(t types.Type) string {
	return types.TypeString(t, func(p *types.Package) string {
		if p == c.pkg {
			return ""
		}
		return p.Name()
	})
}

// exprNode is a parsed surface type expression such as list<event_envelope<P>>.
type exprNode struct {
	name string
	args []exprNode
}

func parseExpr(src string) (exprNode, error) {
	n, rest, err := parseNode(strings.TrimSpace(src))
	if err != nil {
		return exprNode{}, fmt.Errorf("type expression %q: %w", src, err)
	}
	if strings.TrimSpace(rest) != "" {
		return exprNode{}, fmt.Errorf("type expression %q: unexpected %q", src, rest)
	}
	return n, nil
}

func parseNode(s string) (exprNode, string, error) {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) && (s[i] == '_' || s[i] >= 'a' && s[i] <= 'z' || s[i] >= 'A' && s[i] <= 'Z' || s[i] >= '0' && s[i] <= '9') {
		i++
	}
	if i == 0 {
		return exprNode{}, "", fmt.Errorf("identifier expected at %q", s)
	}
	n := exprNode{name: s[:i]}
	rest := strings.TrimSpace(s[i:])
	if !strings.HasPrefix(rest, "<") {
		return n, rest, nil
	}
	rest = rest[1:]
	for {
		arg, r, err := parseNode(rest)
		if err != nil {
			return exprNode{}, "", err
		}
		n.args = append(n.args, arg)
		rest = strings.TrimSpace(r)
		switch {
		case strings.HasPrefix(rest, ","):
			rest = rest[1:]
		case strings.HasPrefix(rest, ">"):
			return n, rest[1:], nil
		default:
			return exprNode{}, "", fmt.Errorf("',' or '>' expected at %q", rest)
		}
	}
}

// goType renders a surface type expression as the Go type the mapping
// prescribes. input selects the form used for arguments instead of results.
func (c *checker) goType(expr string, input bool) (string, error) {
	n, err := parseExpr(expr)
	if err != nil {
		return "", err
	}
	return c.render(n, input)
}

func (c *checker) render(n exprNode, input bool) (string, error) {
	if tmpl, ok := c.m.Containers[n.name]; ok && (n.name == "optional" || n.name == "list") {
		if len(n.args) != 1 {
			return "", fmt.Errorf("%s<> takes one argument", n.name)
		}
		inner, err := c.render(n.args[0], input)
		if err != nil {
			return "", err
		}
		return strings.ReplaceAll(tmpl, "{T}", inner), nil
	}
	if n.name == "optional" || n.name == "list" {
		return "", fmt.Errorf("go.json has no containers.%s", n.name)
	}
	if _, ok := c.s.TypeParameters[n.name]; ok {
		g, ok := c.m.TypeParams[n.name]
		if !ok {
			return "", fmt.Errorf("go.json has no type_parameters.%s", n.name)
		}
		return g, nil
	}
	if _, ok := c.s.ValueTypes[n.name]; ok {
		v, ok := c.m.ValueTypes[n.name]
		if !ok {
			return "", fmt.Errorf("go.json has no value_types.%s", n.name)
		}
		if input {
			return v.Input, nil
		}
		return v.Output, nil
	}
	if tm, ok := c.m.Types[n.name]; ok {
		if len(n.args) == 0 {
			return tm.Name, nil
		}
		args := make([]string, len(n.args))
		for i, a := range n.args {
			g, err := c.render(a, input)
			if err != nil {
				return "", err
			}
			args[i] = g
		}
		return tm.Name + "[" + strings.Join(args, ", ") + "]", nil
	}
	return "", fmt.Errorf("go.json has no mapping for %q", n.name)
}

func (c *checker) typeParamNames(surfaceParams []string, path string) ([]string, bool) {
	out := make([]string, len(surfaceParams))
	for i, p := range surfaceParams {
		g, ok := c.m.TypeParams[p]
		if !ok {
			c.report(path, "go.json has no type_parameters.%s", p)
			return nil, false
		}
		out[i] = g
	}
	return out, true
}

func namedTypeParams(n *types.Named) []string {
	var out []string
	for i := 0; i < n.TypeParams().Len(); i++ {
		out = append(out, n.TypeParams().At(i).Obj().Name())
	}
	return out
}

func (c *checker) lookupNamed(path, goName string) *types.Named {
	obj := c.pkg.Scope().Lookup(goName)
	if obj == nil {
		c.report(path, "type %s not found in package %s", goName, c.pkg.Name())
		return nil
	}
	tn, ok := obj.(*types.TypeName)
	if !ok {
		c.report(path, "%s is %s, want a type", goName, objKind(obj))
		return nil
	}
	named, ok := tn.Type().(*types.Named)
	if !ok {
		c.report(path, "%s is not a defined type", goName)
		return nil
	}
	return named
}

func objKind(obj types.Object) string {
	switch obj.(type) {
	case *types.Func:
		return "a function"
	case *types.Var:
		return "a variable"
	case *types.Const:
		return "a constant"
	default:
		return "not a type"
	}
}

func (c *checker) checkTypes() {
	for _, st := range c.s.Types {
		tm, ok := c.m.Types[st.Name]
		if !ok {
			c.report(st.Name, "go.json has no mapping for type %s", st.Name)
			continue
		}
		named := c.lookupNamed(st.Name, tm.Name)
		if named == nil {
			continue
		}
		wantParams, ok := c.typeParamNames(st.TypeParameters, st.Name)
		if ok && !slices.Equal(namedTypeParams(named), wantParams) {
			c.report(st.Name, "%s has type parameters %v, want %v", tm.Name, namedTypeParams(named), wantParams)
		}
		for _, el := range st.Elements {
			c.checkElement(st, tm, named, el)
		}
		c.checkConstructor(st, tm, named)
	}
}

func (c *checker) checkElement(st SurfaceType, tm TypeMap, named *types.Named, el SurfaceElement) {
	path := st.Name + "." + el.Name
	goName, ok := tm.Elements[el.Name]
	if !ok {
		c.report(path, "go.json has no mapping for element %s", el.Name)
		return
	}
	want, err := c.goType(el.Type, false)
	if err != nil {
		c.report(path, "%v", err)
		return
	}
	obj, _, _ := types.LookupFieldOrMethod(named, true, c.pkg, goName)
	switch tm.Access {
	case "method":
		fn, ok := obj.(*types.Func)
		if !ok {
			c.report(path, "%s has no method %s (want %s() %s)", tm.Name, goName, goName, want)
			return
		}
		sig := fn.Type().(*types.Signature)
		if sig.Params().Len() != 0 || sig.Results().Len() != 1 {
			c.report(path, "%s.%s is func%s, want %s() %s", tm.Name, goName, c.signatureString(sig), goName, want)
			return
		}
		if got := c.typeString(sig.Results().At(0).Type()); got != want {
			c.report(path, "%s.%s returns %s, want %s", tm.Name, goName, got, want)
		}
	case "field":
		v, ok := obj.(*types.Var)
		if !ok || !v.IsField() {
			c.report(path, "%s has no field %s (want %s %s)", tm.Name, goName, goName, want)
			return
		}
		if got := c.typeString(v.Type()); got != want {
			c.report(path, "%s.%s has type %s, want %s", tm.Name, goName, got, want)
		}
	default:
		c.report(st.Name, "go.json access must be \"method\" or \"field\", got %q", tm.Access)
	}
}

func (c *checker) checkConstructor(st SurfaceType, tm TypeMap, named *types.Named) {
	needs := false
	for _, el := range st.Elements {
		if el.Required != nil {
			needs = true
		}
	}
	path := st.Name + " constructor"
	if tm.Constructor == nil {
		if needs {
			c.report(path, "go.json has no constructor mapping for %s", st.Name)
		}
		return
	}
	if !needs {
		return
	}
	cm := tm.Constructor
	elems := map[string]SurfaceElement{}
	for _, el := range st.Elements {
		elems[el.Name] = el
	}

	// The mapping itself must agree with the surface about what is required.
	for _, p := range cm.Params {
		if el, ok := elems[p]; !ok {
			c.report(path, "go.json lists %s as a parameter of %s, but the surface has no such element", p, cm.Name)
		} else if el.Required == nil || !*el.Required {
			c.report(path, "go.json makes optional element %s a required parameter of %s", p, cm.Name)
		}
	}
	var optional []string
	for _, el := range st.Elements {
		if el.Required == nil {
			continue
		}
		if *el.Required {
			if !slices.Contains(cm.Params, el.Name) {
				c.report(path, "go.json does not list required element %s as a parameter of %s", el.Name, cm.Name)
			}
		} else {
			optional = append(optional, el.Name)
			if _, ok := cm.Options.Elements[el.Name]; !ok {
				c.report(path, "go.json maps optional element %s to no option of %s", el.Name, cm.Name)
			}
		}
	}

	obj := c.pkg.Scope().Lookup(cm.Name)
	fn, ok := obj.(*types.Func)
	if !ok {
		c.report(path, "function %s not found in package %s", cm.Name, c.pkg.Name())
		return
	}
	sig := fn.Type().(*types.Signature)

	// Parameters: required elements in the mapping's order, then the options.
	var want []string
	for _, p := range cm.Params {
		el, ok := elems[p]
		if !ok {
			continue
		}
		g, err := c.goType(el.Type, true)
		if err != nil {
			c.report(path, "%v", err)
			return
		}
		want = append(want, g)
	}
	want = append(want, "..."+cm.Options.Type)
	var got []string
	for i := 0; i < sig.Params().Len(); i++ {
		t := sig.Params().At(i).Type()
		if sig.Variadic() && i == sig.Params().Len()-1 {
			t = t.(*types.Slice).Elem()
			got = append(got, "..."+c.typeString(t))
		} else {
			got = append(got, c.typeString(t))
		}
	}
	if !slices.Equal(got, want) {
		c.report(path, "%s parameters are (%s), want (%s)%s", cm.Name,
			strings.Join(got, ", "), strings.Join(want, ", "), c.missingHint(cm, elems, got))
	}

	// Results: the constructed type, then the Go-idiom trailing results.
	wantRes := []string{c.constructedType(tm, named)}
	wantRes = append(wantRes, c.m.Conventions.TrailingResults...)
	var gotRes []string
	for i := 0; i < sig.Results().Len(); i++ {
		gotRes = append(gotRes, c.typeString(sig.Results().At(i).Type()))
	}
	if !slices.Equal(gotRes, wantRes) {
		c.report(path, "%s returns (%s), want (%s)", cm.Name, strings.Join(gotRes, ", "), strings.Join(wantRes, ", "))
	}

	// Each optional element must have an option function.
	sort.Strings(optional)
	for _, name := range optional {
		fnName, ok := cm.Options.Elements[name]
		if !ok {
			continue
		}
		c.checkOption(st.Name+"."+name, fnName, cm.Options.Type, elems[name])
	}
}

func (c *checker) constructedType(tm TypeMap, named *types.Named) string {
	if named.TypeParams().Len() == 0 {
		return tm.Name
	}
	return tm.Name + "[" + strings.Join(namedTypeParams(named), ", ") + "]"
}

// missingHint names the required elements whose type no parameter accepts.
func (c *checker) missingHint(cm *ConstructorMap, elems map[string]SurfaceElement, got []string) string {
	var missing []string
	for _, p := range cm.Params {
		el, ok := elems[p]
		if !ok {
			continue
		}
		g, err := c.goType(el.Type, true)
		if err != nil || slices.Contains(got, g) {
			continue
		}
		missing = append(missing, fmt.Sprintf("%s (%s)", p, g))
	}
	if len(missing) == 0 {
		return ""
	}
	return "; required element not accepted: " + strings.Join(missing, ", ")
}

func (c *checker) checkOption(path, fnName, optType string, el SurfaceElement) {
	fn, ok := c.pkg.Scope().Lookup(fnName).(*types.Func)
	if !ok {
		c.report(path, "option function %s not found in package %s", fnName, c.pkg.Name())
		return
	}
	in, err := c.goType(el.Type, true)
	if err != nil {
		c.report(path, "%v", err)
		return
	}
	sig := fn.Type().(*types.Signature)
	if sig.Params().Len() != 1 || sig.Results().Len() != 1 ||
		c.typeString(sig.Params().At(0).Type()) != in || c.typeString(sig.Results().At(0).Type()) != optType {
		c.report(path, "%s is func%s, want func(%s) %s", fnName, c.signatureString(sig), in, optType)
	}
}

func (c *checker) checkStore() {
	path := "store"
	sm := c.m.Store
	named := c.lookupNamed(path, sm.Name)
	if named == nil {
		return
	}
	iface, ok := named.Underlying().(*types.Interface)
	if !ok {
		c.report(path, "%s is not an interface", sm.Name)
		return
	}
	wantParams, ok := c.typeParamNames(c.s.Store.TypeParameters, path)
	if ok && !slices.Equal(namedTypeParams(named), wantParams) {
		c.report(path, "%s has type parameters %v, want %v", sm.Name, namedTypeParams(named), wantParams)
	}
	for _, op := range c.s.Store.Operations {
		opPath := "store." + op.Name
		goName, ok := sm.Operations[op.Name]
		if !ok {
			c.report(opPath, "go.json has no mapping for operation %s", op.Name)
			continue
		}
		want, err := c.wantOperation(op)
		if err != nil {
			c.report(opPath, "%v", err)
			continue
		}
		var method *types.Func
		for i := 0; i < iface.NumMethods(); i++ {
			if iface.Method(i).Name() == goName {
				method = iface.Method(i)
			}
		}
		if method == nil {
			c.report(opPath, "%s has no method %s (want %s)", sm.Name, goName, want)
			continue
		}
		if got := c.signatureString(method.Type().(*types.Signature)); got != want {
			c.report(opPath, "%s.%s is func%s, want func%s", sm.Name, goName, got, want)
		}
	}
}

// signatureString renders a signature without parameter names, in the same
// form as wantOperation, for example "(context.Context, SeqNr) error".
func (c *checker) signatureString(sig *types.Signature) string {
	var params, results []string
	for i := 0; i < sig.Params().Len(); i++ {
		params = append(params, c.typeString(sig.Params().At(i).Type()))
	}
	for i := 0; i < sig.Results().Len(); i++ {
		results = append(results, c.typeString(sig.Results().At(i).Type()))
	}
	return joinSignature(params, results)
}

func joinSignature(params, results []string) string {
	res := strings.Join(results, ", ")
	if len(results) > 1 {
		res = "(" + res + ")"
	}
	return "(" + strings.Join(params, ", ") + ") " + res
}

// wantOperation renders the expected Go signature, for example
// "(context.Context, SeqNr) ([]EventEnvelope[E], error)".
func (c *checker) wantOperation(op SurfaceOperation) (string, error) {
	params := slices.Clone(c.m.Conventions.LeadingParams)
	for _, in := range op.Inputs {
		g, err := c.goType(in.Type, true)
		if err != nil {
			return "", err
		}
		params = append(params, g)
	}
	var results []string
	if op.Output != "none" {
		g, err := c.goType(op.Output, false)
		if err != nil {
			return "", err
		}
		results = append(results, g)
	}
	results = append(results, c.m.Conventions.TrailingResults...)
	return joinSignature(params, results), nil
}

func (c *checker) checkErrors() {
	em := c.m.Errors
	var iface *types.Interface
	if named := c.lookupNamed("error_categories", em.Interface); named != nil {
		if i, ok := named.Underlying().(*types.Interface); ok {
			iface = i
		} else {
			c.report("error_categories", "%s is not an interface", em.Interface)
		}
	}
	for _, name := range c.s.ErrorCategories.Names {
		path := "error_categories." + name
		goName, ok := em.Names[name]
		if !ok {
			c.report(path, "go.json has no mapping for error category %s", name)
			continue
		}
		named := c.lookupNamed(path, goName)
		if named == nil || iface == nil {
			continue
		}
		if !types.Implements(named, iface) && !types.Implements(types.NewPointer(named), iface) {
			c.report(path, "%s does not implement %s", goName, em.Interface)
		}
	}
}
