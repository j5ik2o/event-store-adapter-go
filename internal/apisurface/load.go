package apisurface

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
)

// Loader type-checks Go packages from source. A Loader is not safe for
// concurrent use because the source importer is not.
type Loader struct {
	fset *token.FileSet
	imp  types.Importer
}

// NewLoader creates a Loader.
func NewLoader() *Loader {
	fset := token.NewFileSet()
	return &Loader{fset: fset, imp: importer.ForCompiler(fset, "source", nil)}
}

// Load type-checks the non-test Go files in dir and returns the package.
func (l *Loader) Load(dir string) (*types.Package, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	bp, err := build.Default.ImportDir(abs, 0)
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", dir, err)
	}
	var files []*ast.File
	for _, name := range bp.GoFiles {
		f, err := parser.ParseFile(l.fset, filepath.Join(abs, name), nil, 0)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", name, err)
		}
		files = append(files, f)
	}
	conf := types.Config{Importer: l.imp, Error: nil}
	pkg, err := conf.Check(bp.Name, l.fset, files, nil)
	if err != nil {
		return nil, fmt.Errorf("type-check %s: %w", dir, err)
	}
	return pkg, nil
}
