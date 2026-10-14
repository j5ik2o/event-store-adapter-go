package apisurface

import (
	"encoding/json"
	"fmt"
	"os"
)

// Surface is the language-neutral API surface (surface.json).
type Surface struct {
	TypeParameters  map[string]string `json:"type_parameters"`
	ValueTypes      map[string]string `json:"value_types"`
	Types           []SurfaceType     `json:"types"`
	Store           SurfaceStore      `json:"store"`
	ErrorCategories SurfaceErrors     `json:"error_categories"`
}

// SurfaceType describes one data type of the surface.
type SurfaceType struct {
	Name           string           `json:"name"`
	TypeParameters []string         `json:"type_parameters"`
	Elements       []SurfaceElement `json:"elements"`
}

// SurfaceElement is one element of a data type. Required is nil when the
// surface does not declare it (for example in snapshot_read).
type SurfaceElement struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required *bool  `json:"required"`
}

// SurfaceStore describes the store and its operations.
type SurfaceStore struct {
	Name           string             `json:"name"`
	TypeParameters []string           `json:"type_parameters"`
	Operations     []SurfaceOperation `json:"operations"`
}

// SurfaceOperation is one store operation.
type SurfaceOperation struct {
	Name   string         `json:"name"`
	Inputs []SurfaceInput `json:"inputs"`
	Output string         `json:"output"`
}

// SurfaceInput is one input of a store operation.
type SurfaceInput struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// SurfaceErrors lists the error categories.
type SurfaceErrors struct {
	Names []string `json:"names"`
}

// Mapping is the Go-specific mapping (go.json).
type Mapping struct {
	Package     string              `json:"package"`
	Conventions Conventions         `json:"conventions"`
	TypeParams  map[string]string   `json:"type_parameters"`
	ValueTypes  map[string]ValueMap `json:"value_types"`
	Containers  map[string]string   `json:"containers"`
	Types       map[string]TypeMap  `json:"types"`
	Store       StoreMap            `json:"store"`
	Errors      ErrorMap            `json:"error_categories"`
}

// Conventions holds Go idioms added to every operation.
type Conventions struct {
	LeadingParams   []string `json:"leading_params"`
	TrailingResults []string `json:"trailing_results"`
}

// ValueMap maps a value kind to Go types used as input and as output.
type ValueMap struct {
	Input  string `json:"input"`
	Output string `json:"output"`
}

// TypeMap maps a surface data type to a Go type.
type TypeMap struct {
	Name        string            `json:"name"`
	Access      string            `json:"access"`
	Elements    map[string]string `json:"elements"`
	Constructor *ConstructorMap   `json:"constructor"`
}

// ConstructorMap maps the construction of a data type to a Go function.
type ConstructorMap struct {
	Name    string     `json:"name"`
	Params  []string   `json:"params"`
	Options OptionsMap `json:"options"`
}

// OptionsMap maps optional elements to functional options.
type OptionsMap struct {
	Type     string            `json:"type"`
	Elements map[string]string `json:"elements"`
}

// StoreMap maps the store to a Go interface.
type StoreMap struct {
	Name       string            `json:"name"`
	Operations map[string]string `json:"operations"`
}

// ErrorMap maps error categories to Go types.
type ErrorMap struct {
	Interface string            `json:"interface"`
	Names     map[string]string `json:"names"`
}

func loadJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// LoadSurface reads surface.json.
func LoadSurface(path string) (*Surface, error) {
	var s Surface
	if err := loadJSON(path, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// LoadMapping reads go.json.
func LoadMapping(path string) (*Mapping, error) {
	var m Mapping
	if err := loadJSON(path, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
