package conformance

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

// ManifestResult is the outcome of comparing manifest.json with the actual files.
type ManifestResult struct {
	Version    string   `json:"version"`
	Verified   bool     `json:"verified"`
	FileCount  int      `json:"file_count"`
	Mismatches []string `json:"mismatches"`
}

type manifestEntry struct {
	Path   string
	SHA256 string
}

// VerifyManifest compares root/manifest.json with the files under root, following
// tools/conformance/manifest.py. Differences are reported as Mismatches; an error is
// returned only when the comparison itself cannot be done.
func VerifyManifest(root string) (ManifestResult, error) {
	res := ManifestResult{Version: DataVersion, Mismatches: []string{}}

	actual, symlinks, err := inventory(root)
	if err != nil {
		return res, err
	}
	res.FileCount = len(actual)
	for _, s := range symlinks {
		res.Mismatches = append(res.Mismatches, s+": symbolic links are not allowed")
	}

	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		return res, fmt.Errorf("read manifest.json: %w", err)
	}
	doc, err := decodeStrictJSON(raw)
	if err != nil {
		return res, fmt.Errorf("manifest.json: %w", err)
	}
	obj, ok := doc.(map[string]any)
	if !ok {
		return res, fmt.Errorf("manifest.json: top level is not an object")
	}
	if obj["format"] != "manifest" {
		res.Mismatches = append(res.Mismatches, fmt.Sprintf("manifest.json: format is %v, want manifest", obj["format"]))
	}
	if obj["version"] != DataVersion {
		res.Mismatches = append(res.Mismatches, fmt.Sprintf("manifest.json: version is %v, want %s", obj["version"], DataVersion))
	}
	list, ok := obj["files"].([]any)
	if !ok {
		return res, fmt.Errorf("manifest.json: files is not an array")
	}
	declared := make([]manifestEntry, 0, len(list))
	for i, item := range list {
		e, ok := item.(map[string]any)
		if !ok {
			return res, fmt.Errorf("manifest.json: files[%d] is not an object", i)
		}
		p, pok := e["path"].(string)
		h, hok := e["sha256"].(string)
		if !pok || !hok {
			return res, fmt.Errorf("manifest.json: files[%d] needs string path and sha256", i)
		}
		declared = append(declared, manifestEntry{Path: p, SHA256: h})
	}

	declaredByPath := map[string]string{}
	for _, e := range declared {
		if _, dup := declaredByPath[e.Path]; dup {
			res.Mismatches = append(res.Mismatches, e.Path+": listed more than once in manifest.json")
			continue
		}
		declaredByPath[e.Path] = e.SHA256
	}
	if !sort.SliceIsSorted(declared, func(i, j int) bool { return declared[i].Path < declared[j].Path }) {
		res.Mismatches = append(res.Mismatches, "manifest.json: files are not in ascending path order")
	}

	actualByPath := map[string]string{}
	for _, e := range actual {
		actualByPath[e.Path] = e.SHA256
	}
	for _, e := range actual {
		want, listed := declaredByPath[e.Path]
		switch {
		case !listed:
			res.Mismatches = append(res.Mismatches, e.Path+": file is not listed in manifest.json")
		case want != e.SHA256:
			res.Mismatches = append(res.Mismatches, e.Path+": sha256 differs from manifest.json")
		}
	}
	paths := make([]string, 0, len(declaredByPath))
	for p := range declaredByPath {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		if _, present := actualByPath[p]; !present {
			res.Mismatches = append(res.Mismatches, p+": listed in manifest.json but the file is missing")
		}
	}

	res.Verified = len(res.Mismatches) == 0
	return res, nil
}

// inventory lists every file under root except manifest.json with the SHA-256 of its raw bytes.
func inventory(root string) ([]manifestEntry, []string, error) {
	var files []manifestEntry
	var symlinks []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.Type()&fs.ModeSymlink != 0 {
			symlinks = append(symlinks, rel)
			return nil
		}
		if d.IsDir() || rel == "manifest.json" {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		files = append(files, manifestEntry{Path: rel, SHA256: hex.EncodeToString(sum[:])})
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("walk %s: %w", root, err)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, symlinks, nil
}
