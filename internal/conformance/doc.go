// Package conformance is the foundation of the runner for the conformance data in conformance/.
//
// It loads and validates the data, verifies manifest.json, classifies every case,
// evaluates required.json and writes conformance-report.json. It runs scenarios
// through the Backend and Store boundary (backend.go) and injects faults through internal/testhook.
// No backend is connected yet, so no case is reported as a success.
package conformance
