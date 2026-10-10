// Package conformance is the foundation of the runner for the conformance data in conformance/.
//
// It loads and validates the data, verifies manifest.json, classifies every case,
// evaluates required.json and writes conformance-report.json. It runs scenarios
// through the Backend and Store boundary (backend.go) and injects faults through internal/testhook.
// ID and sequence-number values use the eventstore core. RunBackend connects
// storage scenarios, time roundtrips and layout checks to real public adapters
// in the external test package, retaining actual operation and fault records.
package conformance
