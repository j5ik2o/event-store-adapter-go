// Package conformance is the foundation of the runner for the conformance data in conformance/.
//
// It loads and validates the data, verifies manifest.json, classifies every case,
// evaluates required.json and writes conformance-report.json. It runs scenarios
// through the Backend and Store boundary (backend.go) and injects faults through internal/testhook.
// ID and sequence-number value cases execute against the eventstore core. No storage
// backend is connected yet; storage scenarios and time roundtrips remain unverified.
package conformance
