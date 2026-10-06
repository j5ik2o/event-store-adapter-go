// Package conformance is the foundation of the runner for the conformance data in conformance/.
//
// It loads and validates the data, verifies manifest.json, classifies every case,
// evaluates required.json and writes conformance-report.json. It does not execute
// scenarios and is not connected to any store yet, so no case is reported as a success.
package conformance
