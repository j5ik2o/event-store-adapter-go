// Package testhook holds the hook types and registration points that the conformance runner
// uses to inject clocks, waits and faults into a store.
//
// The types use only aid strings, seq_nr values and bytes, and depend on the standard library
// alone. Public packages may import testhook; testhook never imports them or the runner.
package testhook
