package eventstore

// EnvelopeOption specifies optional envelope metadata.
type EnvelopeOption struct {
	manifest string
}

// WithManifest supplies an uninterpreted manifest. Omission means an empty string.
// When supplied more than once, the last option takes effect.
func WithManifest(manifest string) EnvelopeOption {
	return EnvelopeOption{manifest: manifest}
}
