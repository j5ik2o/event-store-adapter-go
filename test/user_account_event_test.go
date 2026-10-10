package test

// Event metadata belongs to EventEnvelope; the domain payload has no library interface.
type userAccountEvent struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}
