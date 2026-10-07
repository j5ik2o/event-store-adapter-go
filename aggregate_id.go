package eventstore

import (
	"reflect"
	"strings"
)

// AggregateID supplies the type name and value used to build the T-1 aid string.
// The library does not use caller-defined String or AsString representations.
type AggregateID interface {
	TypeName() string
	Value() string
}

type aggregateID struct {
	typeName string
	value    string
}

func (id aggregateID) TypeName() string { return id.typeName }
func (id aggregateID) Value() string    { return id.value }

// NewAggregateID creates an ID after the same validation used by AidString.
// Empty type names and values are permitted.
func NewAggregateID(typeName, value string) (AggregateID, error) {
	id := aggregateID{typeName: typeName, value: value}
	if _, err := AidString(id); err != nil {
		return nil, err
	}
	return id, nil
}

// AidString builds {type name}-{value}. It rejects nil IDs under T-2,
// hyphens in the type name under T-11, and more than 1024 UTF-8 bytes under T-12.
func AidString(id AggregateID) (string, error) {
	if id == nil {
		return "", &ContractViolationError{Rule: "T-2"}
	}
	// An interface can hold a typed nil. Check before invoking either ID method.
	v := reflect.ValueOf(id)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return "", &ContractViolationError{Rule: "T-2"}
		}
	}
	typeName := id.TypeName()
	if strings.Contains(typeName, "-") {
		return "", &ContractViolationError{Rule: "T-11"}
	}
	aid := typeName + "-" + id.Value()
	if len(aid) > 1024 {
		return "", &ContractViolationError{Rule: "T-12"}
	}
	return aid, nil
}
