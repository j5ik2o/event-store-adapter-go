package eventstore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type userID struct {
	typeName string
	value    string
}

func (id userID) TypeName() string { return id.typeName }
func (id userID) Value() string    { return id.value }
func (id userID) String() string   { panic("caller-defined String must not be used") }
func (id userID) AsString() string { panic("caller-defined AsString must not be used") }

func TestAggregateIDFormatAndEmptyParts(t *testing.T) {
	for _, tc := range []struct{ typeName, value, want string }{
		{"Order", "123", "Order-123"}, {"", "値", "-値"}, {"型", "", "型-"}, {"", "", "-"}, {"order", "item-1", "order-item-1"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			got, err := AidString(userID{tc.typeName, tc.value})
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			id, err := NewAggregateID(tc.typeName, tc.value)
			require.NoError(t, err)
			assert.Equal(t, tc.typeName, id.TypeName())
			assert.Equal(t, tc.value, id.Value())
			got, err = AidString(id)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestAggregateIDHyphenType(t *testing.T) {
	for _, typeName := range []string{"-", "order-item"} {
		aid, err := AidString(userID{typeName, "1"})
		assert.Empty(t, aid)
		requireContractViolation(t, err, "T-11")
		id, err := NewAggregateID(typeName, "1")
		assert.Nil(t, id)
		requireContractViolation(t, err, "T-11")
	}
}

func TestAggregateIDUTF8ByteBoundaries(t *testing.T) {
	for _, multibyte := range []bool{false, true} {
		for _, size := range []int{1023, 1024, 1025} {
			t.Run(fmt.Sprintf("multibyte=%t/bytes=%d", multibyte, size), func(t *testing.T) {
				typeName, value := "T", strings.Repeat("a", size-2)
				if multibyte {
					typeName = "型"
					bytes := size - len(typeName) - 1
					value = strings.Repeat("あ", bytes/3) + strings.Repeat("a", bytes%3)
				}
				want := typeName + "-" + value
				require.Len(t, want, size)
				aid, err := AidString(userID{typeName, value})
				id, constructorErr := NewAggregateID(typeName, value)
				if size > 1024 {
					assert.Empty(t, aid)
					assert.Nil(t, id)
					requireContractViolation(t, err, "T-12")
					requireContractViolation(t, constructorErr, "T-12")
					return
				}
				require.NoError(t, err)
				require.NoError(t, constructorErr)
				assert.Equal(t, want, aid)
				aid, err = AidString(id)
				require.NoError(t, err)
				assert.Equal(t, want, aid)
			})
		}
	}
}

type nilPointerID struct{}

func (*nilPointerID) TypeName() string { panic("nil ID method called") }
func (*nilPointerID) Value() string    { panic("nil ID method called") }

type nilMapID map[string]string

func (nilMapID) TypeName() string { panic("nil ID method called") }
func (nilMapID) Value() string    { panic("nil ID method called") }

type nilSliceID []string

func (nilSliceID) TypeName() string { panic("nil ID method called") }
func (nilSliceID) Value() string    { panic("nil ID method called") }

type nilFuncID func()

func (nilFuncID) TypeName() string { panic("nil ID method called") }
func (nilFuncID) Value() string    { panic("nil ID method called") }

type nilChanID chan string

func (nilChanID) TypeName() string { panic("nil ID method called") }
func (nilChanID) Value() string    { panic("nil ID method called") }

func TestAidStringNil(t *testing.T) {
	for _, id := range []AggregateID{nil, (*nilPointerID)(nil), nilMapID(nil), nilSliceID(nil), nilFuncID(nil), nilChanID(nil)} {
		t.Run(fmt.Sprintf("%T", id), func(t *testing.T) {
			aid, err := AidString(id)
			assert.Empty(t, aid)
			requireContractViolation(t, err, "T-2")
		})
	}
}
