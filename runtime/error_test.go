package runtime

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorAs(t *testing.T) {
	err := NewDragonboatError(99, "hello")
	err = fmt.Errorf("wrap1: %w", err)
	err = fmt.Errorf("wrap2: %w", err)
	assert.Equal(t, GetDragonboatErrorCode(err), int32(99))
}

func TestDragonboatError1(t *testing.T) {
	r := MakeDragonboatResult(nil, fmt.Errorf("wrap1: %w", NewDragonboatError(ErrCodeInternal, "fatal")))
	v, err := ParseDragonboatResult(r)
	assert.Nil(t, v)
	assert.Equal(t, GetDragonboatErrorCode(err), ErrCodeInternal)
}

func TestDragonboatError2(t *testing.T) {
	r := MakeDragonboatResult(nil, nil)
	v, err := ParseDragonboatResult(r)
	assert.Nil(t, v)
	assert.Nil(t, err)
}

func TestDragonboatError3(t *testing.T) {
	r := MakeDragonboatResult(&DragonboatExample{Data: "data1"}, nil)
	v, err := ParseDragonboatResult(r)
	assert.Nil(t, err)
	assert.Equal(t, v.(*DragonboatExample).Data, "data1")
}

func TestDragonboatError4(t *testing.T) {
	r := MakeDragonboatResult(&DragonboatExample{Data: "data1"}, NewDragonboatError(ErrCodeInternal, "partial"))
	assert.Equal(t, int32(r.Value), ErrCodeInternal)
	v, err := ParseDragonboatResult(r)
	assert.Equal(t, GetDragonboatErrorCode(err), ErrCodeInternal)
	assert.Equal(t, v.(*DragonboatExample).Data, "data1")
}

func TestGetDragonboatErrorCode(t *testing.T) {
	code1 := GetDragonboatErrorCode(nil)
	assert.Equal(t, code1, ErrCodeOK)
	code2 := GetDragonboatErrorCode(fmt.Errorf("hello"))
	assert.Equal(t, code2, ErrCodeInternal)
	code3 := GetDragonboatErrorCode(NewDragonboatError(999, "hello"))
	assert.Equal(t, code3, int32(999))
	code4 := GetDragonboatErrorCode(fmt.Errorf("wrap: %w", NewDragonboatError(401, "hello")))
	assert.Equal(t, code4, int32(401))
	code5 := GetDragonboatErrorCode(fmt.Errorf("wrap: %w", fmt.Errorf("hello")))
	assert.Equal(t, code5, ErrCodeInternal)
	code6 := GetDragonboatErrorCode(fmt.Errorf("wrap: %w", nil))
	assert.Equal(t, code6, ErrCodeInternal)
	var err *DragonboatError
	code7 := GetDragonboatErrorCode(err)
	assert.Equal(t, code7, ErrCodeOK)
}
