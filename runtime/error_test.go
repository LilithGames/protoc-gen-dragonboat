package runtime

import (
	"fmt"
	"testing"
	"errors"

	"github.com/stretchr/testify/assert"
)

func TestErrorAs(t *testing.T) {
	err := NewDragonboatError(99, "hello")
	err = fmt.Errorf("wrap1: %w", err)
	err = fmt.Errorf("wrap2: %w", err)
	var berr *DragonboatError
	if errors.As(err, &berr) {
		fmt.Printf("%+v\n", berr.Code)
		assert.Equal(t, berr.Code, int32(99))
	}
}

func TestDragonboatError1(t *testing.T) {
	r := MakeDragonboatResult(nil, fmt.Errorf("wrap1: %w", NewDragonboatError(500, "fatal")))
	v, err := ParseDragonboatResult(r)
	assert.Nil(t, v)
	fmt.Printf("%+v\n", err)
	assert.Equal(t, err.(*DragonboatError).Code, int32(500))
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
	r := MakeDragonboatResult(&DragonboatExample{Data: "data1"}, NewDragonboatError(500, "partial"))
	v, err := ParseDragonboatResult(r)
	assert.Equal(t, err.(*DragonboatError).Code, int32(500))
	assert.Equal(t, v.(*DragonboatExample).Data, "data1")
}
