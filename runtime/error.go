package runtime

import (
	"errors"
)

var ErrUnknownRequest error = errors.New("ErrUnknownRequest")

const CodeInternalError int32 = 500

func NewDragonboatError(code int32, msg string) error {
	return &DragonboatError{Code: code, Msg: msg}
}
func (it *DragonboatError) Error() string {
	return it.Msg
}
