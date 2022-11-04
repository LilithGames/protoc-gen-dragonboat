package runtime

import (
	"errors"
)

var ErrUnknownRequest error = errors.New("ErrUnknownRequest")

const ErrCodeOK int32 = 0
const ErrCodeBadReqeust int32 = 400
const ErrCodeInternal int32 = 500

func NewDragonboatError(code int32, msg string) error {
	if code == ErrCodeOK {
		return nil
	}
	return &DragonboatError{Code: code, Msg: msg}
}
func (it *DragonboatError) Error() string {
	return it.Msg
}

func GetDragonboatErrorCode(err error) int32 {
	if err == nil {
		return ErrCodeOK
	}
	var derr *DragonboatError
	if errors.As(err, &derr) {
		if derr == nil {
			return ErrCodeOK
		}
		return derr.Code
	} else {
		return ErrCodeInternal
	}
}
