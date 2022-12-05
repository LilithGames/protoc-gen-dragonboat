package runtime

import (
	"errors"
)

const ErrCodeOK int32 = 0
const ErrCodeBadReqeust int32 = 400
const ErrCodeUnknownRequest int32 = 450
const ErrCodeMigrationInvalid int32 = 460
const ErrCodeMigrationExpired int32 = 461
const ErrCodeAlreadyMigrating int32 = 462
const ErrCodeVersionNotMatch  int32 = 463
const ErrCodeInternal int32 = 500
const ErrCodeMigrating int32 = 560

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

