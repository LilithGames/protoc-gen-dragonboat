package runtime

func NewDragonboatError(code int32, msg string) error {
	return &DragonboatError{Code: code, Msg: msg}
}
func (it *DragonboatError) Error() string {
	return it.Msg
}
