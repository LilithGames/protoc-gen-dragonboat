package runtime

import (
	"fmt"
	"errors"

	"google.golang.org/protobuf/proto"
	sm "github.com/lni/dragonboat/v3/statemachine"
	anypb "google.golang.org/protobuf/types/known/anypb"
)

func MakeDragonboatResult(msg proto.Message, err error) sm.Result {
	dr := &DragonboatResult{}
	if msg != nil {
		if data, merr := anypb.New(msg); merr != nil {
			if err != nil {
				err = fmt.Errorf("origin error: %v, anypb.Marshal: %w", err, merr)
			} else {
				err = fmt.Errorf("anypb.Marshal err: %w", merr)
			}
		} else {
			dr.Data = data
		}
	}
	var derr *DragonboatError
	if err == nil {
		dr.Error = nil
	} else if errors.As(err, &derr) {
		dr.Error = &DragonboatError{Code: derr.Code, Msg: err.Error()}
	} else {
		dr.Error = &DragonboatError{Code: 500, Msg: err.Error()}
	}

	if bs, err := proto.Marshal(dr); err != nil {
		panic(fmt.Errorf("proto.Marshal(DragonboatResult) err: %w", err))
	} else {
		return sm.Result{Data: bs}
	}
}