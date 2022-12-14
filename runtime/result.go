package runtime

import (
	"fmt"
	"errors"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"google.golang.org/protobuf/proto"
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
		dr.Error = &DragonboatError{Code: ErrCodeInternal, Msg: err.Error()}
	}

	if bs, err := proto.Marshal(dr); err != nil {
		panic(fmt.Errorf("proto.Marshal(DragonboatResult) err: %w", err))
	} else {
		value := uint64(ErrCodeOK)
		if dr.Error != nil {
			value = uint64(dr.Error.Code)
		}
		return sm.Result{Value: value, Data: bs}
	}
}

func GetDragonboatResultErrorCode(r sm.Result) int32 {
	return int32(r.Value)
}

func ParseDragonboatResult(result sm.Result) (proto.Message, error) {
	dr := &DragonboatResult{}
	if err := proto.Unmarshal(result.Data, dr); err != nil {
		return nil, fmt.Errorf("proto.Unmarshal(DragonboatResult) err: %w", err)
	}
	if dr.Data == nil {
		if GetDragonboatErrorCode(dr.Error) == ErrCodeOK {
			return nil, nil
		}
		return nil, dr.Error
	}
	msg, err := anypb.UnmarshalNew(dr.Data, proto.UnmarshalOptions{DiscardUnknown: true})
	if err != nil {
		return nil, fmt.Errorf("anypb.UnmarshalNew() err: %w", err)
	}
	if GetDragonboatErrorCode(dr.Error) == ErrCodeOK {
		return msg, nil
	}
	return msg, dr.Error
}

func ClientResponseConversion[T proto.Message](resp proto.Message, err error) (T, error) {
	if r, ok := resp.(T); ok {
		return r, err
	} else if err != nil {
		return *new(T), err
	} else {
		return *new(T), fmt.Errorf("%T cannot conversion to %T", resp, *new(T))
	}
}
