package runtime

import (
	"fmt"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"google.golang.org/protobuf/proto"
	anypb "google.golang.org/protobuf/types/known/anypb"

)

func ParseDragonboatResult(result sm.Result) (proto.Message, error) {
	dr := DragonboatResult{}
	if err := proto.Unmarshal(result.Data, &dr); err != nil {
		return nil, fmt.Errorf("proto.Unmarshal(DragonboatResult) err: %w", err)
	}
	if dr.Data == nil {
		return nil, dr.Error
	}
	msg, err := anypb.UnmarshalNew(dr.Data, proto.UnmarshalOptions{DiscardUnknown: true})
	if err != nil {
		return nil, fmt.Errorf("anypb.UnmarshalNew() err: %w", err)
	}
	return msg, dr.Error
}
