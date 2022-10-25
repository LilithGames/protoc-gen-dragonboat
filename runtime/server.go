package runtime

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	anypb "google.golang.org/protobuf/types/known/anypb"
)

func ParseDragonboatRequest(data []byte) (proto.Message, error) {
	req := DragonboatRequest{}
	if err := proto.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("DragonboatRequest unmarshal err: %w", err)
	}
	msg, err := anypb.UnmarshalNew(req.Data, proto.UnmarshalOptions{DiscardUnknown: true})
	if err != nil {
		return nil, fmt.Errorf("DragonboatRequest.Data unmarshal err: %w", err)
	}
	return msg, nil
}
