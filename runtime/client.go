package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"google.golang.org/protobuf/proto"
	anypb "google.golang.org/protobuf/types/known/anypb"
)

type IDragonboatClient interface {
	Query(ctx context.Context, q proto.Message, opts ...DragonboatClientOption) (proto.Message, error)
	Mutate(ctx context.Context, m proto.Message, opts ...DragonboatClientOption) (proto.Message, error)
}

type DragonboatClient struct {
	nh      *dragonboat.NodeHost
	shardID uint64

	o *dragonboatClientOptions
}

func NewDragonboatClient(nh *dragonboat.NodeHost, shardID uint64, opts ...DragonboatClientOption) IDragonboatClient {
	o := getDragonboatClientOptions(opts...)
	if o.session == nil {
		o.session = nh.GetNoOPSession(shardID)
	}
	return &DragonboatClient{
		nh:      nh,
		shardID: shardID,
		o:       o,
	}
}

func (it *DragonboatClient) Query(ctx context.Context, query proto.Message, opts ...DragonboatClientOption) (proto.Message, error) {
	o := getDragonboatClientOptionsWithBase(it.o, opts...)
	if o.timeout != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *o.timeout)
		defer cancel()
	}

	var result interface{}
	var err error
	if o.stale {
		result, err = it.nh.StaleRead(it.shardID, query)
	} else {
		result, err = it.nh.SyncRead(ctx, it.shardID, query)
	}
	if err != nil {
		return nil, fmt.Errorf("SyncRead(%v), err: %w", query, err)
	}
	if result == nil {
		return nil, nil
	}
	r, ok := result.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("result(%v) expect to be proto.Message", result)
	}
	return r, nil
}

func (it *DragonboatClient) Mutate(ctx context.Context, mutation proto.Message, opts ...DragonboatClientOption) (proto.Message, error) {
	o := getDragonboatClientOptionsWithBase(it.o, opts...)
	if o.timeout != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *o.timeout)
		defer cancel()
	}
	data, err := anypb.New(mutation)
	if err != nil {
		return nil, fmt.Errorf("marshal mutation data err: %w", err)
	}
	req := &DragonboatRequest{Data: data}
	cmd, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal mutation request err: %w", err)
	}
	result, err := it.nh.SyncPropose(ctx, o.session, cmd)
	if err != nil {
		return nil, fmt.Errorf("SyncPropose err: %w", err)
	}
	return ParseDragonboatResult(result)
}

type dragonboatClientOptions struct {
	session *client.Session
	timeout *time.Duration
	stale bool
}

type DragonboatClientOption interface {
	apply(*dragonboatClientOptions)
}

type funcDragonboatClientOption struct {
	f func(*dragonboatClientOptions)
}

func (it *funcDragonboatClientOption) apply(o *dragonboatClientOptions) {
	it.f(o)
}

func newFuncDragonboatClientOption(f func(*dragonboatClientOptions)) DragonboatClientOption {
	return &funcDragonboatClientOption{f: f}
}
func getDragonboatClientOptions(opts ...DragonboatClientOption) *dragonboatClientOptions {
	o := &dragonboatClientOptions{}
	for _, opt := range opts {
		opt.apply(o)
	}
	return o
}

func getDragonboatClientOptionsWithBase(base *dragonboatClientOptions, opts ...DragonboatClientOption) *dragonboatClientOptions {
	o := &dragonboatClientOptions{}
	*o = *base
	for _, opt := range opts {
		opt.apply(o)
	}
	return o
}

func WithClientSession(session *client.Session) DragonboatClientOption {
	return newFuncDragonboatClientOption(func(o *dragonboatClientOptions) {
		o.session = session
	})
}

func WithClientTimeout(timeout time.Duration) DragonboatClientOption {
	return newFuncDragonboatClientOption(func(o *dragonboatClientOptions) {
		o.timeout = &timeout
	})
}

func WithClientStale(stale bool) DragonboatClientOption {
	return newFuncDragonboatClientOption(func(o *dragonboatClientOptions) {
		o.stale = stale
	})
}
