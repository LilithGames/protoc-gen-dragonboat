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
	Save(ctx context.Context, opts ...DragonboatClientOption) (uint64, error)
	AddNode(ctx context.Context, nodeID uint64, addr string, opts ...DragonboatClientOption) error
	RemoveNode(ctx context.Context, nodeID uint64, opts ...DragonboatClientOption) error
	GetShardID() uint64
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
		fmt.Printf("%+v\n", err)
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

func (it *DragonboatClient) Save(ctx context.Context, opts ...DragonboatClientOption) (uint64, error) {
	o := getDragonboatClientOptionsWithBase(it.o, opts...)
	if o.timeout != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *o.timeout)
		defer cancel()
	}
	opt := dragonboat.SnapshotOption{}
	if o.compactOverhead != nil {
		opt.OverrideCompactionOverhead = true
		opt.CompactionOverhead = *o.compactOverhead
	}
	if o.exportPath != nil {
		opt.Exported = true
		opt.ExportPath = *o.exportPath
	}
	index, err := it.nh.SyncRequestSnapshot(ctx, it.shardID, opt)
	if err != nil {
		return 0, fmt.Errorf("SyncRequestSnapshot err: %w", err)
	}
	return index, nil
}

func (it *DragonboatClient) AddNode(ctx context.Context, nodeID uint64, addr string, opts ...DragonboatClientOption) error {
	o := getDragonboatClientOptionsWithBase(it.o, opts...)
	if o.timeout != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *o.timeout)
		defer cancel()
	}
	if err := it.nh.SyncRequestAddNode(ctx, it.shardID, nodeID, addr, 0); err != nil {
		return fmt.Errorf("SyncRequestAddNode(%d, %d, %s) err: %w", it.shardID, nodeID, addr, err)
	}
	return nil
}

func (it *DragonboatClient) RemoveNode(ctx context.Context, nodeID uint64, opts ...DragonboatClientOption) error {
	o := getDragonboatClientOptionsWithBase(it.o, opts...)
	if o.timeout != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *o.timeout)
		defer cancel()
	}
	if err := it.nh.SyncRequestDeleteNode(ctx, it.shardID, nodeID, 0); err != nil {
		return fmt.Errorf("SyncRequestDeleteNode(%d, %d) err: %w", it.shardID, nodeID, err)
	}
	return nil
}

func (it *DragonboatClient) GetShardID() uint64 {
	return it.shardID
}

type dragonboatClientOptions struct {
	session *client.Session
	timeout *time.Duration
	stale bool
	compactOverhead *uint64
	exportPath *string
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

func DragonboatClientCompactOverhead(compactOverhead uint64) DragonboatClientOption {
	return newFuncDragonboatClientOption(func(o *dragonboatClientOptions) {
		o.compactOverhead = &compactOverhead
	})
}
func DragonboatClientExportPath(exportPath string) DragonboatClientOption {
	return newFuncDragonboatClientOption(func(o *dragonboatClientOptions) {
		o.exportPath = &exportPath
	})
}
