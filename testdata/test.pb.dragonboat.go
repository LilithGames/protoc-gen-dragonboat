// This code was autogenerated from protoc-gen-dragonboat, do not edit.
package testdata

import (
	"context"
	"fmt"
	"runtime/debug"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"google.golang.org/protobuf/proto"

	"github.com/LilithGames/protoc-gen-dragonboat/runtime"
)

type ITestDragonboatServer interface {
	QueryAddressBook(req *QueryAddressBookRequest) (*QueryAddressBookResponse, error)
	MutateAddressBook(req *MutateAddressBookRequest) (*MutateAddressBookResponse, error)
}

type ITestDragonboatClient interface {
	QueryAddressBook(ctx context.Context, req *QueryAddressBookRequest, opts ...runtime.DragonboatClientOption) (*QueryAddressBookResponse, error)
	MutateAddressBook(ctx context.Context, req *MutateAddressBookRequest, opts ...runtime.DragonboatClientOption) (*MutateAddressBookResponse, error)
}

func DragonboatTestLookup(s ITestDragonboatServer, query interface{}) (result interface{}, err error) {
	defer func() {
		if perr := recover(); perr != nil {
			err = fmt.Errorf("panic: %v\nstacktrace from panic: %s", perr, string(debug.Stack()))
		}
	}()
	switch q := query.(type) {
	case *QueryAddressBookRequest:
		resp, err := s.QueryAddressBook(q)
		if err != nil {
			return resp, fmt.Errorf("ITestServer.QueryAddressBook(%v) err: %w", q, err)
		}
		return resp, nil
	default:
		return nil, fmt.Errorf("%w(type: %T)", runtime.ErrUnknownRequest, q)
	}
}

func DragonboatTestUpdateDispatch(s ITestDragonboatServer, msg proto.Message) (result proto.Message, err error) {
	defer func() {
		if perr := recover(); perr != nil {
			err = fmt.Errorf("panic: %v\nstacktrace from panic: %s", perr, string(debug.Stack()))
		}
	}()
	switch m := msg.(type) {
	case *MutateAddressBookRequest:
		resp, err := s.MutateAddressBook(m)
		return resp, err
	default:
		return nil, fmt.Errorf("%w(type: %T)", runtime.ErrUnknownRequest, m)
	}
}

func DragonboatTestUpdate(s ITestDragonboatServer, data []byte) (sm.Result, error) {
	msg, err := runtime.ParseDragonboatRequest(data)
	if err != nil {
		return runtime.MakeDragonboatResult(nil, err), nil
	}
	resp, err := DragonboatTestUpdateDispatch(s, msg)
	return runtime.MakeDragonboatResult(resp, err), nil
}

func DragonboatTestConcurrencyUpdate(s ITestDragonboatServer, entries []sm.Entry) ([]sm.Entry, error) {
	for i := range entries {
		entry := &entries[i]
		msg, err := runtime.ParseDragonboatRequest(entry.Cmd)
		if err != nil {
			entry.Result = runtime.MakeDragonboatResult(nil, err)
		} else {
			resp, err := DragonboatTestUpdateDispatch(s, msg)
			entry.Result = runtime.MakeDragonboatResult(resp, err)
		}
	}
	return entries, nil
}

type TestDragonboatClient struct {
	client runtime.IDragonboatClient
}

func NewTestDragonboatClient(client runtime.IDragonboatClient) ITestDragonboatClient {
	return &TestDragonboatClient{client: client}
}
func (it *TestDragonboatClient) QueryAddressBook(ctx context.Context, req *QueryAddressBookRequest, opts ...runtime.DragonboatClientOption) (*QueryAddressBookResponse, error) {
	resp, err := it.client.Query(ctx, req, opts...)
	if r, ok := resp.(*QueryAddressBookResponse); ok {
		return r, err
	} else if err != nil {
		return nil, err
	} else {
		return nil, fmt.Errorf("cannot parse %T response type to *QueryAddressBookResponse", resp)
	}
}
func (it *TestDragonboatClient) MutateAddressBook(ctx context.Context, req *MutateAddressBookRequest, opts ...runtime.DragonboatClientOption) (*MutateAddressBookResponse, error) {
	resp, err := it.client.Mutate(ctx, req, opts...)
	if r, ok := resp.(*MutateAddressBookResponse); ok {
		return r, err
	} else if err != nil {
		return nil, err
	} else {
		return nil, fmt.Errorf("cannot parse %T response type to *MutateAddressBookResponse", resp)
	}
}
