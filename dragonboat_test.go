package main

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	pb "github.com/LilithGames/protoc-gen-dragonboat/testdata"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/assert"

	"github.com/LilithGames/protoc-gen-dragonboat/runtime"
)

type stateMachine struct {
	ClusterID uint64
	NodeID    uint64
	Count     uint64
}

func newStateMachine(clusterID uint64, nodeID uint64) sm.IStateMachine {
	return &stateMachine{ClusterID: clusterID, NodeID: nodeID}
}
func (it *stateMachine) Lookup(query interface{}) (interface{}, error) {
	return pb.DragonboatTestLookup(it, query)
}
func (it *stateMachine) Update(data []byte) (sm.Result, error) {
	return pb.DragonboatTestUpdate(it, data)
}
func (it *stateMachine) SaveSnapshot(w io.Writer, fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	return nil
}
func (it *stateMachine) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	return nil
}
func (it *stateMachine) Close() error {
	return nil
}

func (it *stateMachine) QueryAddressBook(req *pb.QueryAddressBookRequest) (*pb.QueryAddressBookResponse, error) {
	if req.Id == 999 {
		panic(999)
	}
	return &pb.QueryAddressBookResponse{
		Data: []*pb.AddressBook{
			&pb.AddressBook{Data: &pb.AddressBook_Company{Company: &pb.Company{Name: fmt.Sprintf("%d", it.Count)}}},
		},
	}, nil
}
func (it *stateMachine) MutateAddressBook(req *pb.MutateAddressBookRequest) (*pb.MutateAddressBookResponse, error) {
	if req.Id == 999 {
		panic(999)
	}
	it.Count++
	return &pb.MutateAddressBookResponse{Count: int32(it.Count)}, nil
}

type concurrency struct {
	ClusterID uint64
	NodeID    uint64
	Count     uint64
}

func newConcurrency(clusterID uint64, nodeID uint64) sm.IConcurrentStateMachine {
	return &concurrency{ClusterID: clusterID, NodeID: nodeID}
}

func (it *concurrency) Lookup(query interface{}) (interface{}, error) {
	return pb.DragonboatTestLookup(it, query)
}
func (it *concurrency) Update(entries []sm.Entry) ([]sm.Entry, error) {
	return pb.DragonboatTestConcurrencyUpdate(it, entries)
}
func (it *concurrency) PrepareSnapshot() (interface{}, error) {
	return nil, nil
}
func (it *concurrency) SaveSnapshot(p interface{}, w io.Writer, fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	return nil
}
func (it *concurrency) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	return nil
}
func (it *concurrency) Close() error {
	return nil
}
func (it *concurrency) QueryAddressBook(req *pb.QueryAddressBookRequest) (*pb.QueryAddressBookResponse, error) {
	if req.Id == 999 {
		panic(999)
	}
	return &pb.QueryAddressBookResponse{
		Data: []*pb.AddressBook{
			&pb.AddressBook{Data: &pb.AddressBook_Company{Company: &pb.Company{Name: fmt.Sprintf("%d", it.Count)}}},
		},
	}, nil
}
func (it *concurrency) MutateAddressBook(req *pb.MutateAddressBookRequest) (*pb.MutateAddressBookResponse, error) {
	if req.Id == 888 {
		return nil, runtime.NewDragonboatError(888, "hello derror")
	}
	if req.Id == 999 {
		panic(999)
	}
	it.Count++
	return &pb.MutateAddressBookResponse{Count: int32(it.Count)}, nil
}

func TestDragonboat(t *testing.T) {
	initials := map[uint64]string{1: "127.0.0.1:63000"}
	nhs, stop := newDragonboat(0, initials, sm.CreateStateMachineFunc(newStateMachine))
	defer stop()
	nh := nhs[1]

	dc := runtime.NewDragonboatClient(nh, 0)
	client := pb.NewTestDragonboatClient(dc)
	resp, err := client.QueryAddressBook(context.TODO(), &pb.QueryAddressBookRequest{Id: 0}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)

	assert.Equal(t, "0", resp.Data[0].Data.(*pb.AddressBook_Company).Company.Name)
	resp2, err := client.MutateAddressBook(context.TODO(), &pb.MutateAddressBookRequest{Id: 0}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	assert.Equal(t, int32(1), resp2.Count)

	resp, err = client.QueryAddressBook(context.TODO(), &pb.QueryAddressBookRequest{Id: 0}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	assert.Equal(t, "1", resp.Data[0].Data.(*pb.AddressBook_Company).Company.Name)

	resp, err = client.QueryAddressBook(context.TODO(), &pb.QueryAddressBookRequest{Id: 0}, runtime.WithClientTimeout(time.Second), runtime.WithClientStale(true))
	assert.Nil(t, err)
	assert.Equal(t, "1", resp.Data[0].Data.(*pb.AddressBook_Company).Company.Name)

	// test panic
	_, err = client.MutateAddressBook(context.TODO(), &pb.MutateAddressBookRequest{Id: 999}, runtime.WithClientTimeout(time.Second))
	assert.NotNil(t, err)
	_, err = client.QueryAddressBook(context.TODO(), &pb.QueryAddressBookRequest{Id: 999}, runtime.WithClientTimeout(time.Second))
	assert.NotNil(t, err)

	resp5, err := dc.Query(context.TODO(), nil, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, resp5)
	assert.Nil(t, err)
}

func TestDragonboatConcurrency(t *testing.T) {
	initials := map[uint64]string{1: "127.0.0.1:63000"}
	nhs, stop := newDragonboat(0, initials, sm.CreateConcurrentStateMachineFunc(newConcurrency))
	defer stop()
	nh := nhs[1]

	client := pb.NewTestDragonboatClient(runtime.NewDragonboatClient(nh, 0))
	resp, err := client.QueryAddressBook(context.TODO(), &pb.QueryAddressBookRequest{Id: 0}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	assert.Equal(t, "0", resp.Data[0].Data.(*pb.AddressBook_Company).Company.Name)

	_, err = client.MutateAddressBook(context.TODO(), &pb.MutateAddressBookRequest{Id: 0}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)

	resp, err = client.QueryAddressBook(context.TODO(), &pb.QueryAddressBookRequest{Id: 0}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	assert.Equal(t, "1", resp.Data[0].Data.(*pb.AddressBook_Company).Company.Name)

	resp, err = client.QueryAddressBook(context.TODO(), &pb.QueryAddressBookRequest{Id: 0}, runtime.WithClientTimeout(time.Second), runtime.WithClientStale(true))
	assert.Nil(t, err)
	assert.Equal(t, "1", resp.Data[0].Data.(*pb.AddressBook_Company).Company.Name)

	// test error
	_, err = client.MutateAddressBook(context.TODO(), &pb.MutateAddressBookRequest{Id: 888}, runtime.WithClientTimeout(time.Second))
	assert.NotNil(t, err)
	assert.Equal(t, int32(888), runtime.GetDragonboatErrorCode(err))

	// test panic
	_, err = client.MutateAddressBook(context.TODO(), &pb.MutateAddressBookRequest{Id: 999}, runtime.WithClientTimeout(time.Second))
	assert.NotNil(t, err)
	_, err = client.QueryAddressBook(context.TODO(), &pb.QueryAddressBookRequest{Id: 999}, runtime.WithClientTimeout(time.Second))
	assert.NotNil(t, err)

}
