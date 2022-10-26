package main

import (
	"fmt"
	"io"
	"testing"
	"context"
	"time"
	"errors"

	pb "github.com/LilithGames/protoc-gen-dragonboat/testdata"
	sm "github.com/lni/dragonboat/v3/statemachine"
	vfs "github.com/lni/goutils/vfs"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
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
	return &pb.QueryAddressBookResponse{
		Data: []*pb.AddressBook{
			&pb.AddressBook{Data: &pb.AddressBook_Company{Company: &pb.Company{Name: fmt.Sprintf("%d", it.Count)}}},
		},
	}, nil
}
func (it *stateMachine) MutateAddressBook(req *pb.MutateAddressBookRequest) (*pb.MutateAddressBookResponse, error) {
	it.Count++
	return &pb.MutateAddressBookResponse{Count: int32(it.Count)}, nil
}

func newDragonboat(t *testing.T) *dragonboat.NodeHost {
	conf := config.NodeHostConfig{
		NodeHostDir: "single_nodehost_test_dir_safe_to_delete",
		AddressByNodeHostID: false,
		RTTMillisecond:      1,
		RaftAddress: "127.0.0.1:63000",
		Expert: config.ExpertConfig{
			FS: vfs.Default,
			TestNodeHostID: 0,
		},
	}
	nh, err := dragonboat.NewNodeHost(conf)
	fmt.Printf("%+v\n", err)
	assert.Nil(t, err)
	return nh
}
func startShard(t *testing.T, nh *dragonboat.NodeHost) {
	conf := config.Config{
		NodeID: 1,
		ClusterID: 0,
		CheckQuorum: true,
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		SnapshotEntries:    1,
		CompactionOverhead: 5,
		OrderedConfigChange: false,
	}
	initials := map[uint64]string{1: "127.0.0.1:63000"}
	err := nh.StartCluster(initials, false, newStateMachine, conf)
	assert.Nil(t, err)
}
func waitReady(nh *dragonboat.NodeHost) {
	for {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
		defer cancel()
		if _, err := nh.SyncRead(ctx, 0, nil); err != nil {
			if errors.Is(err, dragonboat.ErrClusterNotReady) {
				time.Sleep(time.Second)
				continue
			}
		}
		return
	}
}

func TestDragonboat(t *testing.T) {
	var p pb.Person
	fmt.Printf("%+v\n", p)
	nh := newDragonboat(t)
	defer func() {
		nh.Stop()
		err := vfs.Default.RemoveAll("single_nodehost_test_dir_safe_to_delete")
		assert.Nil(t, err)
	}()
	startShard(t, nh)
	waitReady(nh)

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
}
