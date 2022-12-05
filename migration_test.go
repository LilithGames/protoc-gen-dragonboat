package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"github.com/LilithGames/protoc-gen-dragonboat/runtime"
	pb "github.com/LilithGames/protoc-gen-dragonboat/testdata"
	"github.com/lni/dragonboat/v3"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/lni/goutils/syncutil"
	assert "github.com/stretchr/testify/require"
)

type testMigrationStateMachine struct {
	ClusterID uint64
	NodeID    uint64
	Step      uint64
	Count     uint64
}

func newTestMigrationStateMachineFacotry(step uint64) sm.CreateStateMachineFunc {
	return func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		return &testMigrationStateMachine{ClusterID: clusterID, NodeID: nodeID, Step: step}
	}
}

func (it *testMigrationStateMachine) Lookup(query interface{}) (interface{}, error) {
	return pb.DragonboatTestLookup(it, query)
}
func (it *testMigrationStateMachine) Update(data []byte) (sm.Result, error) {
	return pb.DragonboatTestUpdate(it, data)
}
func (it *testMigrationStateMachine) SaveSnapshot(w io.Writer, fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	it.save(w)
	return nil
}
func (it *testMigrationStateMachine) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	it.recovery(r)
	return nil
}
func (it *testMigrationStateMachine) Close() error {
	return nil
}

func (it *testMigrationStateMachine) QueryAddressBook(req *pb.QueryAddressBookRequest) (*pb.QueryAddressBookResponse, error) {
	return &pb.QueryAddressBookResponse{
		Data: []*pb.AddressBook{
			&pb.AddressBook{Data: &pb.AddressBook_Company{Company: &pb.Company{Name: fmt.Sprintf("%d", it.Count)}}},
		},
	}, nil
}
func (it *testMigrationStateMachine) MutateAddressBook(req *pb.MutateAddressBookRequest) (*pb.MutateAddressBookResponse, error) {
	it.Count += it.Step
	return &pb.MutateAddressBookResponse{Count: int32(it.Count)}, nil
}

func (it *testMigrationStateMachine) save(w io.Writer) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, it.Count)
	w.Write(data)
}
func (it *testMigrationStateMachine) recovery(r io.Reader) {
	data, _ := io.ReadAll(r)
	v := binary.LittleEndian.Uint64(data)
	it.Count = v
}
func (it *testMigrationStateMachine) SaveState(req *runtime.SaveStateRequest) (*runtime.SaveStateResponse, error) {
	it.save(req.Writer)
	return &runtime.SaveStateResponse{}, nil
}
func (it *testMigrationStateMachine) Migrate(req *runtime.MigrateRequest) (*runtime.MigrateResponse, error) {
	it.recovery(req.Reader)
	return &runtime.MigrateResponse{}, nil
}

func makeNode(nodeID uint64, version string) *runtime.MigrationNode{
	return &runtime.MigrationNode{
		NodeId: nodeID, 
		Version: version,
		Nodes: map[uint64]bool{1: true, 2: true, 3: true},
	}
}

func TestMigration(t *testing.T) {
	clusterID := uint64(0)
	initials := map[uint64]string{
		1: "127.0.0.1:3001",
		2: "127.0.0.1:3002",
		3: "127.0.0.1:3003",
	}
	var err error
	ver1 := "codeVersion1"
	ver2 := "codeVersion2"
	fn1 := runtime.NewMigrationStateMachineWrapper(newTestMigrationStateMachineFacotry(1))
	fn2 := runtime.NewMigrationStateMachineWrapper(newTestMigrationStateMachineFacotry(10))
	nh, stop := newDragonboat(clusterID, initials, fn1)
	defer stop()
	ctx := context.TODO()
	req := &runtime.DragonboatPrepareMigrationRequest{
		CompressionType: runtime.CompressionType_Snappy,
		Expire:          0,
	}
	stopper := tick_count(nh[1], clusterID)
	mc1 := runtime.NewMigrationDragonboatClient(runtime.NewDragonboatClient(nh[1], clusterID))
	mc2 := runtime.NewMigrationDragonboatClient(runtime.NewDragonboatClient(nh[2], clusterID))
	mc3 := runtime.NewMigrationDragonboatClient(runtime.NewDragonboatClient(nh[3], clusterID))
	err = mc1.CheckVersion(ctx, ver1, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	err = mc2.CheckVersion(ctx, ver1, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	err = mc3.CheckVersion(ctx, ver1, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	err = mc1.UpdateUpgradedNode(ctx, &runtime.DragonboatUpdateMigrationRequest{Node: makeNode(1, ver1)}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	err = mc2.UpdateUpgradedNode(ctx, &runtime.DragonboatUpdateMigrationRequest{Node: makeNode(2, ver1)}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	err = mc3.UpdateUpgradedNode(ctx, &runtime.DragonboatUpdateMigrationRequest{Node: makeNode(3, ver1)}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	err = mc1.UpdateSavedNode(ctx, &runtime.DragonboatCompleteMigrationRequest{Node: makeNode(1, ver1)}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	err = mc2.UpdateSavedNode(ctx, &runtime.DragonboatCompleteMigrationRequest{Node: makeNode(2, ver1)}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	err = mc3.UpdateSavedNode(ctx, &runtime.DragonboatCompleteMigrationRequest{Node: makeNode(3, ver1)}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	_ = stopper

	nh[3].Stop()
	waitReady(nh[1], clusterID)
	err = mc1.Migrate(ctx, req, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	nh[3] = newNodeHost(3, initials[3])
	startShard(nh[3], clusterID, 3, nil, fn2)
	waitReady(nh[3], clusterID)
	mc3 = runtime.NewMigrationDragonboatClient(runtime.NewDragonboatClient(nh[3], clusterID))
	err = mc3.UpdateUpgradedNode(ctx, &runtime.DragonboatUpdateMigrationRequest{Node: makeNode(3, ver2)}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)

	nh[2].Stop()
	waitReady(nh[1], clusterID)
	err = mc1.Migrate(ctx, req, runtime.WithClientTimeout(time.Second))
	assert.True(t, runtime.GetDragonboatErrorCode(err) == runtime.ErrCodeAlreadyMigrating)
	nh[2] = newNodeHost(2, initials[2])
	startShard(nh[2], clusterID, 2, nil, fn2)
	waitReady(nh[2], clusterID)
	mc2 = runtime.NewMigrationDragonboatClient(runtime.NewDragonboatClient(nh[2], clusterID))
	err = mc2.UpdateUpgradedNode(ctx, &runtime.DragonboatUpdateMigrationRequest{Node: makeNode(2, ver2)}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)

	nh[1].Stop()
	waitReady(nh[2], clusterID)
	err = mc2.Migrate(ctx, req, runtime.WithClientTimeout(time.Second))
	assert.True(t, runtime.GetDragonboatErrorCode(err) == runtime.ErrCodeAlreadyMigrating)
	nh[1] = newNodeHost(1, initials[1])
	startShard(nh[1], clusterID, 1, nil, fn2)
	waitReady(nh[1], clusterID)
	mc1 = runtime.NewMigrationDragonboatClient(runtime.NewDragonboatClient(nh[1], clusterID))
	err = mc1.UpdateUpgradedNode(ctx, &runtime.DragonboatUpdateMigrationRequest{Node: makeNode(1, ver2)}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)

	err = mc1.UpdateSavedNode(ctx, &runtime.DragonboatCompleteMigrationRequest{Node: makeNode(1, ver2)}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	err = mc2.UpdateSavedNode(ctx, &runtime.DragonboatCompleteMigrationRequest{Node: makeNode(2, ver2)}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	err = mc3.UpdateSavedNode(ctx, &runtime.DragonboatCompleteMigrationRequest{Node: makeNode(3, ver2)}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)

	view, err := mc1.QueryMigration(ctx, &runtime.DragonboatQueryMigrationRequest{}, runtime.WithClientTimeout(time.Second))
	assert.Nil(t, err)
	assert.Equal(t, runtime.MigrationStateType_Empty, view.View.Type)
	assert.Equal(t, ver2, view.View.Version)
	assert.True(t, view.View.StateIndex > 5)
	stopper.Stop()

	count1 := get_count(nh[1], clusterID)
	count2 := get_count(nh[2], clusterID)
	count3 := get_count(nh[3], clusterID)
	assert.True(t, *count1 == *count2, *count2 == *count3)
}

func tick_count(nh *dragonboat.NodeHost, clusterID uint64) *syncutil.Stopper {
	stopper := syncutil.NewStopper()
	stopper.RunWorker(func() {
		c := pb.NewTestDragonboatClient(runtime.NewDragonboatClient(nh, clusterID))
		t := time.NewTicker(time.Millisecond * 30)
		defer t.Stop()
		ctx := context.Background()
		for {
			select {
			case <-t.C:
				_, err := c.MutateAddressBook(ctx, &pb.MutateAddressBookRequest{}, runtime.WithClientTimeout(time.Millisecond * 30))
				if err != nil {
					log.Println("[WARN]", fmt.Errorf("node %d shard %d tick err: %w", nh.NodeHostConfig().Expert.TestNodeHostID, clusterID, err))
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})
	return stopper

}

func get_count(nh *dragonboat.NodeHost, clusterID uint64) *string {
	c := pb.NewTestDragonboatClient(runtime.NewDragonboatClient(nh, clusterID))
	ctx := context.Background()
	resp, err := c.QueryAddressBook(ctx, &pb.QueryAddressBookRequest{}, runtime.WithClientTimeout(time.Second))
	if err != nil {
		log.Println("[WARN]", fmt.Errorf("QueryAddressBook err: %w", err))
		return nil
	}
	count := resp.Data[0].Data.(*pb.AddressBook_Company).Company.Name
	return &count
}
