package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/LilithGames/protoc-gen-dragonboat/runtime"
	pb "github.com/LilithGames/protoc-gen-dragonboat/testdata"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/assert"
)

type testConrrentMigrationStateMachine struct {
	ClusterID uint64
	NodeID    uint64
	Step      uint64
	Count     uint64
	mu        sync.RWMutex
}

func newTestConrrentMigrationStateMachineFactory(step uint64) sm.CreateConcurrentStateMachineFunc {
	return func (clusterID uint64, nodeID uint64) sm.IConcurrentStateMachine {
		return &testConrrentMigrationStateMachine{
			ClusterID: clusterID,
			NodeID:    nodeID,
			Step:      step,
		}
	}
}

func (it *testConrrentMigrationStateMachine) Update(entries []sm.Entry) ([]sm.Entry, error) {
	return pb.DragonboatTestConcurrentUpdate(it, entries)
}
func (it *testConrrentMigrationStateMachine) Lookup(query interface{}) (interface{}, error) {
	return pb.DragonboatTestLookup(it, query)
}
func (it *testConrrentMigrationStateMachine) PrepareSnapshot() (interface{}, error) {
	it.mu.RLock()
	defer it.mu.RUnlock()
	count := it.Count
	return count, nil
}
func (it *testConrrentMigrationStateMachine) SaveSnapshot(state interface{}, w io.Writer, fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	it.save(w, state.(uint64))
	return nil
}
func (it *testConrrentMigrationStateMachine) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	it.recovery(r)
	return nil
}
func (it *testConrrentMigrationStateMachine) Close() error {
	return nil
}

func (it *testConrrentMigrationStateMachine) QueryAddressBook(req *pb.QueryAddressBookRequest) (*pb.QueryAddressBookResponse, error) {
	it.mu.RLock()
	defer it.mu.RUnlock()
	return &pb.QueryAddressBookResponse{
		Data: []*pb.AddressBook{
			&pb.AddressBook{Data: &pb.AddressBook_Company{Company: &pb.Company{Name: fmt.Sprintf("%d", it.Count)}}},
		},
	}, nil
}
func (it *testConrrentMigrationStateMachine) MutateAddressBook(req *pb.MutateAddressBookRequest) (*pb.MutateAddressBookResponse, error) {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.Count++
	return &pb.MutateAddressBookResponse{Count: int32(it.Count)}, nil
}

func (it *testConrrentMigrationStateMachine) SaveState(req *runtime.SaveStateRequest) (*runtime.SaveStateResponse, error) {
	it.mu.RLock()
	defer it.mu.RUnlock()
	it.save(req.Writer, it.Count)
	return &runtime.SaveStateResponse{}, nil
}

func (it *testConrrentMigrationStateMachine) Migrate(req *runtime.MigrateRequest) (*runtime.MigrateResponse, error) {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.recovery(req.Reader)
	return &runtime.MigrateResponse{}, nil
}

func (it *testConrrentMigrationStateMachine) save(w io.Writer, state uint64) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, state)
	w.Write(data)
}

func (it *testConrrentMigrationStateMachine) recovery(r io.Reader) {
	data, _ := io.ReadAll(r)
	v := binary.LittleEndian.Uint64(data)
	it.Count = v
}

func TestConcurrentMigration(t *testing.T) {
	clusterID := uint64(0)
	initials := map[uint64]string{
		1: "127.0.0.1:3001",
		2: "127.0.0.1:3002",
		3: "127.0.0.1:3003",
	}
	var err error
	ver1 := "codeVersion1"
	ver2 := "codeVersion2"
	fn1 := runtime.NewMigrationConcurrencyStateMachineWrapper(newTestConrrentMigrationStateMachineFactory(1))
	fn2 := runtime.NewMigrationConcurrencyStateMachineWrapper(newTestConrrentMigrationStateMachineFactory(2))
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
	assert.Equal(t, view.View.Type, runtime.MigrationStateType_Empty)
	assert.Equal(t, view.View.Version, ver2)
	stopper.Stop()

	count1 := get_count(nh[1], clusterID)
	count2 := get_count(nh[2], clusterID)
	count3 := get_count(nh[3], clusterID)
	assert.True(t, *count1 == *count2, *count2 == *count3)
}
