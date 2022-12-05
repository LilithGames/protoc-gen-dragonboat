package main

import (
	"os"
	"context"
	"fmt"
	"io"
	"log"
	"testing"
	"time"
	"encoding/binary"
	"errors"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/tools"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/assert"
	"github.com/lni/goutils/syncutil"
)

var bus *Bus

func init() {
	bus = &Bus{}
}

type exampleStateMachine struct {
	ClusterID uint64
	NodeID    uint64
	Tick      uint64
}

func NewExampleStateMachine(clusterID uint64, nodeID uint64) sm.IStateMachine {
	return &exampleStateMachine{ClusterID: clusterID, NodeID: nodeID}
}
func (it *exampleStateMachine) Update(req []byte) (sm.Result, error) {
	if string(req) == "tick" {
		it.Tick++
		log.Println("[INFO]", fmt.Sprintf("Node: %d Tick %d", it.NodeID, it.Tick))
	} else if string(req) == "nodeID" {
		log.Println("[INFO]", fmt.Sprintf("Node: %d UpdateNodeID", it.NodeID))
		return sm.Result{Value: it.NodeID}, nil
	}

	return sm.Result{}, nil
}
func (it *exampleStateMachine) Lookup(req interface{}) (interface{}, error) {
	log.Println("[INFO]", fmt.Sprintf("Node: %d Lookup", it.NodeID))
	if s, ok := req.(string); ok {
		if s == "nodeID" {
			return it.NodeID, nil
		}
	}
	return it.Tick, nil
}
func (it *exampleStateMachine) SaveSnapshot(w io.Writer, _ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	log.Println("[INFO]", fmt.Sprintf("Node: %d Save", it.NodeID))
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, it.Tick)
	w.Write(data)
	return nil
}
func (it *exampleStateMachine) RecoverFromSnapshot(r io.Reader, _ []sm.SnapshotFile, _ <-chan struct{}) error {
	log.Println("[INFO]", fmt.Sprintf("Node: %d Recover", it.NodeID))
	data, _ := io.ReadAll(r)
	v := binary.LittleEndian.Uint64(data)
	it.Tick = v
	return nil
}
func (it *exampleStateMachine) Close() error {
	log.Println("[INFO]", fmt.Sprintf("Node: %d Close", it.NodeID))
	return nil
}

func tick(nh *dragonboat.NodeHost, clusterID uint64) *syncutil.Stopper {
	stopper := syncutil.NewStopper()
	stopper.RunWorker(func() {
		t := time.NewTicker(time.Millisecond * 30)
		defer t.Stop()
		nop := nh.GetNoOPSession(clusterID)
		ctx := context.Background()
		for {
			select {
			case <-t.C:
				ctx1, cancel := context.WithTimeout(ctx, time.Millisecond*30)
				_, err := nh.SyncPropose(ctx1, nop, []byte("tick"))
				cancel()
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

func taskSnapshot(nh *dragonboat.NodeHost, clusterID uint64) uint64 {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	r, err := nh.SyncRequestSnapshot(ctx, clusterID, dragonboat.SnapshotOption{
		CompactionOverhead: 0,
		OverrideCompactionOverhead: true,
	})
	if err != nil {
		log.Println("[WARN]", fmt.Errorf("node %d RequestSnapshot err: %w", nh.NodeHostConfig().Expert.TestNodeHostID, err))
		return 0
	}
	return r
}

func taskSnapshotFile(nh *dragonboat.NodeHost, clusterID uint64) string {
	path := "single_nodehost_test_dir_safe_to_delete/snapshot"
	os.MkdirAll(path, os.ModePerm)
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*3)
	defer cancel()
	rs, err := nh.RequestSnapshot(clusterID, dragonboat.SnapshotOption{
		ExportPath: path,
		Exported: true,
		CompactionOverhead: 0,
		OverrideCompactionOverhead: true,
	}, time.Second*3)
	if err != nil {
		panic(err)
	}
	var index uint64
	select {
	case r := <-rs.AppliedC():
		if r.Completed() {
			index = r.SnapshotIndex()
		} else {
			err = fmt.Errorf("snapshot failed")
		}
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err != nil {
		log.Println("[WARN]", fmt.Errorf("node %d RequestSnapshot err: %w", nh.NodeHostConfig().Expert.TestNodeHostID, err))
		return path
	}
	return fmt.Sprintf("%s/snapshot-%016X", path, index)
}

func getLeader(nh *dragonboat.NodeHost, clusterID uint64) *uint64 {
	nodeID, valid, err := nh.GetLeaderID(clusterID)
	if err != nil || !valid {
		return nil
	}
	return &nodeID
}

func TestSnapshot4(t *testing.T) {
	clusterID := uint64(0)
	initials := map[uint64]string{
		1: "127.0.0.1:3001",
		2: "127.0.0.1:3002",
		3: "127.0.0.1:3003",
	}
	nh, stop := newDragonboat(clusterID, initials, sm.CreateStateMachineFunc(NewExampleStateMachine))
	defer stop()
	taskSnapshotFile(nh[3], clusterID)
	// _ = nh
	time.Sleep(time.Second*10)

}

func TestAddNode(t *testing.T) {
	clusterID := uint64(0)
	initials := map[uint64]string{
		1: "127.0.0.1:3001",
		2: "127.0.0.1:3002",
	}
	nh, stop := newDragonboat(clusterID, initials, sm.CreateStateMachineFunc(NewExampleStateMachine))
	defer stop()
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	var err error
	nh[3] = newNodeHost(3, "127.0.0.1:3003")
	startShard(nh[3], clusterID, 3, nil, sm.CreateStateMachineFunc(NewExampleStateMachine))
	nhi := nh[3].GetNodeHostInfo(dragonboat.NodeHostInfoOption{true})
	fmt.Printf("%+v\n", nhi.ClusterInfoList)
	waitReady(nh[3], clusterID)
	err = nh[3].SyncRequestAddNode(ctx, clusterID, 3, "127.0.0.1:3003", 0)
	assert.Nil(t, err)
}

func TestRemoveNode(t *testing.T) {
	clusterID := uint64(0)
	initials := map[uint64]string{
		1: "127.0.0.1:3001",
		2: "127.0.0.1:3002",
	}
	nh, stop := newDragonboat(clusterID, initials, sm.CreateStateMachineFunc(NewExampleStateMachine))
	defer stop()
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	var err error
	err = nh[1].SyncRequestDeleteNode(ctx, clusterID, 2, 0)
	assert.Nil(t, err)
	err = nh[1].SyncRequestDeleteNode(ctx, clusterID, 2, 0)
	fmt.Printf("%+v\n", err)
}

func TestSnapshot(t *testing.T) {
	clusterID := uint64(0)
	initials := map[uint64]string{
		1: "127.0.0.1:3001",
		2: "127.0.0.1:3002",
		3: "127.0.0.1:3003",
	}
	nh, stop := newDragonboat(clusterID, initials, sm.CreateStateMachineFunc(NewExampleStateMachine))
	defer stop()

	tick(nh[1], clusterID)
	time.Sleep(time.Second)

	taskSnapshot(nh[3], clusterID)
	nh[3].StopCluster(clusterID)
	log.Println("[INFO]", fmt.Sprintf("Node 3 Stopped"))
	time.Sleep(time.Millisecond*30)

	leaderID := getLeader(nh[1], clusterID)
	if leaderID == nil {
		panic("no leader")
	}

	taskSnapshot(nh[*leaderID], clusterID)

	startShard(nh[3], clusterID, 3, nil, sm.CreateStateMachineFunc(NewExampleStateMachine))
	waitReady(nh[3], clusterID)

	time.Sleep(time.Second)

}

func TestSnapshot3(t *testing.T) {
	clusterID := uint64(0)
	initials := map[uint64]string{
		1: "127.0.0.1:3001",
		2: "127.0.0.1:3002",
		3: "127.0.0.1:3003",
	}
	nh, stop := newDragonboat(clusterID, initials, sm.CreateStateMachineFunc(NewExampleStateMachine))
	defer stop()
	// _ = stop

	tick(nh[1], clusterID)
	time.Sleep(time.Millisecond*300)

	// nh[3].StopCluster(clusterID)
	conf3 := nh[3].NodeHostConfig()
	nh[3].Stop()
	log.Println("[INFO]", fmt.Sprintf("Node 3 Stopped"))
	time.Sleep(time.Millisecond*10*3)

	path := taskSnapshotFile(nh[1], clusterID)

	if err := tools.ImportSnapshot(conf3, path, initials, 3); err != nil {
		log.Fatalln("[FATAL]", fmt.Errorf("ImportSnapshot err: %w", err))
	}
	log.Println("[INFO]", fmt.Sprintf("ImportSnapshot %s success", path))
	nh[3] = newNodeHost(3, initials[3])
	time.Sleep(time.Millisecond*300)
	nh[3].Stop()


	time.Sleep(time.Millisecond*300)
	path = taskSnapshotFile(nh[2], clusterID)
	time.Sleep(time.Millisecond*300)

	if err := tools.ImportSnapshot(conf3, path, initials, 3); err != nil {
		log.Fatalln("[FATAL]", fmt.Errorf("ImportSnapshot err: %w", err))
	}
	log.Println("[INFO]", fmt.Sprintf("ImportSnapshot %s success", path))
	nh[3] = newNodeHost(3, initials[3])

	startShard(nh[3], clusterID, 3, nil, sm.CreateStateMachineFunc(NewExampleStateMachine))
	waitReady(nh[3], clusterID)
	time.Sleep(time.Millisecond*300)
	println("hulucc success")

}

func TestNodeHost1(t *testing.T) {
	clusterID := uint64(0)
	initials := map[uint64]string{
		1: "127.0.0.1:3001",
	}
	nh, stop := newDragonboat(clusterID, initials, sm.CreateStateMachineFunc(NewExampleStateMachine))
	defer stop()
	nhi := nh[1].GetNodeHostInfo(dragonboat.NodeHostInfoOption{true})
	println("hulucc1", len(nhi.ClusterInfoList))
	nh[1].Stop()
	nh[1] = newNodeHost(1, initials[1])
	nhi = nh[1].GetNodeHostInfo(dragonboat.NodeHostInfoOption{false})
	println(nh[1].HasNodeInfo(clusterID, 1))
	println("hulucc3", len(nhi.LogInfo))

}

func TestNodeHost2(t *testing.T) {
	clusterID := uint64(0)
	initials := map[uint64]string{
		1: "127.0.0.1:3001",
		2: "127.0.0.1:3002",
	}
	nh, stop := newDragonboat(clusterID, initials, sm.CreateStateMachineFunc(NewExampleStateMachine))
	defer stop()
	nh[1].Stop()

	// nh[1] = newNodeHost(1, initials[1])
	nhi := nh[2].GetNodeHostInfo(dragonboat.NodeHostInfoOption{false})
	fmt.Printf("hulucc: %+v\n", nhi.ClusterInfoList[0].Nodes)

}


func TestSnapshot2(t *testing.T) {
	clusterID := uint64(0)
	initials := map[uint64]string{
		1: "127.0.0.1:3001",
		2: "127.0.0.1:3002",
		3: "127.0.0.1:3003",
	}
	nh, stop := newDragonboat(clusterID, initials, sm.CreateStateMachineFunc(NewExampleStateMachine))
	defer stop()

	tick(nh[1], clusterID)
	time.Sleep(time.Second)

	nh[3].StopCluster(clusterID)
	log.Println("[INFO]", fmt.Sprintf("Node 3 Stopped"))
	time.Sleep(time.Millisecond*300)

	startShard(nh[3], clusterID, 3, nil, sm.CreateStateMachineFunc(NewExampleStateMachine))
	waitReady(nh[3], clusterID)

	time.Sleep(time.Second)

}

func TestRemoveData(t *testing.T) {
	clusterID := uint64(0)
	initials := map[uint64]string{
		1: "127.0.0.1:3001",
		2: "127.0.0.1:3002",
		3: "127.0.0.1:3003",
	}
	nh, stop := newDragonboat(clusterID, initials, sm.CreateStateMachineFunc(NewExampleStateMachine))
	defer stop()
	if err := nh[3].StopCluster(clusterID); err != nil {
		log.Fatalln("[FATAL]", fmt.Errorf("stop cluster err: %w", err))
	}
	log.Println("[INFO]", fmt.Sprintf("cluster stopped"))
	nhi := nh[3].GetNodeHostInfo(dragonboat.NodeHostInfoOption{false})
	println("start")
	for _, ni := range nhi.LogInfo {
		println(ni.ClusterID, ni.NodeID)
	}
	for {
		if err := nh[3].RemoveData(clusterID, 3); err != nil {
			if errors.Is(err, dragonboat.ErrClusterNotStopped) {
				time.Sleep(time.Millisecond)
				continue
			}
			log.Fatalln("[FATAL]", fmt.Errorf("remove data err: %w", err))
		} else {
			break
		}
	}

	println("start")
	nhi = nh[3].GetNodeHostInfo(dragonboat.NodeHostInfoOption{false})
	for _, ni := range nhi.LogInfo {
		println(ni.ClusterID, ni.NodeID)
	}

}

func TestLookup(t *testing.T) {
	clusterID := uint64(0)
	initials := map[uint64]string{
		1: "127.0.0.1:3001",
		2: "127.0.0.1:3002",
		3: "127.0.0.1:3003",
	}
	nh, stop := newDragonboat(clusterID, initials, sm.CreateStateMachineFunc(NewExampleStateMachine))
	defer stop()
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	leaderID := getLeader(nh[1], clusterID)
	log.Println("[INFO]", fmt.Sprintf("Current leader nodeID: %d", *leaderID))
	nodeID1, err := nh[1].SyncRead(ctx, clusterID, "nodeID")
	assert.Nil(t, err)
	fmt.Printf("GetNodeID: %+v\n", nodeID1)
	nodeID2, err := nh[2].SyncRead(ctx, clusterID, "nodeID")
	assert.Nil(t, err)
	fmt.Printf("GetNodeID: %+v\n", nodeID2)
	nodeID3, err := nh[3].SyncRead(ctx, clusterID, "nodeID")
	assert.Nil(t, err)
	fmt.Printf("GetNodeID: %+v\n", nodeID3)

	p1, err := nh[1].SyncPropose(ctx, nh[1].GetNoOPSession(clusterID), []byte("nodeID"))
	assert.Nil(t, err)
	fmt.Printf("Propose1NodeID: %+v\n", p1.Value)
	p2, err := nh[2].SyncPropose(ctx, nh[2].GetNoOPSession(clusterID), []byte("nodeID"))
	assert.Nil(t, err)
	fmt.Printf("Propose2NodeID: %+v\n", p2.Value)
	p3, err := nh[3].SyncPropose(ctx, nh[3].GetNoOPSession(clusterID), []byte("nodeID"))
	assert.Nil(t, err)
	fmt.Printf("Propose3NodeID: %+v\n", p3.Value)
}
