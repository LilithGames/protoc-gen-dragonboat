package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"testing"
	"time"
	"encoding/binary"

	"github.com/lni/dragonboat/v3"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/assert"
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

func tick(nh *dragonboat.NodeHost, clusterID uint64) {
	go func() {
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
			}
		}
	}()
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

func getLeader(nh *dragonboat.NodeHost, clusterID uint64) *uint64 {
	nodeID, valid, err := nh.GetLeaderID(clusterID)
	if err != nil || !valid {
		return nil
	}
	return &nodeID
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
