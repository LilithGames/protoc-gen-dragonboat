package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	sm "github.com/lni/dragonboat/v3/statemachine"
	vfs "github.com/lni/goutils/vfs"
)

func newNodeHost(nhid uint64, addr string) *dragonboat.NodeHost {
	conf := config.NodeHostConfig{
		NodeHostDir:         fmt.Sprintf("single_nodehost_test_dir_safe_to_delete/%d", nhid),
		AddressByNodeHostID: false,
		RTTMillisecond:      1,
		RaftAddress:         addr,
		NotifyCommit:        false,
		Expert: config.ExpertConfig{
			FS:             vfs.Default,
			TestNodeHostID: nhid,
		},
	}
	nh, err := dragonboat.NewNodeHost(conf)
	if err != nil {
		panic(err)
	}
	return nh
}

func startShard(nh *dragonboat.NodeHost, clusterID uint64, nodeID uint64, initials map[uint64]string, fn interface{}) {
	conf := config.Config{
		NodeID:              nodeID,
		ClusterID:           clusterID,
		CheckQuorum:         true,
		ElectionRTT:         5,
		HeartbeatRTT:        1,
		SnapshotEntries:     0,
		CompactionOverhead:  0,
		OrderedConfigChange: false,
	}
	var err error
	switch f := fn.(type) {
	case sm.CreateStateMachineFunc:
		err = nh.StartCluster(initials, false, f, conf)
	case sm.CreateConcurrentStateMachineFunc:
		err = nh.StartConcurrentCluster(initials, false, f, conf)
	default:
		panic(fmt.Errorf("unknown fn type: %T", f))
	}
	if err != nil {
		panic(err)
	}
}


func waitReady(nh *dragonboat.NodeHost, clusterID uint64) {
	for {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
		defer cancel()
		if _, err := nh.SyncRead(ctx, clusterID, nil); err != nil {
			if errors.Is(err, dragonboat.ErrClusterNotReady) {
				time.Sleep(time.Second)
				continue
			}
		}
		return
	}
}

func newDragonboat(clusterID uint64, initials map[uint64]string, fn any) (map[uint64]*dragonboat.NodeHost, func()) {
	nh := make(map[uint64]*dragonboat.NodeHost, len(initials))
	for nhid, addr := range initials {
		nh[nhid] = newNodeHost(nhid, addr)
	}
	stop := func() {
		for _, n := range nh {
			n.Stop()
		}
		if err := vfs.Default.RemoveAll("single_nodehost_test_dir_safe_to_delete"); err != nil {
			log.Println("[WARN]", fmt.Errorf("vfs.RemoveAll err: %w", err))
		}
	}
	wg := sync.WaitGroup{}
	for nhid, n := range nh {
		startShard(n, clusterID, nhid, initials, fn)
		wg.Add(1)
		go func() {
			defer wg.Done()
			waitReady(n, clusterID)
		}()
	}
	wg.Wait()
	return nh, stop
}

type Bus struct {
	d map[uint64][]func(uint64, any)
	m sync.RWMutex
}

func (it *Bus) SyncPub(topic uint64, event any) {
	it.m.RLock()
	fnlist, ok := it.d[topic]
	if !ok {
		it.m.RUnlock()
		return
	}
	it.m.RUnlock()
	for _, fn := range fnlist {
		fn(topic, event)
	}
}

func (it *Bus) SyncSub(topic uint64, fn func(topic uint64, event any)) {
	it.m.Lock()
	defer it.m.Unlock()
	if it.d == nil {
		it.d = make(map[uint64][]func(uint64, any))
	}
	if fnlist, ok := it.d[topic]; ok {
		it.d[topic] = append(fnlist, fn)
	} else {
		it.d[topic] = []func(uint64, any){fn}
	}
}
