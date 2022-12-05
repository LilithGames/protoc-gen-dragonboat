package runtime

import (
	"fmt"
	"io"
	"sync"
	"bufio"
	"log"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

type migrationConcurrentStateMachine struct {
	ShardID uint64
	NodeID  uint64
	inner   sm.IConcurrentStateMachine
	core    *migrationCore
	mu      sync.RWMutex
}

func NewMigrationConcurrencyStateMachineWrapper(fn sm.CreateConcurrentStateMachineFunc) sm.CreateConcurrentStateMachineFunc {
	return func(clusterID uint64, nodeID uint64) sm.IConcurrentStateMachine {
		inner := fn(clusterID, nodeID)
		migratable, ok := inner.(IMigratable)
		if !ok {
			panic("inner statemachine need to implement IMigratable")
		}
		return &migrationConcurrentStateMachine{
			ShardID: clusterID,
			NodeID:  nodeID,
			inner:   inner,
			core:    NewMigrationCore(clusterID, nodeID, migratable),
		}
	}
}

func (it *migrationConcurrentStateMachine) Update(entries []sm.Entry) ([]sm.Entry, error) {
	results := make([]sm.Entry, 0, len(entries))
	for i := range entries {
		result, err := it.updateEntry(entries[i])
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (it *migrationConcurrentStateMachine) updateEntry(entry sm.Entry) (sm.Entry, error) {
	if !it.core.isMigrating() {
		es, err := it.inner.Update([]sm.Entry{entry})
		if GetDragonboatResultErrorCode(es[0].Result) != ErrCodeUnknownRequest {
			return es[0], err
		}
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	r, err := it.core.update(entry.Cmd)
	if GetDragonboatResultErrorCode(r) == ErrCodeUnknownRequest && it.core.isMigrating() {
		it.core.doExpireTick()
		entry.Result = MakeDragonboatResult(nil, NewDragonboatError(ErrCodeMigrating, "ErrCodeMigrating"))
		return entry, err
	}
	entry.Result = r
	return entry, err
}

func (it *migrationConcurrentStateMachine) Lookup(query interface{}) (interface{}, error) {
	r, err := it.inner.Lookup(query)
	if GetDragonboatErrorCode(err) != ErrCodeUnknownRequest {
		return r, err
	}
	it.mu.RLock()
	defer it.mu.RUnlock()
	return it.core.lookup(query)
}

type PreparedSnapshot struct {
	Snapshot []byte
	Abort    bool
}

func (it *migrationConcurrentStateMachine) PrepareSnapshot() (interface{}, error) {
	it.mu.RLock()
	defer it.mu.RUnlock()
	innerSnapshot, err := it.inner.PrepareSnapshot()
	if err != nil {
		return innerSnapshot, err
	}
	snapshots := NewMemoryMultiSnapshot(innerSnapshot)

	if !it.core.canSnapshot() {
		snapshots.Push(MigrationStateMachineSnapshotFileID, &PreparedSnapshot{Snapshot: nil, Abort: true})
	} else {
		coreSnapshot, err := it.core.save()
		if err != nil {
			return nil, fmt.Errorf("core.save err: %w", err)
		}
		snapshots.Push(MigrationStateMachineSnapshotFileID, &PreparedSnapshot{Snapshot: coreSnapshot, Abort: false})
	}

	return snapshots.Next(), nil
}

func (it *migrationConcurrentStateMachine) SaveSnapshot(state interface{}, w io.Writer, fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	snapshots := NewMemoryMultiSnapshot(state)
	coreSnapshot, err := snapshots.Pop(MigrationStateMachineSnapshotFileID)
	if err != nil {
		return fmt.Errorf("snapshots.Pop(MigrationStateMachineSnapshotFileID) err: %w", err)
	}
	corePreparedSnapshot := coreSnapshot.(*PreparedSnapshot)
	if corePreparedSnapshot.Abort {
		return sm.ErrSnapshotAborted
	} else {
		if err := MultiSnapshotWriteItem(w, MigrationStateMachineSnapshotFileID, corePreparedSnapshot.Snapshot); err != nil {
			return fmt.Errorf("MultiSnapshotWriteItem MigrationStateMachineSnapshotFileID err: %w", err)
		}
	}
	return it.inner.SaveSnapshot(snapshots.Next(), w, fc, done)
}

func (it *migrationConcurrentStateMachine) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	rd := bufio.NewReader(r)
	coreSnapshot, err := MultiSnapshotReadItem(rd, MigrationStateMachineSnapshotFileID)
	if err == nil {
		if err := it.core.load(coreSnapshot); err != nil {
			return fmt.Errorf("core.load err: %w", err)
		}
	} else {
		// maybe adding new middleware
		log.Println("[WARN]", fmt.Errorf("MultiSnapshotReadItem err: %w", err))
	}
	return it.inner.RecoverFromSnapshot(rd, files, done)
}

func (it *migrationConcurrentStateMachine) Close() error {
	return it.inner.Close()
}
