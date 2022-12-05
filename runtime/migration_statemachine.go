package runtime

import (
	"bufio"
	"fmt"
	"io"
	"log"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

type migrationStateMachine struct {
	ShardID uint64
	NodeID  uint64
	inner   sm.IStateMachine
	core    *migrationCore
}

func NewMigrationStateMachineWrapper(fn sm.CreateStateMachineFunc) sm.CreateStateMachineFunc {
	return func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		inner := fn(clusterID, nodeID)
		migratable, ok := inner.(IMigratable)
		if !ok {
			panic("inner statemachine need to implement IMigratable")
		}
		return &migrationStateMachine{
			ShardID: clusterID,
			NodeID:  nodeID,
			inner:   inner,
			core:    NewMigrationCore(clusterID, nodeID, migratable),
		}
	}
}

func (it *migrationStateMachine) Update(data []byte) (sm.Result, error) {
	if !it.core.isMigrating() {
		r, err := it.inner.Update(data)
		if GetDragonboatResultErrorCode(r) != ErrCodeUnknownRequest {
			return r, err
		}
	}
	r, err := it.core.update(data)
	if GetDragonboatResultErrorCode(r) == ErrCodeUnknownRequest && it.core.isMigrating() {
		it.core.doExpireTick()
		return MakeDragonboatResult(nil, NewDragonboatError(ErrCodeMigrating, "ErrCodeMigrating")), nil
	}
	return r, err
}

func (it *migrationStateMachine) Lookup(query interface{}) (interface{}, error) {
	r, err := it.inner.Lookup(query)
	if GetDragonboatErrorCode(err) != ErrCodeUnknownRequest {
		return r, err
	}
	return it.core.lookup(query)
}

func (it *migrationStateMachine) SaveSnapshot(w io.Writer, fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	if !it.core.canSnapshot() {
		return sm.ErrSnapshotAborted
	}
	bs, err := it.core.save()
	if err != nil {
		return fmt.Errorf("core.save err: %w", err)
	}
	if err := MultiSnapshotWriteItem(w, MigrationStateMachineSnapshotFileID, bs); err != nil {
		return fmt.Errorf("MultiSnapshotWriteItem err: %w", err)
	}
	return it.inner.SaveSnapshot(w, fc, done)
}

func (it *migrationStateMachine) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	rd := bufio.NewReader(r)
	bs, err := MultiSnapshotReadItem(rd, MigrationStateMachineSnapshotFileID)
	if err == nil {
		if err := it.core.load(bs); err != nil {
			return fmt.Errorf("core.load err: %w", err)
		}
	} else {
		// maybe adding new middleware
		log.Println("[WARN]", fmt.Errorf("MultiSnapshotReadItem err: %w", err))
	}
	return it.inner.RecoverFromSnapshot(rd, files, done)
}

func (it *migrationStateMachine) Close() error {
	return it.inner.Close()
}
