package runtime

import (
	"sync"
	"fmt"
	"encoding/json"
)

type IVersionManager interface {
	CodeVersion(shardID uint64) string
	DataVersion(shardID uint64) string
	SetDataVersion(shardID uint64, ver string)
	Migrating(shardID uint64) bool
	Commit(shardID uint64)
	Rollback(shardID uint64)
	Dirty(shardID uint64) bool
}

type VersionManager struct {
	mu sync.RWMutex
	storage IStorage
	codeVersion string
	dataVersion map[uint64]string
}

func NewVersionManager(storage IStorage, codeVersion string) IVersionManager {
	it :=  &VersionManager{
		storage: storage,
		codeVersion: codeVersion,
	}
	it.dataVersion = it.load()
	return it
}

func (it *VersionManager) load() map[uint64]string {
	s, err := it.storage.Get("VersionManager")
	if err != nil {
		return make(map[uint64]string, 0)
	}
	var state map[uint64]string
	if err := json.Unmarshal([]byte(s), &state); err != nil {
		panic(fmt.Errorf("json.Unmarshal err: %w", err))
	}
	return state
}
func (it *VersionManager) save(state map[uint64]string) {
	bs, err := json.Marshal(state)
	if err != nil {
		panic(fmt.Errorf("json.Marshal err: %w", err))
	}
	if err := it.storage.Set("VersionManager", string(bs)); err != nil {
		panic(fmt.Errorf("storage.Set err: %w", err))
	}
}

func (it *VersionManager) CodeVersion(shardID uint64) string {
	return it.codeVersion
}

func (it *VersionManager) DataVersion(shardID uint64) string {
	it.mu.RLock()
	defer it.mu.RUnlock()
	if v, ok := it.dataVersion[shardID]; ok {
		return v
	}
	return ""
}

func (it *VersionManager) Migrating(shardID uint64) bool {
	return it.DataVersion(shardID) != it.CodeVersion(shardID)
}

func (it *VersionManager) SetDataVersion(shardID uint64, ver string) {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.dataVersion[shardID] = ver
}

func (it *VersionManager) Commit(shardID uint64) {
	it.mu.Lock()
	defer it.mu.Unlock()
	state := it.load()
	if v, ok := it.dataVersion[shardID]; ok {
		state[shardID] = v
	} else {
		state[shardID] = ""
	}
	it.save(state)
}

func (it *VersionManager) Rollback(shardID uint64) {
	it.mu.Lock()
	defer it.mu.Unlock()
	state := it.load()
	if v, ok := state[shardID]; ok {
		it.dataVersion[shardID] = v
	} else {
		it.dataVersion[shardID] = ""
	}
}

func (it *VersionManager) Dirty(shardID uint64) bool {
	it.mu.RLock()
	defer it.mu.RUnlock()
	state := it.load()
	var v1, v2 string
	if v, ok := it.dataVersion[shardID]; ok {
		v1 = v
	}
	if v, ok := state[shardID]; ok {
		v2 = v
	}
	return v1 != v2
}
