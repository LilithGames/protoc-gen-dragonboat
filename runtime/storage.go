package runtime

import (
	"fmt"
	"sync"
)

type IStorage interface {
	Get(key string) (string, error)
	Set(key string, value string) error
}

type MemoryStroage struct {
	mu    sync.RWMutex
	state map[string]string
}

func NewMemoryStroage() IStorage {
	return &MemoryStroage{state: make(map[string]string, 0)}
}

func (it *MemoryStroage) Get(key string) (string, error) {
	it.mu.RLock()
	defer it.mu.RUnlock()
	if v, ok := it.state[key]; ok {
		return v, nil
	}
	return "", fmt.Errorf("NotFound")
}

func (it *MemoryStroage) Set(key string, value string) error {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.state[key] = value
	return nil
}
