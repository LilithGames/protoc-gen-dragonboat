package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
)


func Test_Core_UpdateMigration(t *testing.T) {
	core := NewMigrationCore(0, 1, nil)
	core.UpdateMigration(&DragonboatUpdateMigrationRequest{Node: &MigrationNode{
		NodeId: 1,
		Version: "ver1",
		Nodes: map[uint64]bool{1: false, 2: false, 3: false},
	}})
	core.state.Epoch = 1
	core.UpdateMigration(&DragonboatUpdateMigrationRequest{Node: &MigrationNode{
		NodeId: 1,
		Version: "ver1",
		Nodes: map[uint64]bool{1: false, 2: false, 3: false},
	}})
	core.UpdateMigration(&DragonboatUpdateMigrationRequest{Node: &MigrationNode{
		NodeId: 2,
		Version: "ver1",
		Nodes: map[uint64]bool{1: false, 2: false, 3: false},
	}})
	core.UpdateMigration(&DragonboatUpdateMigrationRequest{Node: &MigrationNode{
		NodeId: 3,
		Version: "ver1",
		Nodes: map[uint64]bool{1: false, 2: false, 3: false},
	}})
	assert.Equal(t, MigrationStateType_Upgraded, core.state.Type)
}

func Test_getEpochState(t *testing.T) {
	state := map[uint64]*MigrationNodeWrapper{
		1: &MigrationNodeWrapper{
			Epoch: 0,
			Node: &MigrationNode{
				NodeId: 1,
				Version: "ver1",
				Nodes: map[uint64]bool{1: true, 2: true, 3: true},
			},
		},
		2: &MigrationNodeWrapper{
			Epoch: 1,
			Node: &MigrationNode{
				NodeId: 2,
				Version: "ver1",
				Nodes: map[uint64]bool{1: true, 2: true, 3: true},
			},
		},
	}
	nodes := getEpochState(state, 1)
	assert.Equal(t, 1, len(nodes))
}

func Test_checkNodeConsistency_positive(t *testing.T) {
	state := map[uint64]*MigrationNode{
		1: &MigrationNode{
			NodeId: 1,
			Version: "ver1",
			Nodes: map[uint64]bool{1: true, 2: true, 3: true},
		},
		2: &MigrationNode{
			NodeId: 2,
			Version: "ver1",
			Nodes: map[uint64]bool{1: true, 2: true, 3: true},
		},
		3: &MigrationNode{
			NodeId: 3,
			Version: "ver1",
			Nodes: map[uint64]bool{1: true, 2: true, 3: true},
		},
	}
	ok, ver := checkNodeConsistency(state)
	assert.True(t, ok)
	assert.Equal(t, "ver1", ver)
}

func Test_checkNodeConsistency_negtive(t *testing.T) {
	state := map[uint64]*MigrationNode{
		1: &MigrationNode{
			NodeId: 1,
			Version: "ver1",
			Nodes: map[uint64]bool{1: true, 2: true, 3: true},
		},
		2: &MigrationNode{
			NodeId: 2,
			Version: "ver1",
			Nodes: map[uint64]bool{1: true, 2: true, 3: true},
		},
	}
	ok, _ := checkNodeConsistency(state)
	assert.False(t, ok)
}

func Test_checkNodeCouldConsistency_positive(t *testing.T) {
	state := map[uint64]*MigrationNode{
		1: &MigrationNode{
			NodeId: 1,
			Version: "ver1",
			Nodes: map[uint64]bool{1: true, 2: true, 3: true},
		},
		2: &MigrationNode{
			NodeId: 2,
			Version: "ver1",
			Nodes: map[uint64]bool{1: true, 2: true, 3: true},
		},
	}
	node := &MigrationNode{
		NodeId: 3,
		Version: "ver1",
		Nodes: map[uint64]bool{1: true, 2: true, 3: true},
	}
	ok := checkNodeCouldConsistency(state, node)
	assert.True(t, ok)
}

func Test_checkNodeCouldConsistency_negtive(t *testing.T) {
	state := map[uint64]*MigrationNode{
		1: &MigrationNode{
			NodeId: 1,
			Version: "ver1",
			Nodes: map[uint64]bool{1: true, 2: true, 3: true},
		},
		2: &MigrationNode{
			NodeId: 2,
			Version: "ver1",
			Nodes: map[uint64]bool{1: true, 2: true, 3: true},
		},
	}
	node := &MigrationNode{
		NodeId: 3,
		Version: "ver1",
		Nodes: map[uint64]bool{1: true, 2: true, 4: true},
	}
	ok := checkNodeCouldConsistency(state, node)
	assert.False(t, ok)
}
