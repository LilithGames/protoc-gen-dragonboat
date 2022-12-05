package runtime

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"reflect"

	"github.com/golang/snappy"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"google.golang.org/protobuf/proto"
)

const MigrationStateMachineSnapshotFileID uint64 = 29254309
const RepairMigrationConfirmText string = "REPAIRMIGRATION_MAY_CAUSE_INCONSISTENCY_AND_I_KNOW_WHAT_I_AM_DOING"

type IMigratable interface {
	// return error meas panic
	SaveState(*SaveStateRequest) (*SaveStateResponse, error)
	// return error means panic
	Migrate(*MigrateRequest) (*MigrateResponse, error)
}

type SaveStateRequest struct {
	Writer io.Writer
}
type SaveStateResponse struct{}
type MigrateRequest struct {
	Reader      io.Reader
	Version string
}
type MigrateResponse struct{}

type PassthroughMigratable struct {
	inner IMigratable
}

func NewPassthroughMigratable(inner IMigratable) *PassthroughMigratable {
	return &PassthroughMigratable{inner}
}
func (it *PassthroughMigratable) SaveState(req *SaveStateRequest) (*SaveStateResponse, error) {
	return it.inner.SaveState(req)
}
func (it *PassthroughMigratable) Migrate(req *MigrateRequest) (*MigrateResponse, error) {
	return it.inner.Migrate(req)
}

type migrationCore struct {
	ShardID uint64
	NodeID  uint64

	migratable IMigratable
	state      *MigrationState
}

func NewMigrationCore(shardID uint64, nodeID uint64, migratable IMigratable) *migrationCore {
	return &migrationCore{
		ShardID:    shardID,
		NodeID:     nodeID,
		migratable: migratable,
		state: &MigrationState{
			Version: "",
			Type:    MigrationStateType_Upgrading,
		},
	}
}

func (it *migrationCore) PrepareMigration(req *DragonboatPrepareMigrationRequest) (*DragonboatPrepareMigrationResponse, error) {
	if it.state.Type != MigrationStateType_Empty {
		return nil, NewDragonboatError(ErrCodeMigrationInvalid, "ErrCodeMigrationInvalid")
	}
	var buffer bytes.Buffer
	if req.CompressionType == CompressionType_None {
		w := bufio.NewWriter(&buffer)
		if _, err := it.migratable.SaveState(&SaveStateRequest{Writer: w}); err != nil {
			panic(fmt.Errorf("migratable.SaveState err: %w", err))
		}
		w.Flush()
	} else if req.CompressionType == CompressionType_Snappy {
		w := snappy.NewBufferedWriter(&buffer)
		if _, err := it.migratable.SaveState(&SaveStateRequest{Writer: w}); err != nil {
			panic(fmt.Errorf("migratable.SaveState err: %w", err))
		}
		w.Close()
	} else {
		return nil, NewDragonboatError(ErrCodeMigrationInvalid, fmt.Sprintf("unknown compression type: %s", req.CompressionType.String()))
	}
	it.state.Expire = req.Expire
	it.state.Type = MigrationStateType_Migrating
	log.Println("[INFO]", fmt.Sprintf("MigrationCore: shard %d prepare migration with %s", it.ShardID, it.state.Version))
	return &DragonboatPrepareMigrationResponse{
		Migration: &Migration{
			State:           buffer.Bytes(),
			CompressionType: req.CompressionType,
		},
	}, nil
}

func (it *migrationCore) CommitMigration(req *DragonboatCommitMigrationRequest) (*DragonboatCommitMigrationResponse, error) {
	if it.state.Type == MigrationStateType_Expired {
		return nil, NewDragonboatError(ErrCodeMigrationExpired, "ErrCodeMigrationExpired")
	}
	if it.state.Type != MigrationStateType_Migrating {
		return nil, NewDragonboatError(ErrCodeMigrationInvalid, "ErrCodeMigrationInvalid")
	}

	buffer := bytes.NewBuffer(req.Migration.State)
	var rd io.Reader
	if req.Migration.CompressionType == CompressionType_None {
		rd = bufio.NewReader(buffer)
	} else if req.Migration.CompressionType == CompressionType_Snappy {
		rd = snappy.NewReader(buffer)
	}

	if _, err := it.migratable.Migrate(&MigrateRequest{Reader: rd, Version: it.state.Version}); err != nil {
		panic(fmt.Errorf("migratable.Migrate err: %w", err))
	}
	it.state.Expire = 0
	it.state.Type = MigrationStateType_Upgrading
	it.state.NodeUpgraded = nil
	it.state.Epoch = 0
	it.state.StateIndex++
	log.Println("[INFO]", fmt.Sprintf("MigrationCore: shard %d migration commit with version %s", it.ShardID, it.state.Version))
	return &DragonboatCommitMigrationResponse{Type: it.state.Type}, nil
}

func (it *migrationCore) UpdateMigration(req *DragonboatUpdateMigrationRequest) (*DragonboatUpdateMigrationResponse, error) {
	if it.state.Type != MigrationStateType_Upgrading {
		return nil, NewDragonboatError(ErrCodeMigrationInvalid, "ErrCodeMigrationInvalid")
	}
	if it.state.NodeUpgraded == nil {
		it.state.NodeUpgraded = make(map[uint64]*MigrationNodeWrapper, 0)
	}
	dirty := false
	if n, ok := it.state.NodeUpgraded[req.Node.NodeId]; ok {
		if n.Epoch == it.state.Epoch {
			if proto.Equal(n.Node, req.Node) {
				return &DragonboatUpdateMigrationResponse{Updated: 0}, nil
			}
			it.state.Epoch++
		}
		it.state.NodeUpgraded[req.Node.NodeId] = &MigrationNodeWrapper{Node: req.Node, Epoch: it.state.Epoch}
		dirty = true
	} else {
		if !checkNodeCouldConsistency(getEpochState(it.state.NodeUpgraded, it.state.Epoch), req.Node) {
			it.state.Epoch++
		}
		it.state.NodeUpgraded[req.Node.NodeId] = &MigrationNodeWrapper{Node: req.Node, Epoch: it.state.Epoch}
		dirty = true
	}

	if ok, version := checkNodeConsistency(getEpochState(it.state.NodeUpgraded, it.state.Epoch)); ok {
		oldVersion := it.state.Version
		it.state.Type = MigrationStateType_Upgraded
		it.state.Version = version
		it.state.Epoch = 0
		dirty = true
		log.Println("[INFO]", fmt.Sprintf("MigrationCore: shard %d all node upgraded with version %s => %s", it.ShardID, oldVersion, version))
	}
	if dirty {
		it.state.StateIndex++
		return &DragonboatUpdateMigrationResponse{Updated: 1}, nil
	}
	return &DragonboatUpdateMigrationResponse{Updated: 0}, nil
}

func (it *migrationCore) CompleteMigration(req *DragonboatCompleteMigrationRequest) (*DragonboatCompleteMigrationResponse, error) {
	if it.state.Type != MigrationStateType_Upgraded {
		return nil, NewDragonboatError(ErrCodeMigrationInvalid, "ErrCodeMigrationInvalid")
	}
	if it.state.Version != req.Node.Version {
		return nil, NewDragonboatError(ErrCodeMigrationInvalid, fmt.Sprintf("VersionNotMatch expected: %s, actually: %s", it.state.Version, req.Node.Version))
	}
	if it.state.NodeSaved == nil {
		it.state.NodeSaved = make(map[uint64]*MigrationNodeWrapper, 0)
	}
	dirty := false
	if n, ok := it.state.NodeSaved[req.Node.NodeId]; ok {
		if n.Epoch == it.state.Epoch {
			if proto.Equal(n.Node, req.Node) {
				return &DragonboatCompleteMigrationResponse{Updated: 0}, nil
			}
			it.state.Epoch++
		}
		it.state.NodeSaved[req.Node.NodeId] = &MigrationNodeWrapper{Node: req.Node, Epoch: it.state.Epoch}
		dirty = true
	} else {
		if !checkNodeCouldConsistency(getEpochState(it.state.NodeSaved, it.state.Epoch), req.Node) {
			it.state.Epoch++
		}
		it.state.NodeSaved[req.Node.NodeId] = &MigrationNodeWrapper{Node: req.Node, Epoch: it.state.Epoch}
		dirty = true
	}

	if ok, _ := checkNodeConsistency(getEpochState(it.state.NodeSaved, it.state.Epoch)); ok {
		it.resetState()
		dirty = true
		log.Println("[INFO]", fmt.Sprintf("MigrationCore: shard %d all node saved with version %s", it.ShardID, it.state.Version))
	}
	if dirty {
		it.state.StateIndex++
		return &DragonboatCompleteMigrationResponse{Updated: 1}, nil
	}
	return &DragonboatCompleteMigrationResponse{Updated: 0}, nil
}

func (it *migrationCore) ResetMigration(req *DragonboatResetMigrationRequest) (*DragonboatResetMigrationResponse, error) {
	if it.state.Type == MigrationStateType_Empty {
		return &DragonboatResetMigrationResponse{}, nil
	}
	if it.state.Type == MigrationStateType_Expired {
		it.resetState()
		it.state.StateIndex++
		return &DragonboatResetMigrationResponse{}, nil
	}
	if it.state.Type == MigrationStateType_Migrating {
		it.resetState()
		it.state.StateIndex++
		return &DragonboatResetMigrationResponse{}, nil
	}
	if it.state.Type == MigrationStateType_Upgrading {
		return nil, NewDragonboatError(ErrCodeMigrationInvalid, "ErrCodeMigrationInvalid")
	}
	if it.state.Type == MigrationStateType_Upgraded {
		it.state.Type = MigrationStateType_Upgrading
		it.state.NodeUpgraded = nil
		it.state.StateIndex++
		return &DragonboatResetMigrationResponse{}, nil
	}
	return nil, NewDragonboatError(ErrCodeMigrationInvalid, "ErrCodeMigrationInvalid")
}

func (it *migrationCore) QueryMigration(req *DragonboatQueryMigrationRequest) (*DragonboatQueryMigrationResponse, error) {
	view := MigrationStateView(*it.state)
	return &DragonboatQueryMigrationResponse{
		View: &view,
	}, nil
}

func (it *migrationCore) QueryIndex(req *DragonboatQueryIndexRequest) (*DragonboatQueryIndexResponse, error) {
	return &DragonboatQueryIndexResponse{StateIndex: it.state.StateIndex}, nil
}

func (it *migrationCore) RepairMigration(req *DragonboatRepairMigrationRequest) (*DragonboatRepairMigrationResponse, error) {
	if req.Confirm != RepairMigrationConfirmText {
		return nil, NewDragonboatError(ErrCodeMigrationInvalid, fmt.Sprintf("confirm text not match: %s", req.Confirm))
	}
	it.resetState()
	it.state.Version = req.CurrentVersion
	it.state.StateIndex++
	return &DragonboatRepairMigrationResponse{}, nil
}

func (it *migrationCore) resetState() {
	it.state.Type = MigrationStateType_Empty
	it.state.Expire = 0
	it.state.Epoch = 0
	it.state.NodeUpgraded = nil
	it.state.NodeSaved = nil
}

func (it *migrationCore) dispatch(msg proto.Message) (proto.Message, error) {
	switch m := msg.(type) {
	case *DragonboatPrepareMigrationRequest:
		return it.PrepareMigration(m)
	case *DragonboatCommitMigrationRequest:
		return it.CommitMigration(m)
	case *DragonboatResetMigrationRequest:
		return it.ResetMigration(m)
	case *DragonboatQueryMigrationRequest:
		return it.QueryMigration(m)
	case *DragonboatQueryIndexRequest:
		return it.QueryIndex(m)
	case *DragonboatUpdateMigrationRequest:
		return it.UpdateMigration(m)
	case *DragonboatCompleteMigrationRequest:
		return it.CompleteMigration(m)
	case *DragonboatRepairMigrationRequest:
		return it.RepairMigration(m)
	default:
		return nil, NewDragonboatError(ErrCodeUnknownRequest, "ErrCodeUnknownRequest")
	}
}
func (it *migrationCore) update(data []byte) (sm.Result, error) {
	msg, err := ParseDragonboatRequest(data)
	if err != nil {
		return MakeDragonboatResult(nil, NewDragonboatError(ErrCodeUnknownRequest, err.Error())), nil
	}
	resp, err := it.dispatch(msg)
	if err != nil {
		return MakeDragonboatResult(resp, fmt.Errorf("update dispatch err: %w", err)), nil
	}
	return MakeDragonboatResult(resp, nil), nil
}

func (it *migrationCore) lookup(query interface{}) (interface{}, error) {
	q, ok := query.(proto.Message)
	if !ok {
		return nil, NewDragonboatError(ErrCodeUnknownRequest, "query is not proto.Message")
	}
	resp, err := it.dispatch(q)
	if err != nil {
		return resp, fmt.Errorf("lookup dispatch err: %w", err)
	}
	return resp, nil

}

func (it *migrationCore) save() ([]byte, error) {
	bs, err := proto.Marshal(it.state)
	if err != nil {
		return nil, fmt.Errorf("proto.Marshal err: %w", err)
	}
	log.Println("[INFO]", fmt.Sprintf("MigrationCore: shard %d save snapshot with version %s", it.ShardID, it.state.Version))
	return bs, nil
}

func (it *migrationCore) load(bs []byte) error {
	var state MigrationState
	if err := proto.Unmarshal(bs, &state); err != nil {
		return fmt.Errorf("proto.Unmarshhal err: %w", err)
	}
	it.state = &state
	log.Println("[INFO]", fmt.Sprintf("MigrationCore: shard %d load snapshot with verson: %s", it.ShardID, it.state.Version))
	return nil
}

func (it *migrationCore) doExpireTick() {
	if it.state.Type != MigrationStateType_Migrating {
		return
	}
	if it.state.Expire == 0 {
		it.state.Type = MigrationStateType_Expired
		return
	}
	it.state.Expire--
	if it.state.Expire == 0 {
		it.state.Type = MigrationStateType_Expired
		return
	}
}

func (it *migrationCore) isMigrating() bool {
	return it.state.Type == MigrationStateType_Migrating
}

func (it *migrationCore) canSnapshot() bool {
	if it.state.Type == MigrationStateType_Empty {
		return true
	}
	if it.state.Type == MigrationStateType_Expired {
		return true
	}
	if it.state.Type == MigrationStateType_Upgraded {
		return true
	}
	return false
}

func sliceToMap[T, U, R comparable](items []T, fn func(item T) (U, R)) map[U]R {
	result := make(map[U]R, len(items))
	for i := range items {
		k, v := fn(items[i])
		result[k] = v
	}
	return result
}

func mapToMap[K1, V1, K2, V2 comparable](items map[K1]V1, fn func(k K1, v V1) (K2, V2)) map[K2]V2 {
	result := make(map[K2]V2, len(items))
	for k, v := range items {
		k2, v2 := fn(k, v)
		result[k2] = v2
	}
	return result
}

func getEpochState(state map[uint64]*MigrationNodeWrapper, epoch uint64) map[uint64]*MigrationNode {
	nodes := make(map[uint64]*MigrationNode, 0)
	for k, v := range state {
		if v.Epoch == epoch {
			nodes[k] = v.Node
		}
	}
	return nodes
}

func checkNodeConsistency(state map[uint64]*MigrationNode) (bool, string) {
	var version string
	members := mapToMap(state, func(k1 uint64, v1 *MigrationNode)(uint64, struct{}) {
		return k1, struct{}{}
	})
	for _, node := range state {
		nodes := mapToMap(node.Nodes, func(k1 uint64, v1 bool) (uint64, struct{}) {
			return k1, struct{}{}
		})
		if !reflect.DeepEqual(members, nodes) {
			return false, version
		}
		if version == "" {
			version = node.Version
		} else if version != node.Version {
			return false, version
		}
	}
	return true, version
}

func checkNodeCouldConsistency(state map[uint64]*MigrationNode, node *MigrationNode) bool {
	if _, ok := node.Nodes[node.NodeId]; !ok {
		return false
	}
	for _, n := range state {
		if _, ok := node.Nodes[n.NodeId]; !ok {
			return false
		}
		if !reflect.DeepEqual(n.Nodes, node.Nodes) {
			return false
		}
		if n.Version != node.Version {
			return false
		}
	}
	
	return true
}
