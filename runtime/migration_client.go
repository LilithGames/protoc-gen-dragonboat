package runtime

import (
	"context"
	"fmt"
	"errors"

	"github.com/lni/dragonboat/v3"
	"google.golang.org/protobuf/proto"
)

type IMigrationDragonboatClient interface {
	PrepareMigration(ctx context.Context, req *DragonboatPrepareMigrationRequest, opts ...DragonboatClientOption) (*DragonboatPrepareMigrationResponse, error)
	CommitMigration(ctx context.Context, req *DragonboatCommitMigrationRequest, opts ...DragonboatClientOption) (*DragonboatCommitMigrationResponse, error)
	UpdateMigration(ctx context.Context, req *DragonboatUpdateMigrationRequest, opts ...DragonboatClientOption) (*DragonboatUpdateMigrationResponse, error)
	CompleteMigration(ctx context.Context, req *DragonboatCompleteMigrationRequest, opts ...DragonboatClientOption) (*DragonboatCompleteMigrationResponse, error)
	ResetMigration(ctx context.Context, req *DragonboatResetMigrationRequest, opts ...DragonboatClientOption) (*DragonboatResetMigrationResponse, error)
	QueryMigration(ctx context.Context, req *DragonboatQueryMigrationRequest, opts ...DragonboatClientOption) (*DragonboatQueryMigrationResponse, error)
	QueryIndex(ctx context.Context, req *DragonboatQueryIndexRequest, opts ...DragonboatClientOption) (*DragonboatQueryIndexResponse, error)
	RepairMigration(ctx context.Context, req *DragonboatRepairMigrationRequest, opts ...DragonboatClientOption) (*DragonboatRepairMigrationResponse, error)

	CheckVersion(ctx context.Context, version string, opts ...DragonboatClientOption) error
	Migrate(ctx context.Context, req *DragonboatPrepareMigrationRequest, opts ...DragonboatClientOption) error
	UpdateUpgradedNode(ctx context.Context, req *DragonboatUpdateMigrationRequest, opts ...DragonboatClientOption) error
	UpdateSavedNode(ctx context.Context, req *DragonboatCompleteMigrationRequest, opts ...DragonboatClientOption) error
}

type UpdateNodeOption struct {

}

type MigrationDragonboatClient struct {
	c IDragonboatClient
}

func NewMigrationDragonboatClient(c IDragonboatClient) IMigrationDragonboatClient {
	return &MigrationDragonboatClient{c: c}
}
func (it *MigrationDragonboatClient) PrepareMigration(ctx context.Context, req *DragonboatPrepareMigrationRequest, opts ...DragonboatClientOption) (*DragonboatPrepareMigrationResponse, error) {
	resp, err := it.c.Mutate(ctx, req, opts...)
	return ClientResponseConversion[*DragonboatPrepareMigrationResponse](resp, err)
}
func (it *MigrationDragonboatClient) CommitMigration(ctx context.Context, req *DragonboatCommitMigrationRequest, opts ...DragonboatClientOption) (*DragonboatCommitMigrationResponse, error) {
	resp, err := it.c.Mutate(ctx, req, opts...)
	return ClientResponseConversion[*DragonboatCommitMigrationResponse](resp, err)
}
func (it *MigrationDragonboatClient) UpdateMigration(ctx context.Context, req *DragonboatUpdateMigrationRequest, opts ...DragonboatClientOption) (*DragonboatUpdateMigrationResponse, error) {
	resp, err := it.c.Mutate(ctx, req, opts...)
	return ClientResponseConversion[*DragonboatUpdateMigrationResponse](resp, err)
}
func (it *MigrationDragonboatClient) CompleteMigration(ctx context.Context, req *DragonboatCompleteMigrationRequest, opts ...DragonboatClientOption) (*DragonboatCompleteMigrationResponse, error) {
	resp, err := it.c.Mutate(ctx, req, opts...)
	return ClientResponseConversion[*DragonboatCompleteMigrationResponse](resp, err)
}
func (it *MigrationDragonboatClient) ResetMigration(ctx context.Context, req *DragonboatResetMigrationRequest, opts ...DragonboatClientOption) (*DragonboatResetMigrationResponse, error) {
	resp, err := it.c.Mutate(ctx, req, opts...)
	return ClientResponseConversion[*DragonboatResetMigrationResponse](resp, err)
}
func (it *MigrationDragonboatClient) QueryMigration(ctx context.Context, req *DragonboatQueryMigrationRequest, opts ...DragonboatClientOption) (*DragonboatQueryMigrationResponse, error) {
	resp, err := it.c.Query(ctx, req, opts...)
	return ClientResponseConversion[*DragonboatQueryMigrationResponse](resp, err)
}
func (it *MigrationDragonboatClient) QueryIndex(ctx context.Context, req *DragonboatQueryIndexRequest, opts ...DragonboatClientOption) (*DragonboatQueryIndexResponse, error) {
	resp, err := it.c.Query(ctx, req, opts...)
	return ClientResponseConversion[*DragonboatQueryIndexResponse](resp, err)
}
func (it *MigrationDragonboatClient) RepairMigration(ctx context.Context, req *DragonboatRepairMigrationRequest, opts ...DragonboatClientOption) (*DragonboatRepairMigrationResponse, error) {
	resp, err := it.c.Mutate(ctx, req, opts...)
	return ClientResponseConversion[*DragonboatRepairMigrationResponse](resp, err)
}

func (it *MigrationDragonboatClient) CheckVersion(ctx context.Context, version string, opts ...DragonboatClientOption) error {
	q, err := it.QueryMigration(ctx, &DragonboatQueryMigrationRequest{}, opts...)
	if err != nil {
		return fmt.Errorf("QueryMigration err: %w", err)
	}
	if q.View.Type == MigrationStateType_Upgrading {
		return nil
	}
	if q.View.Version != version {
		return NewDragonboatError(ErrCodeVersionNotMatch, fmt.Sprintf("current version: %s, local version: %s", q.View.Version, version))
	}
	return nil
}

func (it *MigrationDragonboatClient) Migrate(ctx context.Context, req *DragonboatPrepareMigrationRequest, opts ...DragonboatClientOption) error {
	mi, err := it.QueryMigration(ctx, &DragonboatQueryMigrationRequest{}, opts...)
	if err != nil {
		return fmt.Errorf("QueryMigration err: %w", err)
	}
	t := mi.View.Type
	if t == MigrationStateType_Upgrading {
		return NewDragonboatError(ErrCodeAlreadyMigrating, "migration already upgrading, please do upgrade")
	}
	if t == MigrationStateType_Upgraded {
		return NewDragonboatError(ErrCodeAlreadyMigrating, "migration already upgraded, please do save")
	}
	if t == MigrationStateType_Expired || mi.View.Type == MigrationStateType_Migrating {
		if _, err := it.ResetMigration(ctx, &DragonboatResetMigrationRequest{}, opts...); err != nil {
			return fmt.Errorf("ResetMigration err: %w", err)
		}
		t = MigrationStateType_Empty
	}
	if t == MigrationStateType_Empty {
		prepare, err := it.PrepareMigration(ctx, req, opts...)
		if err != nil {
			return fmt.Errorf("PrepareMigration err: %w", err)
		}
		if _, err := it.CommitMigration(ctx, &DragonboatCommitMigrationRequest{Migration: prepare.Migration}, opts...); err != nil {
			return fmt.Errorf("CommitMigration err: %w", err)
		}
		return nil
	}
	return NewDragonboatError(ErrCodeMigrationInvalid, fmt.Sprintf("unknown MigrationStateType type %s", t.String()))
}

func (it *MigrationDragonboatClient) UpdateUpgradedNode(ctx context.Context, req *DragonboatUpdateMigrationRequest, opts ...DragonboatClientOption) error {
	q, err := it.QueryMigration(ctx, &DragonboatQueryMigrationRequest{}, opts...)
	if err != nil {
		return fmt.Errorf("QueryNodeMigration err: %w", err)
	}
	if q.View.Type != MigrationStateType_Upgrading {
		return NewDragonboatError(ErrCodeMigrationInvalid, fmt.Sprintf("ErrCodeMigrationInvalid: %s", q.View.Type.String()))
	}
	if node, ok := q.View.NodeUpgraded[req.Node.NodeId]; ok {
		if node.Epoch == q.View.Epoch {
			if proto.Equal(node.Node, req.Node) {
				return nil
			}
		}
	}
	if _, err := it.UpdateMigration(ctx, req, opts...); err != nil {
		return fmt.Errorf("UpdateMigration(%v) err: %w", req, err)
	}
	return nil
}
func (it *MigrationDragonboatClient) UpdateSavedNode(ctx context.Context, req *DragonboatCompleteMigrationRequest, opts ...DragonboatClientOption) error {
	q, err := it.QueryMigration(ctx, &DragonboatQueryMigrationRequest{}, opts...)
	if err != nil {
		return fmt.Errorf("QueryNodeMigration err: %w", err)
	}
	if q.View.Type != MigrationStateType_Upgraded {
		return NewDragonboatError(ErrCodeMigrationInvalid, fmt.Sprintf("ErrCodeMigrationInvalid: %s", q.View.Type.String()))
	}
	if q.View.Version != req.Node.Version {
		return NewDragonboatError(ErrCodeMigrationInvalid, fmt.Sprintf("VersionNotMatch expected: %s, actually: %s", q.View.Version, req.Node.Version))
	}
	node, ok := q.View.NodeSaved[req.Node.NodeId]
	if !ok {
		if _, err := it.c.Save(ctx, append(opts, DragonboatClientCompactOverhead(0))...); err != nil && !errors.Is(err, dragonboat.ErrRejected) {
			return fmt.Errorf("Save err: %w", err)
		}
	} else {
		if node.Epoch == q.View.Epoch {
			if proto.Equal(node.Node, req.Node) {
				return nil
			}
		}
	}
	if _, err := it.CompleteMigration(ctx, req, opts...); err != nil {
		return fmt.Errorf("CompleteMigration(%d) err: %w", req.Node.NodeId, err)
	}
	return nil
}
