package runtime

import (
	"context"
)

type IVoidDragonboatClient interface {
	VoidQuery(ctx context.Context, opts ...DragonboatClientOption) error
	VoidMutate(ctx context.Context, opts ...DragonboatClientOption) error
}

type VoidDragonboatClient struct {
	c IDragonboatClient
}

func NewVoidDragonboatClient(c IDragonboatClient) IVoidDragonboatClient {
	return &VoidDragonboatClient{c}
}

func (it *VoidDragonboatClient) VoidQuery(ctx context.Context, opts ...DragonboatClientOption) error {
	resp, err := it.c.Query(ctx, &DragonboatVoid{}, opts...)
	_, err = ClientResponseConversion[*DragonboatVoid](resp, err)
	return err
}
func (it *VoidDragonboatClient) VoidMutate(ctx context.Context, opts ...DragonboatClientOption) error {
	resp, err := it.c.Mutate(ctx, &DragonboatVoid{}, opts...)
	_, err = ClientResponseConversion[*DragonboatVoid](resp, err)
	return err
}
