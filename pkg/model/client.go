package model

import (
	"context"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pb"
	"google.golang.org/grpc"
)

// UpdateTenantOption represents an option to NewTenant.
type UpdateTenantOption func(*public_v1.UpdateTenantRequest)

// WithUpdateTenantConfig defines config for a tenant.
func WithUpdateTenantConfig(config TenantConfig) UpdateTenantOption {
	return func(request *public_v1.UpdateTenantRequest) {
		request.Config = UnwrapTenantConfig(config)
	}
}

// Client is a client for querying placement information.
type Client interface {
	ListTenants(ctx context.Context) ([]TenantInfo, error)
	NewTenant(ctx context.Context, name TenantName, cfg TenantConfig) (Tenant, error)
	ReadTenant(ctx context.Context, name TenantName) (TenantInfo, error)
	UpdateTenant(ctx context.Context, tenant TenantInfo, opts ...UpdateTenantOption) (TenantInfo, error)
	DeleteTenant(ctx context.Context, name TenantName) error

	ListPlacements(ctx context.Context, name TenantName) ([]PlacementInfo, error)
	InfoPlacement(ctx context.Context, name QualifiedPlacementName) (PlacementInfo, error)
}

type client struct {
	management public_v1.ManagementServiceClient
	placement  public_v1.PlacementServiceClient
}

func (c *client) ListTenants(ctx context.Context) ([]TenantInfo, error) {
	req := &public_v1.ListTenantsRequest{}
	resp, err := c.management.ListTenants(ctx, req)
	if err != nil {
		return nil, err
	}
	return slicex.Map(resp.GetTenants(), WrapTenantInfo), nil
}

func (c *client) NewTenant(ctx context.Context, name TenantName, cfg TenantConfig) (Tenant, error) {
	req := &public_v1.NewTenantRequest{
		Name:   string(name),
		Config: UnwrapTenantConfig(cfg),
	}
	resp, err := c.management.NewTenant(ctx, req)
	if err != nil {
		return Tenant{}, err
	}
	return WrapTenant(resp.GetTenant()), nil
}

func (c *client) ReadTenant(ctx context.Context, name TenantName) (TenantInfo, error) {
	req := &public_v1.ReadTenantRequest{Name: string(name)}
	resp, err := c.management.ReadTenant(ctx, req)
	if err != nil {
		return TenantInfo{}, err
	}
	return WrapTenantInfo(resp.GetTenant()), nil
}

func (c *client) UpdateTenant(ctx context.Context, tenant TenantInfo, opts ...UpdateTenantOption) (TenantInfo, error) {
	req := &public_v1.UpdateTenantRequest{
		Name:    string(tenant.Name()),
		Version: int64(tenant.Version()),
		Config:  UnwrapTenantConfig(tenant.Tenant().Config()),
	}
	for _, opt := range opts {
		opt(req)
	}

	upd, err := c.management.UpdateTenant(ctx, req)
	if err != nil {
		return TenantInfo{}, err
	}
	return WrapTenantInfo(upd.Tenant), nil
}

func (c *client) DeleteTenant(ctx context.Context, name TenantName) error {
	req := &public_v1.DeleteTenantRequest{Name: string(name)}
	_, err := c.management.DeleteTenant(ctx, req)
	return err
}

func NewClient(cc *grpc.ClientConn) Client {
	return &client{
		management: public_v1.NewManagementServiceClient(cc),
		placement:  public_v1.NewPlacementServiceClient(cc),
	}
}

func (c *client) ListPlacements(ctx context.Context, name TenantName) ([]PlacementInfo, error) {
	req := &public_v1.ListPlacementsRequest{
		Tenant: string(name),
	}
	resp, err := c.placement.List(ctx, req)
	if err != nil {
		return nil, err
	}
	return slicex.Map(resp.GetInfo(), WrapPlacementInfo), nil
}

func (c *client) InfoPlacement(ctx context.Context, name QualifiedPlacementName) (PlacementInfo, error) {
	req := &public_v1.InfoPlacementRequest{
		Name: name.ToProto(),
	}
	resp, err := c.placement.Info(ctx, req)
	if err != nil {
		return PlacementInfo{}, err
	}
	return WrapPlacementInfo(resp.GetInfo()), nil
}
