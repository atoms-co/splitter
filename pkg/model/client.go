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

// UpdateDomainOption represents an option to NewDomain.
type UpdateDomainOption func(*public_v1.UpdateDomainRequest)

// WithUpdateDomainConfig defines config for a domain.
func WithUpdateDomainConfig(config DomainConfig) UpdateDomainOption {
	return func(request *public_v1.UpdateDomainRequest) {
		request.Config = UnwrapDomainConfig(config)
	}
}

// Client is a client for querying placement information.
type Client interface {
	ListTenants(ctx context.Context) ([]TenantInfo, error)
	NewTenant(ctx context.Context, name TenantName, cfg TenantConfig) (TenantInfo, error)
	InfoTenant(ctx context.Context, name TenantName) (TenantInfo, error)
	UpdateTenant(ctx context.Context, tenant TenantInfo, opts ...UpdateTenantOption) (TenantInfo, error)
	DeleteTenant(ctx context.Context, name TenantName) error

	ListDomains(ctx context.Context, name TenantName) ([]DomainInfo, error)
	NewDomain(ctx context.Context, name QualifiedDomainName, domainType DomainType, cfg DomainConfig) (DomainInfo, error)
	InfoDomain(ctx context.Context, name QualifiedDomainName) (DomainInfo, error)
	UpdateDomain(ctx context.Context, domain DomainInfo, opts ...UpdateDomainOption) (DomainInfo, error)
	DeleteDomain(ctx context.Context, name QualifiedDomainName) error

	ListPlacements(ctx context.Context, name TenantName) ([]PlacementInfo, error)
	InfoPlacement(ctx context.Context, name QualifiedPlacementName) (PlacementInfo, error)
}

type client struct {
	management public_v1.ManagementServiceClient
	placement  public_v1.PlacementServiceClient
}

func NewClient(cc *grpc.ClientConn) Client {
	return &client{
		management: public_v1.NewManagementServiceClient(cc),
		placement:  public_v1.NewPlacementServiceClient(cc),
	}
}

func (c *client) ListTenants(ctx context.Context) ([]TenantInfo, error) {
	req := &public_v1.ListTenantsRequest{}
	resp, err := c.management.ListTenants(ctx, req)
	if err != nil {
		return nil, err
	}
	return slicex.Map(resp.GetTenants(), WrapTenantInfo), nil
}

func (c *client) NewTenant(ctx context.Context, name TenantName, cfg TenantConfig) (TenantInfo, error) {
	req := &public_v1.NewTenantRequest{
		Name:   string(name),
		Config: UnwrapTenantConfig(cfg),
	}
	resp, err := c.management.NewTenant(ctx, req)
	if err != nil {
		return TenantInfo{}, err
	}
	return WrapTenantInfo(resp.GetTenant()), nil
}

func (c *client) InfoTenant(ctx context.Context, name TenantName) (TenantInfo, error) {
	req := &public_v1.InfoTenantRequest{Name: string(name)}
	resp, err := c.management.InfoTenant(ctx, req)
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
	return WrapTenantInfo(upd.GetTenant()), nil
}

func (c *client) DeleteTenant(ctx context.Context, name TenantName) error {
	req := &public_v1.DeleteTenantRequest{Name: string(name)}
	_, err := c.management.DeleteTenant(ctx, req)
	return err
}

func (c *client) ListDomains(ctx context.Context, name TenantName) ([]DomainInfo, error) {
	req := &public_v1.ListDomainsRequest{
		Tenant: string(name),
	}
	resp, err := c.management.ListDomains(ctx, req)
	if err != nil {
		return nil, err
	}
	return slicex.Map(resp.GetDomains(), WrapDomainInfo), nil
}

func (c *client) NewDomain(ctx context.Context, name QualifiedDomainName, domainType DomainType, cfg DomainConfig) (DomainInfo, error) {
	req := &public_v1.NewDomainRequest{
		Name:   name.ToProto(),
		Type:   domainType,
		Config: UnwrapDomainConfig(cfg),
	}
	resp, err := c.management.NewDomain(ctx, req)
	if err != nil {
		return DomainInfo{}, err
	}
	return WrapDomainInfo(resp.GetDomain()), nil
}

func (c *client) InfoDomain(ctx context.Context, name QualifiedDomainName) (DomainInfo, error) {
	req := &public_v1.InfoDomainRequest{Name: name.ToProto()}
	resp, err := c.management.InfoDomain(ctx, req)
	if err != nil {
		return DomainInfo{}, err
	}
	return WrapDomainInfo(resp.GetDomain()), nil
}

func (c *client) UpdateDomain(ctx context.Context, domain DomainInfo, opts ...UpdateDomainOption) (DomainInfo, error) {
	req := &public_v1.UpdateDomainRequest{
		Name:    domain.Name().ToProto(),
		Version: int64(domain.Version()),
		Config:  UnwrapDomainConfig(domain.Domain().Config()),
	}
	for _, opt := range opts {
		opt(req)
	}

	upd, err := c.management.UpdateDomain(ctx, req)
	if err != nil {
		return DomainInfo{}, err
	}
	return WrapDomainInfo(upd.GetDomain()), nil
}

func (c *client) DeleteDomain(ctx context.Context, name QualifiedDomainName) error {
	req := &public_v1.DeleteDomainRequest{Name: name.ToProto()}
	_, err := c.management.DeleteDomain(ctx, req)
	return err
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
