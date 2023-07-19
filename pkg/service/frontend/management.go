package frontend

import (
	"context"
	"go.atoms.co/splitter/pb"
)

type ManagementService struct {
}

func NewManagementService() *ManagementService {
	return &ManagementService{}
}

func (s *ManagementService) ListTenants(ctx context.Context, request *public_v1.ListTenantsRequest) (*public_v1.ListTenantsResponse, error) {
	return nil, nil
}

func (s *ManagementService) NewTenant(ctx context.Context, request *public_v1.NewTenantRequest) (*public_v1.NewTenantResponse, error) {
	return nil, nil
}

func (s *ManagementService) ReadTenant(ctx context.Context, request *public_v1.ReadTenantRequest) (*public_v1.ReadTenantResponse, error) {
	return nil, nil
}

func (s *ManagementService) UpdateTenant(ctx context.Context, request *public_v1.UpdateTenantRequest) (*public_v1.UpdateTenantResponse, error) {
	return nil, nil
}

func (s *ManagementService) DeleteTenant(ctx context.Context, request *public_v1.DeleteTenantRequest) (*public_v1.DeleteTenantResponse, error) {
	return nil, nil
}
