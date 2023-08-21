package frontend

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ManagementService struct {
	cl      clock.Clock
	storage storage.Storage
}

func NewManagementService(cl clock.Clock, storage storage.Storage) *ManagementService {
	return &ManagementService{
		cl:      cl,
		storage: storage,
	}
}

func (s *ManagementService) ListTenants(ctx context.Context, req *public_v1.ListTenantsRequest) (*public_v1.ListTenantsResponse, error) {
	tenants, err := s.storage.Tenants().List(ctx)
	if err != nil {
		log.Errorf(ctx, "ListTenants failed: %v", err)
		return nil, model.WrapError(err)
	}
	return &public_v1.ListTenantsResponse{Tenants: slicex.Map(tenants, model.UnwrapTenantInfo)}, nil
}

func (s *ManagementService) NewTenant(ctx context.Context, req *public_v1.NewTenantRequest) (*public_v1.NewTenantResponse, error) {
	name, cfg := req.GetName(), req.GetConfig()
	tenant, err := model.NewTenant(model.TenantName(name), s.cl.Now(), model.WithTenantConfig(model.WrapTenantConfig(cfg)))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse tenant: %v", err)
	}
	if err := s.storage.Tenants().New(ctx, tenant); err != nil {
		log.Errorf(ctx, "NewTenant failed: %v", err)
		return nil, model.WrapError(err)
	}
	return &public_v1.NewTenantResponse{
		Tenant: model.UnwrapTenant(tenant),
	}, nil
}

func (s *ManagementService) ReadTenant(ctx context.Context, req *public_v1.ReadTenantRequest) (*public_v1.ReadTenantResponse, error) {
	tenant, err := s.storage.Tenants().Read(ctx, model.TenantName(req.GetName()))
	if err != nil {
		log.Errorf(ctx, "ReadTenant failed: %v", err)
		return nil, model.WrapError(err)
	}
	return &public_v1.ReadTenantResponse{Tenant: model.UnwrapTenantInfo(tenant)}, nil
}

func (s *ManagementService) UpdateTenant(ctx context.Context, req *public_v1.UpdateTenantRequest) (*public_v1.UpdateTenantResponse, error) {
	name, cfg, version := req.GetName(), req.GetConfig(), req.GetVersion()
	tenant, err := model.NewTenant(model.TenantName(name), s.cl.Now(), model.WithTenantConfig(model.WrapTenantConfig(cfg)))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse tenant: %v", err)
	}
	if err := s.storage.Tenants().Update(ctx, tenant, model.Version(version)); err != nil {
		log.Errorf(ctx, "UpdateTenant failed: %v", err)
		return nil, model.WrapError(err)
	}
	version += 1
	return &public_v1.UpdateTenantResponse{
		Tenant: model.UnwrapTenantInfo(model.NewTenantInfo(tenant, model.Version(version), s.cl.Now())), // TODO(jhhurwitz): 08/18/2023: Have db return updated info for accurate time
	}, nil
}

func (s *ManagementService) DeleteTenant(ctx context.Context, req *public_v1.DeleteTenantRequest) (*public_v1.DeleteTenantResponse, error) {
	name := req.GetName()
	if err := s.storage.Tenants().Delete(ctx, model.TenantName(name)); err != nil {
		log.Errorf(ctx, "DeleteTenant failed: %v", err)
		return nil, model.WrapError(err)
	}
	return &public_v1.DeleteTenantResponse{}, nil
}
