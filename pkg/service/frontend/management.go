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
	storage storage.Storage // TODO(jhhurwitz): 09/01/2023 Move to leader when forwarding is implemented
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
	existing, err := s.storage.Tenants().Read(ctx, model.TenantName(name))
	if err != nil {
		return nil, model.WrapError(err)
	}
	upd, err := model.UpdateTenant(existing.Tenant(), model.WithTenantConfig(model.WrapTenantConfig(cfg)))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid tenant update: %v", err)
	}
	info, err := s.storage.Tenants().Update(ctx, upd, model.Version(version))
	if err != nil {
		log.Errorf(ctx, "UpdateTenant failed: %v", err)
		return nil, model.WrapError(err)
	}
	return &public_v1.UpdateTenantResponse{
		Tenant: model.UnwrapTenantInfo(info),
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

func (s *ManagementService) ListDomains(ctx context.Context, req *public_v1.ListDomainsRequest) (*public_v1.ListDomainsResponse, error) {
	domains, err := s.storage.Domains().List(ctx, model.TenantName(req.GetTenant()))
	if err != nil {
		log.Errorf(ctx, "ListDomains failed: %v", err)
		return nil, model.WrapError(err)
	}
	return &public_v1.ListDomainsResponse{Domains: slicex.Map(domains, model.UnwrapDomainInfo)}, nil
}

func (s *ManagementService) NewDomain(ctx context.Context, req *public_v1.NewDomainRequest) (*public_v1.NewDomainResponse, error) {
	name, domainType, cfg := req.GetName(), req.GetType(), req.GetConfig()
	fqdn, err := model.ParseQualifiedDomainName(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse domain name: %v", err)
	}
	domain, err := model.NewDomain(fqdn, domainType, s.cl.Now(), model.WithDomainConfig(model.WrapDomainConfig(cfg)))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse domain: %v", err)
	}
	if err := s.storage.Domains().New(ctx, domain); err != nil {
		log.Errorf(ctx, "NewDomain failed: %v", err)
		return nil, model.WrapError(err)
	}
	return &public_v1.NewDomainResponse{
		Domain: model.UnwrapDomain(domain),
	}, nil
}

func (s *ManagementService) ReadDomain(ctx context.Context, req *public_v1.ReadDomainRequest) (*public_v1.ReadDomainResponse, error) {
	fqdn, err := model.ParseQualifiedDomainName(req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse domain name: %v", err)
	}
	domain, err := s.storage.Domains().Read(ctx, fqdn)
	if err != nil {
		log.Errorf(ctx, "ReadDomain failed: %v", err)
		return nil, model.WrapError(err)
	}
	return &public_v1.ReadDomainResponse{Domain: model.UnwrapDomainInfo(domain)}, nil
}

func (s *ManagementService) UpdateDomain(ctx context.Context, req *public_v1.UpdateDomainRequest) (*public_v1.UpdateDomainResponse, error) {
	name, cfg, version := req.GetName(), req.GetConfig(), req.GetVersion()
	fqdn, err := model.ParseQualifiedDomainName(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse domain name: %v", err)
	}
	existing, err := s.storage.Domains().Read(ctx, fqdn)
	if err != nil {
		return nil, model.WrapError(err)
	}
	upd, err := model.UpdateDomain(existing.Domain(), model.WithDomainConfig(model.WrapDomainConfig(cfg)))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid domain update: %v", err)
	}
	info, err := s.storage.Domains().Update(ctx, upd, model.Version(version))
	if err != nil {
		log.Errorf(ctx, "UpdateDomain failed: %v", err)
		return nil, model.WrapError(err)
	}
	return &public_v1.UpdateDomainResponse{
		Domain: model.UnwrapDomainInfo(info),
	}, nil
}

func (s *ManagementService) DeleteDomain(ctx context.Context, req *public_v1.DeleteDomainRequest) (*public_v1.DeleteDomainResponse, error) {
	name := req.GetName()
	fqdn, err := model.ParseQualifiedDomainName(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse domain name: %v", err)
	}
	if err := s.storage.Domains().Delete(ctx, fqdn); err != nil {
		log.Errorf(ctx, "DeleteDomain failed: %v", err)
		return nil, model.WrapError(err)
	}
	return &public_v1.DeleteDomainResponse{}, nil
}
