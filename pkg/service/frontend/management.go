package frontend

import (
	"context"
	"go.atoms.co/lib/log"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ManagementService struct {
	resolver leader.Resolver
	proxy    leader.Proxy
}

func NewManagementService(proxy leader.Proxy, resolver leader.Resolver) *ManagementService {
	return &ManagementService{proxy: proxy, resolver: resolver}
}

func (s *ManagementService) ListTenants(ctx context.Context, req *public_v1.ListTenantsRequest) (*public_v1.ListTenantsResponse, error) {
	resp, err := s.invokeTenant(ctx, &internal_v1.TenantRequest{
		Req: &internal_v1.TenantRequest_List{
			List: req,
		},
	})
	return resp.GetList(), model.WrapError(err)
}

func (s *ManagementService) NewTenant(ctx context.Context, req *public_v1.NewTenantRequest) (*public_v1.NewTenantResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty tenant name")
	}

	resp, err := s.invokeTenant(ctx, &internal_v1.TenantRequest{
		Req: &internal_v1.TenantRequest_New{
			New: req,
		},
	})
	return resp.GetNew(), model.WrapError(err)
}

func (s *ManagementService) InfoTenant(ctx context.Context, req *public_v1.InfoTenantRequest) (*public_v1.InfoTenantResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty tenant name")
	}

	resp, err := s.invokeTenant(ctx, &internal_v1.TenantRequest{
		Req: &internal_v1.TenantRequest_Info{
			Info: req,
		},
	})
	return resp.GetInfo(), model.WrapError(err)
}

func (s *ManagementService) UpdateTenant(ctx context.Context, req *public_v1.UpdateTenantRequest) (*public_v1.UpdateTenantResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty tenant name")
	}

	resp, err := s.invokeTenant(ctx, &internal_v1.TenantRequest{
		Req: &internal_v1.TenantRequest_Update{
			Update: req,
		},
	})
	return resp.GetUpdate(), model.WrapError(err)
}

func (s *ManagementService) DeleteTenant(ctx context.Context, req *public_v1.DeleteTenantRequest) (*public_v1.DeleteTenantResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty tenant name")
	}

	resp, err := s.invokeTenant(ctx, &internal_v1.TenantRequest{
		Req: &internal_v1.TenantRequest_Delete{
			Delete: req,
		},
	})
	return resp.GetDelete(), model.WrapError(err)
}

func (s *ManagementService) ListServices(ctx context.Context, req *public_v1.ListServicesRequest) (*public_v1.ListServicesResponse, error) {
	if req.GetTenant() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty tenant name")
	}

	resp, err := s.invokeService(ctx, &internal_v1.ServiceRequest{
		Req: &internal_v1.ServiceRequest_List{
			List: req,
		},
	})
	return resp.GetList(), model.WrapError(err)
}

func (s *ManagementService) NewService(ctx context.Context, req *public_v1.NewServiceRequest) (*public_v1.NewServiceResponse, error) {
	if _, err := model.ParseQualifiedServiceName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeService(ctx, &internal_v1.ServiceRequest{
		Req: &internal_v1.ServiceRequest_New{
			New: req,
		},
	})
	return resp.GetNew(), model.WrapError(err)
}

func (s *ManagementService) InfoService(ctx context.Context, req *public_v1.InfoServiceRequest) (*public_v1.InfoServiceResponse, error) {
	if _, err := model.ParseQualifiedServiceName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeService(ctx, &internal_v1.ServiceRequest{
		Req: &internal_v1.ServiceRequest_Info{
			Info: req,
		},
	})
	return resp.GetInfo(), model.WrapError(err)
}

func (s *ManagementService) UpdateService(ctx context.Context, req *public_v1.UpdateServiceRequest) (*public_v1.UpdateServiceResponse, error) {
	if _, err := model.ParseQualifiedServiceName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeService(ctx, &internal_v1.ServiceRequest{
		Req: &internal_v1.ServiceRequest_Update{
			Update: req,
		},
	})
	return resp.GetUpdate(), model.WrapError(err)
}

func (s *ManagementService) DeleteService(ctx context.Context, req *public_v1.DeleteServiceRequest) (*public_v1.DeleteServiceResponse, error) {
	if _, err := model.ParseQualifiedServiceName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeService(ctx, &internal_v1.ServiceRequest{
		Req: &internal_v1.ServiceRequest_Delete{
			Delete: req,
		},
	})
	return resp.GetDelete(), model.WrapError(err)
}

func (s *ManagementService) ListDomains(ctx context.Context, req *public_v1.ListDomainsRequest) (*public_v1.ListDomainsResponse, error) {
	if _, err := model.ParseQualifiedServiceName(req.GetService()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeDomain(ctx, &internal_v1.DomainRequest{
		Req: &internal_v1.DomainRequest_List{
			List: req,
		},
	})
	return resp.GetList(), model.WrapError(err)
}

func (s *ManagementService) NewDomain(ctx context.Context, req *public_v1.NewDomainRequest) (*public_v1.NewDomainResponse, error) {
	if _, err := model.ParseQualifiedDomainName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeDomain(ctx, &internal_v1.DomainRequest{
		Req: &internal_v1.DomainRequest_New{
			New: req,
		},
	})
	return resp.GetNew(), model.WrapError(err)
}

func (s *ManagementService) UpdateDomain(ctx context.Context, req *public_v1.UpdateDomainRequest) (*public_v1.UpdateDomainResponse, error) {
	if _, err := model.ParseQualifiedDomainName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeDomain(ctx, &internal_v1.DomainRequest{
		Req: &internal_v1.DomainRequest_Update{
			Update: req,
		},
	})
	return resp.GetUpdate(), model.WrapError(err)
}

func (s *ManagementService) DeleteDomain(ctx context.Context, req *public_v1.DeleteDomainRequest) (*public_v1.DeleteDomainResponse, error) {
	if _, err := model.ParseQualifiedDomainName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeDomain(ctx, &internal_v1.DomainRequest{
		Req: &internal_v1.DomainRequest_Delete{
			Delete: req,
		},
	})
	return resp.GetDelete(), model.WrapError(err)
}

func (s *ManagementService) invokeTenant(ctx context.Context, request *internal_v1.TenantRequest) (*internal_v1.TenantResponse, error) {
	req := leader.NewHandleTenantRequest(request)

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.LeaderHandleResponse, error) {
		return core.InvokeExZero(ctx, s.resolver, internal_v1.LeaderServiceClient.Handle, req.Proto, func() (*internal_v1.LeaderHandleResponse, error) {
			return s.proxy.Handle(ctx, req)
		})
	})

	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, model.WrapError(err)
	}
	return resp.GetTenant(), nil
}

func (s *ManagementService) invokeService(ctx context.Context, request *internal_v1.ServiceRequest) (*internal_v1.ServiceResponse, error) {
	req := leader.NewHandleServiceRequest(request)

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.LeaderHandleResponse, error) {
		return core.InvokeExZero(ctx, s.resolver, internal_v1.LeaderServiceClient.Handle, req.Proto, func() (*internal_v1.LeaderHandleResponse, error) {
			return s.proxy.Handle(ctx, req)
		})
	})

	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, model.WrapError(err)
	}
	return resp.GetService(), nil
}

func (s *ManagementService) invokeDomain(ctx context.Context, request *internal_v1.DomainRequest) (*internal_v1.DomainResponse, error) {
	req := leader.NewHandleDomainRequest(request)

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.LeaderHandleResponse, error) {
		return core.InvokeExZero(ctx, s.resolver, internal_v1.LeaderServiceClient.Handle, req.Proto, func() (*internal_v1.LeaderHandleResponse, error) {
			return s.proxy.Handle(ctx, req)
		})
	})

	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, model.WrapError(err)
	}
	return resp.GetDomain(), nil
}
