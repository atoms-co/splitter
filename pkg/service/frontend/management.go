package frontend

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.atoms.co/lib/log"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
	splitterpb "go.atoms.co/splitter/pb"
)

type ManagementService struct {
	resolver leader.Resolver
	proxy    leader.Proxy
}

func NewManagementService(proxy leader.Proxy, resolver leader.Resolver) *ManagementService {
	return &ManagementService{proxy: proxy, resolver: resolver}
}

func (s *ManagementService) ListTenants(ctx context.Context, req *splitterpb.ListTenantsRequest) (*splitterpb.ListTenantsResponse, error) {
	resp, err := s.invokeTenant(ctx, &splitterprivatepb.TenantRequest{
		Req: &splitterprivatepb.TenantRequest_List{
			List: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetList(), nil
}

func (s *ManagementService) NewTenant(ctx context.Context, req *splitterpb.NewTenantRequest) (*splitterpb.NewTenantResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty tenant name")
	}

	resp, err := s.invokeTenant(ctx, &splitterprivatepb.TenantRequest{
		Req: &splitterprivatepb.TenantRequest_New{
			New: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetNew(), nil
}

func (s *ManagementService) InfoTenant(ctx context.Context, req *splitterpb.InfoTenantRequest) (*splitterpb.InfoTenantResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty tenant name")
	}

	resp, err := s.invokeTenant(ctx, &splitterprivatepb.TenantRequest{
		Req: &splitterprivatepb.TenantRequest_Info{
			Info: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetInfo(), nil
}

func (s *ManagementService) UpdateTenant(ctx context.Context, req *splitterpb.UpdateTenantRequest) (*splitterpb.UpdateTenantResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty tenant name")
	}

	resp, err := s.invokeTenant(ctx, &splitterprivatepb.TenantRequest{
		Req: &splitterprivatepb.TenantRequest_Update{
			Update: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetUpdate(), nil
}

func (s *ManagementService) DeleteTenant(ctx context.Context, req *splitterpb.DeleteTenantRequest) (*splitterpb.DeleteTenantResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty tenant name")
	}

	resp, err := s.invokeTenant(ctx, &splitterprivatepb.TenantRequest{
		Req: &splitterprivatepb.TenantRequest_Delete{
			Delete: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetDelete(), nil
}

func (s *ManagementService) ListServices(ctx context.Context, req *splitterpb.ListServicesRequest) (*splitterpb.ListServicesResponse, error) {
	if req.GetTenant() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty tenant name")
	}

	resp, err := s.invokeService(ctx, &splitterprivatepb.ServiceRequest{
		Req: &splitterprivatepb.ServiceRequest_List{
			List: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetList(), nil
}

func (s *ManagementService) NewService(ctx context.Context, req *splitterpb.NewServiceRequest) (*splitterpb.NewServiceResponse, error) {
	if _, err := model.ParseQualifiedServiceName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeService(ctx, &splitterprivatepb.ServiceRequest{
		Req: &splitterprivatepb.ServiceRequest_New{
			New: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetNew(), nil
}

func (s *ManagementService) InfoService(ctx context.Context, req *splitterpb.InfoServiceRequest) (*splitterpb.InfoServiceResponse, error) {
	if _, err := model.ParseQualifiedServiceName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeService(ctx, &splitterprivatepb.ServiceRequest{
		Req: &splitterprivatepb.ServiceRequest_Info{
			Info: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetInfo(), nil
}

func (s *ManagementService) UpdateService(ctx context.Context, req *splitterpb.UpdateServiceRequest) (*splitterpb.UpdateServiceResponse, error) {
	if _, err := model.ParseQualifiedServiceName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeService(ctx, &splitterprivatepb.ServiceRequest{
		Req: &splitterprivatepb.ServiceRequest_Update{
			Update: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetUpdate(), nil
}

func (s *ManagementService) DeleteService(ctx context.Context, req *splitterpb.DeleteServiceRequest) (*splitterpb.DeleteServiceResponse, error) {
	if _, err := model.ParseQualifiedServiceName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeService(ctx, &splitterprivatepb.ServiceRequest{
		Req: &splitterprivatepb.ServiceRequest_Delete{
			Delete: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetDelete(), nil
}

func (s *ManagementService) ListDomains(ctx context.Context, req *splitterpb.ListDomainsRequest) (*splitterpb.ListDomainsResponse, error) {
	if _, err := model.ParseQualifiedServiceName(req.GetService()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeDomain(ctx, &splitterprivatepb.DomainRequest{
		Req: &splitterprivatepb.DomainRequest_List{
			List: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetList(), nil
}

func (s *ManagementService) NewDomain(ctx context.Context, req *splitterpb.NewDomainRequest) (*splitterpb.NewDomainResponse, error) {
	if _, err := model.ParseQualifiedDomainName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeDomain(ctx, &splitterprivatepb.DomainRequest{
		Req: &splitterprivatepb.DomainRequest_New{
			New: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetNew(), nil
}

func (s *ManagementService) UpdateDomain(ctx context.Context, req *splitterpb.UpdateDomainRequest) (*splitterpb.UpdateDomainResponse, error) {
	if _, err := model.ParseQualifiedDomainName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeDomain(ctx, &splitterprivatepb.DomainRequest{
		Req: &splitterprivatepb.DomainRequest_Update{
			Update: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetUpdate(), nil
}

func (s *ManagementService) DeleteDomain(ctx context.Context, req *splitterpb.DeleteDomainRequest) (*splitterpb.DeleteDomainResponse, error) {
	if _, err := model.ParseQualifiedDomainName(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := s.invokeDomain(ctx, &splitterprivatepb.DomainRequest{
		Req: &splitterprivatepb.DomainRequest_Delete{
			Delete: req,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetDelete(), nil
}

func (s *ManagementService) invokeTenant(ctx context.Context, request *splitterprivatepb.TenantRequest) (*splitterprivatepb.TenantResponse, error) {
	req := leader.NewHandleTenantRequest(request)

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*splitterprivatepb.LeaderHandleResponse, error) {
		return core.InvokeZero(ctx, s.resolver, splitterprivatepb.LeaderServiceClient.Handle, req.Proto, func() (*splitterprivatepb.LeaderHandleResponse, error) {
			return s.proxy.Handle(ctx, req)
		})
	})

	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, err
	}
	return resp.GetTenant(), nil
}

func (s *ManagementService) invokeService(ctx context.Context, request *splitterprivatepb.ServiceRequest) (*splitterprivatepb.ServiceResponse, error) {
	req := leader.NewHandleServiceRequest(request)

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*splitterprivatepb.LeaderHandleResponse, error) {
		return core.InvokeZero(ctx, s.resolver, splitterprivatepb.LeaderServiceClient.Handle, req.Proto, func() (*splitterprivatepb.LeaderHandleResponse, error) {
			return s.proxy.Handle(ctx, req)
		})
	})

	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, err
	}
	return resp.GetService(), nil
}

func (s *ManagementService) invokeDomain(ctx context.Context, request *splitterprivatepb.DomainRequest) (*splitterprivatepb.DomainResponse, error) {
	req := leader.NewHandleDomainRequest(request)

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*splitterprivatepb.LeaderHandleResponse, error) {
		return core.InvokeZero(ctx, s.resolver, splitterprivatepb.LeaderServiceClient.Handle, req.Proto, func() (*splitterprivatepb.LeaderHandleResponse, error) {
			return s.proxy.Handle(ctx, req)
		})
	})

	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, err
	}
	return resp.GetDomain(), nil
}
