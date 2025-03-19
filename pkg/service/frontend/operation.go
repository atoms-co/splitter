package frontend

import (
	"context"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.atoms.co/lib/log"
	"go.atoms.co/splitter/pkg/cluster"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pkg/service/worker"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

type OperationService struct {
	cluster         cluster.Cluster
	worker          worker.Worker
	serviceResolver core.ServiceResolver
	resolver        leader.Resolver
	proxy           leader.Proxy
}

func NewOperationService(c cluster.Cluster, w worker.Worker, serviceResolver core.ServiceResolver, resolver leader.Resolver, proxy leader.Proxy) *OperationService {
	return &OperationService{
		cluster:         c,
		worker:          w,
		serviceResolver: serviceResolver,
		resolver:        resolver,
		proxy:           proxy,
	}
}

func (o *OperationService) CoordinatorInfo(ctx context.Context, request *splitterprivatepb.CoordinatorInfoRequest) (*splitterprivatepb.CoordinatorInfoResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&splitterprivatepb.CoordinatorOperationRequest{
			Req: &splitterprivatepb.CoordinatorOperationRequest_Info{
				Info: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetInfo(), nil
}

func (o *OperationService) CoordinatorRestart(ctx context.Context, request *splitterprivatepb.CoordinatorRestartRequest) (*splitterprivatepb.CoordinatorRestartResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&splitterprivatepb.CoordinatorOperationRequest{
			Req: &splitterprivatepb.CoordinatorOperationRequest_Restart{
				Restart: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetRestart(), nil
}

func (o *OperationService) CoordinatorClusterSync(ctx context.Context, request *splitterprivatepb.CoordinatorClusterSyncRequest) (*splitterprivatepb.CoordinatorClusterSyncResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&splitterprivatepb.CoordinatorOperationRequest{
			Req: &splitterprivatepb.CoordinatorOperationRequest_Sync{
				Sync: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetSync(), nil
}

func (o *OperationService) ConsumerSuspend(ctx context.Context, request *splitterprivatepb.ConsumerSuspendRequest) (*splitterprivatepb.ConsumerSuspendResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&splitterprivatepb.CoordinatorOperationRequest{
			Req: &splitterprivatepb.CoordinatorOperationRequest_Suspend{
				Suspend: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetSuspend(), nil
}

func (o *OperationService) ConsumerResume(ctx context.Context, request *splitterprivatepb.ConsumerResumeRequest) (*splitterprivatepb.ConsumerResumeResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&splitterprivatepb.CoordinatorOperationRequest{
			Req: &splitterprivatepb.CoordinatorOperationRequest_Resume{
				Resume: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetResume(), nil
}

func (o *OperationService) ConsumerDrain(ctx context.Context, request *splitterprivatepb.ConsumerDrainRequest) (*splitterprivatepb.ConsumerDrainResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&splitterprivatepb.CoordinatorOperationRequest{
			Req: &splitterprivatepb.CoordinatorOperationRequest_Drain{
				Drain: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetDrain(), nil
}

func (o *OperationService) CoordinatorRevokeGrants(ctx context.Context, request *splitterprivatepb.CoordinatorRevokeGrantsRequest) (*splitterprivatepb.CoordinatorRevokeGrantsResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&splitterprivatepb.CoordinatorOperationRequest{
			Req: &splitterprivatepb.CoordinatorOperationRequest_RevokeGrants{
				RevokeGrants: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetRevokeGrants(), nil
}

func (o *OperationService) executeCoordinatorRequest(ctx context.Context, name model.QualifiedServiceName, req coordinator.HandleRequest) (*splitterprivatepb.CoordinatorHandleResponse, error) {
	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*splitterprivatepb.CoordinatorHandleResponse, error) {
		return core.Invoke(ctx, o.serviceResolver, name, splitterprivatepb.CoordinatorServiceClient.Handle, req.Proto, func() (*splitterprivatepb.CoordinatorHandleResponse, error) {
			return o.worker.Handle(ctx, req)
		})
	})
	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, model.ToGRPCError(err)
	}
	return resp, err
}

func (o *OperationService) RaftInfo(ctx context.Context, request *splitterprivatepb.RaftInfoRequest) (*splitterprivatepb.RaftInfoResponse, error) {
	return &splitterprivatepb.RaftInfoResponse{
		RaftState: o.cluster.Info(ctx),
	}, nil
}

func (o *OperationService) Snapshot(ctx context.Context, request *splitterprivatepb.SnapshotRequest) (*splitterprivatepb.SnapshotResponse, error) {
	req := leader.NewHandleOperationRequest(&splitterprivatepb.OperationRequest{
		Req: &splitterprivatepb.OperationRequest_Snapshot{
			Snapshot: &splitterprivatepb.SnapshotRequest{},
		},
	})

	resp, err := o.executeLeaderRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetOperation().GetSnapshot(), nil
}

func (o *OperationService) Restore(ctx context.Context, request *splitterprivatepb.RestoreRequest) (*splitterprivatepb.RestoreResponse, error) {
	req := leader.NewHandleOperationRequest(&splitterprivatepb.OperationRequest{
		Req: &splitterprivatepb.OperationRequest_Restore{
			Restore: request,
		},
	})

	resp, err := o.executeLeaderRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetOperation().GetRestore(), nil
}

func (o *OperationService) executeLeaderRequest(ctx context.Context, req leader.HandleRequest) (*splitterprivatepb.LeaderHandleResponse, error) {
	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*splitterprivatepb.LeaderHandleResponse, error) {
		return core.InvokeZero(ctx, o.resolver, splitterprivatepb.LeaderServiceClient.Handle, req.Proto, func() (*splitterprivatepb.LeaderHandleResponse, error) {
			return o.proxy.Handle(ctx, req)
		})
	})

	if err != nil {
		log.Errorf(ctx, "Leader request %v failed: %v", req, err)
		return nil, model.ToGRPCError(err)
	}
	return resp, err
}
