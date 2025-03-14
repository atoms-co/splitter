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
	"go.atoms.co/splitter/pb/private"
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

func (o *OperationService) CoordinatorInfo(ctx context.Context, request *internal_v1.CoordinatorInfoRequest) (*internal_v1.CoordinatorInfoResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&internal_v1.CoordinatorOperationRequest{
			Req: &internal_v1.CoordinatorOperationRequest_Info{
				Info: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetInfo(), nil
}

func (o *OperationService) CoordinatorRestart(ctx context.Context, request *internal_v1.CoordinatorRestartRequest) (*internal_v1.CoordinatorRestartResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&internal_v1.CoordinatorOperationRequest{
			Req: &internal_v1.CoordinatorOperationRequest_Restart{
				Restart: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetRestart(), nil
}

func (o *OperationService) CoordinatorClusterSync(ctx context.Context, request *internal_v1.CoordinatorClusterSyncRequest) (*internal_v1.CoordinatorClusterSyncResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&internal_v1.CoordinatorOperationRequest{
			Req: &internal_v1.CoordinatorOperationRequest_Sync{
				Sync: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetSync(), nil
}

func (o *OperationService) ConsumerSuspend(ctx context.Context, request *internal_v1.ConsumerSuspendRequest) (*internal_v1.ConsumerSuspendResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&internal_v1.CoordinatorOperationRequest{
			Req: &internal_v1.CoordinatorOperationRequest_Suspend{
				Suspend: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetSuspend(), nil
}

func (o *OperationService) ConsumerResume(ctx context.Context, request *internal_v1.ConsumerResumeRequest) (*internal_v1.ConsumerResumeResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&internal_v1.CoordinatorOperationRequest{
			Req: &internal_v1.CoordinatorOperationRequest_Resume{
				Resume: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetResume(), nil
}

func (o *OperationService) ConsumerDrain(ctx context.Context, request *internal_v1.ConsumerDrainRequest) (*internal_v1.ConsumerDrainResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&internal_v1.CoordinatorOperationRequest{
			Req: &internal_v1.CoordinatorOperationRequest_Drain{
				Drain: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetDrain(), nil
}

func (o *OperationService) CoordinatorRevokeGrants(ctx context.Context, request *internal_v1.CoordinatorRevokeGrantsRequest) (*internal_v1.CoordinatorRevokeGrantsResponse, error) {
	name, err := model.ParseQualifiedServiceName(request.GetService())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid service name, %v: %v", proto.CompactTextString(request.GetService()), err)
	}

	req := coordinator.NewHandleCoordinatorOperationRequest(name,
		&internal_v1.CoordinatorOperationRequest{
			Req: &internal_v1.CoordinatorOperationRequest_RevokeGrants{
				RevokeGrants: request,
			},
		})

	resp, err := o.executeCoordinatorRequest(ctx, name, req)
	if err != nil {
		return nil, err
	}

	return resp.GetOperation().GetRevokeGrants(), nil
}

func (o *OperationService) executeCoordinatorRequest(ctx context.Context, name model.QualifiedServiceName, req coordinator.HandleRequest) (*internal_v1.CoordinatorHandleResponse, error) {
	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.CoordinatorHandleResponse, error) {
		return core.Invoke(ctx, o.serviceResolver, name, internal_v1.CoordinatorServiceClient.Handle, req.Proto, func() (*internal_v1.CoordinatorHandleResponse, error) {
			return o.worker.Handle(ctx, req)
		})
	})
	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, model.ToGRPCError(err)
	}
	return resp, err
}

func (o *OperationService) RaftInfo(ctx context.Context, request *internal_v1.RaftInfoRequest) (*internal_v1.RaftInfoResponse, error) {
	return &internal_v1.RaftInfoResponse{
		RaftState: o.cluster.Info(ctx),
	}, nil
}

func (o *OperationService) Snapshot(ctx context.Context, request *internal_v1.SnapshotRequest) (*internal_v1.SnapshotResponse, error) {
	req := leader.NewHandleOperationRequest(&internal_v1.OperationRequest{
		Req: &internal_v1.OperationRequest_Snapshot{
			Snapshot: &internal_v1.SnapshotRequest{},
		},
	})

	resp, err := o.executeLeaderRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetOperation().GetSnapshot(), nil
}

func (o *OperationService) Restore(ctx context.Context, request *internal_v1.RestoreRequest) (*internal_v1.RestoreResponse, error) {
	req := leader.NewHandleOperationRequest(&internal_v1.OperationRequest{
		Req: &internal_v1.OperationRequest_Restore{
			Restore: request,
		},
	})

	resp, err := o.executeLeaderRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetOperation().GetRestore(), nil
}

func (o *OperationService) executeLeaderRequest(ctx context.Context, req leader.HandleRequest) (*internal_v1.LeaderHandleResponse, error) {
	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.LeaderHandleResponse, error) {
		return core.InvokeZero(ctx, o.resolver, internal_v1.LeaderServiceClient.Handle, req.Proto, func() (*internal_v1.LeaderHandleResponse, error) {
			return o.proxy.Handle(ctx, req)
		})
	})

	if err != nil {
		log.Errorf(ctx, "Leader request %v failed: %v", req, err)
		return nil, model.ToGRPCError(err)
	}
	return resp, err
}
