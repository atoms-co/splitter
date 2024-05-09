package frontend

import (
	"context"
	"go.atoms.co/lib/log"
	"go.atoms.co/splitter/pkg/cluster"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pkg/service/worker"
	"go.atoms.co/splitter/pb/private"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type OperationService struct {
	cluster         *cluster.Cluster
	worker          *worker.Worker
	serviceResolver core.ServiceResolver
	resolver        leader.Resolver
	proxy           leader.Proxy
}

func NewOperationService(cluster *cluster.Cluster, worker *worker.Worker, serviceResolver core.ServiceResolver, resolver leader.Resolver, proxy leader.Proxy) *OperationService {
	return &OperationService{
		cluster:         cluster,
		worker:          worker,
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

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.CoordinatorHandleResponse, error) {
		return model.InvokeEx(ctx, o.serviceResolver, name, internal_v1.CoordinatorServiceClient.Handle, req.Proto, func() (*internal_v1.CoordinatorHandleResponse, error) {
			return o.worker.Handle(ctx, req)
		})
	})
	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, model.WrapError(err)
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

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.CoordinatorHandleResponse, error) {
		return model.InvokeEx(ctx, o.serviceResolver, name, internal_v1.CoordinatorServiceClient.Handle, req.Proto, func() (*internal_v1.CoordinatorHandleResponse, error) {
			return o.worker.Handle(ctx, req)
		})
	})
	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, model.WrapError(err)
	}

	return resp.GetOperation().GetRestart(), nil
}

func (o *OperationService) CoordinatorConsumerSuspend(ctx context.Context, request *internal_v1.CoordinatorConsumerSuspendRequest) (*internal_v1.CoordinatorConsumerSuspendResponse, error) {
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

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.CoordinatorHandleResponse, error) {
		return model.InvokeEx(ctx, o.serviceResolver, name, internal_v1.CoordinatorServiceClient.Handle, req.Proto, func() (*internal_v1.CoordinatorHandleResponse, error) {
			return o.worker.Handle(ctx, req)
		})
	})
	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, model.WrapError(err)
	}

	return resp.GetOperation().GetSuspend(), nil
}

func (o *OperationService) CoordinatorConsumerResume(ctx context.Context, request *internal_v1.CoordinatorConsumerResumeRequest) (*internal_v1.CoordinatorConsumerResumeResponse, error) {
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

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.CoordinatorHandleResponse, error) {
		return model.InvokeEx(ctx, o.serviceResolver, name, internal_v1.CoordinatorServiceClient.Handle, req.Proto, func() (*internal_v1.CoordinatorHandleResponse, error) {
			return o.worker.Handle(ctx, req)
		})
	})
	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, model.WrapError(err)
	}

	return resp.GetOperation().GetResume(), nil
}

func (o *OperationService) CoordinatorConsumerDrain(ctx context.Context, request *internal_v1.CoordinatorConsumerDrainRequest) (*internal_v1.CoordinatorConsumerDrainResponse, error) {
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

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.CoordinatorHandleResponse, error) {
		return model.InvokeEx(ctx, o.serviceResolver, name, internal_v1.CoordinatorServiceClient.Handle, req.Proto, func() (*internal_v1.CoordinatorHandleResponse, error) {
			return o.worker.Handle(ctx, req)
		})
	})
	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, model.WrapError(err)
	}

	return resp.GetOperation().GetDrain(), nil
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

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.LeaderHandleResponse, error) {
		return model.InvokeExZero(ctx, o.resolver, internal_v1.LeaderServiceClient.Handle, req.Proto, func() (*internal_v1.LeaderHandleResponse, error) {
			return o.proxy.Handle(ctx, req)
		})
	})

	if err != nil {
		log.Errorf(ctx, "Snapshot %v failed: %v", req, err)
		return nil, model.WrapError(err)
	}
	return resp.GetOperation().GetSnapshot(), nil
}

func (o *OperationService) Restore(ctx context.Context, request *internal_v1.RestoreRequest) (*internal_v1.RestoreResponse, error) {
	req := leader.NewHandleOperationRequest(&internal_v1.OperationRequest{
		Req: &internal_v1.OperationRequest_Restore{
			Restore: request,
		},
	})

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.LeaderHandleResponse, error) {
		return model.InvokeExZero(ctx, o.resolver, internal_v1.LeaderServiceClient.Handle, req.Proto, func() (*internal_v1.LeaderHandleResponse, error) {
			return o.proxy.Handle(ctx, req)
		})
	})

	if err != nil {
		log.Errorf(ctx, "Restore %v failed: %v", req, err)
		return nil, model.WrapError(err)
	}
	return resp.GetOperation().GetRestore(), nil
}
