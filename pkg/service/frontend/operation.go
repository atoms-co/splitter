package frontend

import (
	"context"
	"go.atoms.co/lib/log"
	"go.atoms.co/splitter/pkg/cluster"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pb/private"
)

type OperationService struct {
	cluster  *cluster.Cluster
	resolver leader.Resolver
	proxy    leader.Proxy
}

func NewOperationService(cluster *cluster.Cluster, resolver leader.Resolver, proxy leader.Proxy) *OperationService {
	return &OperationService{
		cluster:  cluster,
		resolver: resolver,
		proxy:    proxy,
	}
}

func (o *OperationService) RaftInfo(ctx context.Context, request *internal_v1.RaftInfoRequest) (*internal_v1.RaftInfoResponse, error) {
	return &internal_v1.RaftInfoResponse{
		RaftState: o.cluster.Info(ctx),
	}, nil
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
		return nil, err
	}
	return resp.GetOperation().GetRestore(), nil
}
