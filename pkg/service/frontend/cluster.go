package frontend

import (
	"context"

	splitterprivatepb "go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pkg/cluster"
)

type ClusterService struct {
	cluster cluster.Cluster
}

func NewClusterService(c cluster.Cluster) *ClusterService {
	return &ClusterService{
		cluster: c,
	}
}

func (r *ClusterService) Notify(ctx context.Context, req *splitterprivatepb.ClusterNotifyRequest) (*splitterprivatepb.ClusterNotifyResponse, error) {
	if err := r.cluster.Notify(ctx, req.GetId(), req.GetAddress()); err != nil {
		return nil, err
	}
	return &splitterprivatepb.ClusterNotifyResponse{}, nil
}

func (r *ClusterService) AddNode(ctx context.Context, req *splitterprivatepb.ClusterAddNodeRequest) (*splitterprivatepb.ClusterAddNodeResponse, error) {
	if err := r.cluster.AddNode(ctx, req.GetId(), req.GetAddress()); err != nil {
		return nil, err
	}
	return &splitterprivatepb.ClusterAddNodeResponse{}, nil
}

func (r *ClusterService) RemoveNode(ctx context.Context, req *splitterprivatepb.ClusterRemoveNodeRequest) (*splitterprivatepb.ClusterRemoveNodeResponse, error) {
	if err := r.cluster.RemoveNode(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &splitterprivatepb.ClusterRemoveNodeResponse{}, nil
}
