package frontend

import (
	"context"

	"go.atoms.co/splitter/pkg/cluster"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
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
