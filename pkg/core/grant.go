package core

import (
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"fmt"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// GrantID is a leader-determined grant id.
type GrantID = allocation.GrantID

type Grant struct {
	pb *internal_v1.Grant
}

func NewGrant(id GrantID, name model.QualifiedServiceName, lease, assigned time.Time) Grant {
	return WrapGrant(&internal_v1.Grant{
		Id:       string(id),
		Service:  name.ToProto(),
		Lease:    timestamppb.New(lease),
		Assigned: timestamppb.New(assigned),
	})
}

func WrapGrant(pb *internal_v1.Grant) Grant {
	return Grant{pb: pb}
}

func UnwrapGrant(g Grant) *internal_v1.Grant {
	return g.pb
}

func (g Grant) ID() GrantID {
	return GrantID(g.pb.GetId())
}

func (g Grant) Service() model.QualifiedServiceName {
	ret, _ := model.ParseQualifiedServiceName(g.pb.GetService())
	return ret
}

func (g Grant) Lease() time.Time {
	return g.pb.GetLease().AsTime()
}

func (g Grant) Assigned() time.Time {
	return g.pb.GetAssigned().AsTime()
}

func (g Grant) String() string {
	return fmt.Sprintf("[id:%v]service:%v, lease: %v, assigned:%v", g.ID(), g.Service(), g.Lease(), g.Assigned())
}
