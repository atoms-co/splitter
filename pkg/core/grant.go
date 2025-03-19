package core

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/model"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

// GrantID is a leader-determined grant id.
type GrantID = allocation.GrantID

type Grant struct {
	pb *splitterprivatepb.Grant
}

func NewGrant(id GrantID, name model.QualifiedServiceName, lease, assigned time.Time) Grant {
	return WrapGrant(&splitterprivatepb.Grant{
		Id:       string(id),
		Service:  name.ToProto(),
		Lease:    timestamppb.New(lease),
		Assigned: timestamppb.New(assigned),
	})
}

func WrapGrant(pb *splitterprivatepb.Grant) Grant {
	return Grant{pb: pb}
}

func UnwrapGrant(g Grant) *splitterprivatepb.Grant {
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
