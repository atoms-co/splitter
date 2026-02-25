package core

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

type ConsumerInfo struct {
	pb *splitterprivatepb.InstanceInfo
}

func NewConsumerInfo(instance model.Instance, joined time.Time, namedKeys []model.DomainKeyName, limit int) ConsumerInfo {
	return WrapConsumerInfo(&splitterprivatepb.InstanceInfo{
		Consumer:  model.UnwrapInstance(instance),
		Joined:    timestamppb.New(joined),
		NamedKeys: slicex.Map(namedKeys, model.DomainKeyName.ToProto),
		Limit:     uint64(limit),
	})
}

func WrapConsumerInfo(ci *splitterprivatepb.InstanceInfo) ConsumerInfo {
	return ConsumerInfo{pb: ci}
}

func UnwrapConsumerInfo(ci ConsumerInfo) *splitterprivatepb.InstanceInfo {
	return ci.pb
}

func (c ConsumerInfo) Instance() model.Instance {
	return model.WrapInstance(c.pb.GetConsumer())
}
