package prefab

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/uuidx"
	"go.atoms.co/splitter/lib/service/location/pb"
	"go.atoms.co/splitter/pkg/model"
	"fmt"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

var (
	Instance1 = NewInstance("centralus", "node-1", "id1")
	Instance2 = NewInstance("northcentralus", "node-2", "id2")
	Instance3 = NewInstance("centralus", "node-3", "id3")
	Instance4 = NewInstance("centralus", "node-4", "id4")
)

func NewInstance(region location.Region, node location.Node, id model.InstanceID) model.Instance {
	instance := location.WrapInstance(&location_v1.Instance{
		Id:       string(id),
		Location: location.New(region, node).ToProto(),
		Created:  timestamppb.New(time.Now()),
	})
	return model.NewInstance(instance, fmt.Sprintf("%v:50051", node))
}

var QDN = model.MustParseQualifiedDomainNameStr

func NewShard(domain string, dtype model.DomainType, region model.Region, from, to string) model.Shard {
	return model.Shard{
		Region: region,
		Domain: QDN(domain),
		Type:   dtype,
		To:     model.Key(pad(to)),
		From:   model.Key(pad(from)),
	}
}

func NewGrantInfo(id string, domain string, dtype model.DomainType, region model.Region, from, to string, state model.GrantState) model.GrantInfo {
	return model.GrantInfo{
		ID:    model.GrantID(id),
		Shard: NewShard(domain, dtype, region, from, to),
		State: state,
	}
}

func NewQDK(domain string, region model.Region, id string) model.QualifiedDomainKey {
	return model.QualifiedDomainKey{
		Domain: QDN(domain),
		Key:    model.DomainKey{Region: region, Key: model.Key(pad(id))},
	}
}

// pad creates a UUID by appending zeros to the provided prefix
func pad(v string) uuid.UUID {
	return uuid.MustParse(fmt.Sprintf("%v%v", v, uuidx.Min.String()[len(v):]))
}
