package prefab

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/uuidx"
	"go.atoms.co/splitter/lib/service/location/pb"
	"go.atoms.co/splitter/pkg/model"
	splitterpb "go.atoms.co/splitter/pb"
)

var (
	Instance1 = NewInstance("centralus", "node-1", "id1", time.Now())
	Instance2 = NewInstance("northcentralus", "node-2", "id2", time.Now())
	Instance3 = NewInstance("centralus", "node-3", "id3", time.Now())
	Instance4 = NewInstance("centralus", "node-4", "id4", time.Now())
)

func NewInstance(region location.Region, node location.Node, id model.InstanceID, ts time.Time) model.Instance {
	instance := location.WrapInstance(&location_v1.Instance{
		Id:       string(id),
		Location: location.New(region, node).ToProto(),
		Created:  timestamppb.New(ts),
	})
	return model.NewInstance(instance, fmt.Sprintf("%v:50051", node))
}

var QDN = model.MustParseQualifiedDomainNameStr

func NewShard(domain string, dtype model.DomainType, region model.Region, from, to string) model.Shard {
	toKey, _ := PadToUUID(to)
	fromKey, _ := PadToUUID(from)
	return model.Shard{
		Region: region,
		Domain: QDN(domain),
		Type:   dtype,
		To:     model.Key(toKey),
		From:   model.Key(fromKey),
	}
}

func NewGrantInfo(id string, domain string, dtype model.DomainType, region model.Region, from, to string, state model.GrantState) model.GrantInfo {
	return model.WrapGrantInfo(&splitterpb.ClusterMessage_GrantInfo{
		Id:    id,
		Shard: NewShard(domain, dtype, region, from, to).ToProto(),
		State: state,
	})
}

func NewQDK(domain string, region model.Region, id string) model.QualifiedDomainKey {
	key, _ := PadToUUID(id)
	return model.QualifiedDomainKey{
		Domain: QDN(domain),
		Key:    model.DomainKey{Region: region, Key: model.Key(key)},
	}
}

// PadToUUID creates a UUID by appending zeros to the provided prefix
func PadToUUID(v string) (uuid.UUID, error) {
	return uuid.Parse(fmt.Sprintf("%v%v", v, uuidx.Min.String()[len(v):]))
}
