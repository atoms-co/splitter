package model

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"go.atoms.co/lib/uuidx"
	"go.atoms.co/splitter/lib/service/location"
	splitterpb "go.atoms.co/splitter/pb"
)

type testPrefab struct {
	Instance1 Instance
	Instance2 Instance
	Instance3 Instance
	Instance4 Instance
}

var prefab = testPrefab{
	Instance1: newPrefabInstance("centralus", "node-1", "id1", time.Now()),
	Instance2: newPrefabInstance("northcentralus", "node-2", "id2", time.Now()),
	Instance3: newPrefabInstance("centralus", "node-3", "id3", time.Now()),
	Instance4: newPrefabInstance("centralus", "node-4", "id4", time.Now()),
}

func newPrefabInstance(region location.Region, node location.Node, id InstanceID, createdAt time.Time) Instance {
	instance := location.NewInstance(location.New(region, node), location.WithInstanceID(id), location.WithInstanceCreatedAt(createdAt))
	return NewInstance(instance, fmt.Sprintf("%v:50051", node))
}

func (testPrefab) NewInstance(region location.Region, node location.Node, id InstanceID, createdAt time.Time) Instance {
	return newPrefabInstance(region, node, id, createdAt)
}

func (testPrefab) QDN(name string) QualifiedDomainName {
	return MustParseQualifiedDomainNameStr(name)
}

func (p testPrefab) NewShard(t *testing.T, domain string, domainType DomainType, region Region, from, to string) Shard {
	return Shard{
		Region: region,
		Domain: p.QDN(domain),
		Type:   domainType,
		To:     Key(p.PadToUUID(t, to)),
		From:   Key(p.PadToUUID(t, from)),
	}
}

func (p testPrefab) NewGrantInfo(t *testing.T, id, domain string, domainType DomainType, region Region, from, to string, state GrantState) GrantInfo {
	return WrapGrantInfo(&splitterpb.ClusterMessage_GrantInfo{
		Id:    id,
		Shard: p.NewShard(t, domain, domainType, region, from, to).ToProto(),
		State: state,
	})
}

func (p testPrefab) NewQDK(t *testing.T, domain string, region Region, id string) QualifiedDomainKey {
	return QualifiedDomainKey{
		Domain: p.QDN(domain),
		Key:    DomainKey{Region: region, Key: Key(p.PadToUUID(t, id))},
	}
}

func (testPrefab) PadToUUID(t *testing.T, value string) uuid.UUID {
	parsed, err := uuid.Parse(fmt.Sprintf("%v%v", value, uuidx.Min.String()[len(value):]))
	require.NoError(t, err)
	return parsed
}
