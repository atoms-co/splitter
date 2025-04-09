package model_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.atoms.co/lib/testing/requirex"
	"go.atoms.co/splitter/pkg/model"
	splitterpb "go.atoms.co/splitter/pb"
)

func TestValidateDomain(t *testing.T) {
	testTime := time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC)

	t.Run("validate unit domain with shards", func(t *testing.T) {
		pb := &splitterpb.Domain{
			Name:    model.MustParseQualifiedDomainNameStr("tenant1/service1/domain1").ToProto(),
			Type:    model.Unit,
			State:   model.DomainActive,
			Created: timestamppb.New(testTime),
			Config: &splitterpb.Domain_Config{
				ShardingPolicy: &splitterpb.ShardingPolicy{
					Shards: 4,
				},
			},
		}

		_, err := model.ParseDomain(pb)
		requirex.Equal(t, err.Error(), "invalid domain: shards cannot be specified for unit domain")
	})

	t.Run("validate global domain without shards", func(t *testing.T) {
		pb := &splitterpb.Domain{
			Name:    model.MustParseQualifiedDomainNameStr("tenant1/service1/domain1").ToProto(),
			Type:    model.Global,
			State:   model.DomainActive,
			Created: timestamppb.New(testTime),
			Config:  &splitterpb.Domain_Config{},
		}

		_, err := model.ParseDomain(pb)
		requirex.Equal(t, err.Error(), "invalid domain: shard count must be >= 1, given: 0")
	})

	t.Run("validate global domain with named key having region", func(t *testing.T) {
		pb := &splitterpb.Domain{
			Name:    model.MustParseQualifiedDomainNameStr("tenant1/service1/domain1").ToProto(),
			Type:    model.Global,
			State:   model.DomainActive,
			Created: timestamppb.New(testTime),
			Config: &splitterpb.Domain_Config{
				ShardingPolicy: &splitterpb.ShardingPolicy{
					Shards: 4,
				},
				Named: []*splitterpb.NamedDomainKey{
					{
						Name: "named1",
						Key: &splitterpb.DomainKey{
							Key:    uuid.New().String(),
							Region: "us-west-2",
						},
					},
				},
			},
		}

		_, err := model.ParseDomain(pb)
		requirex.Equal(t, err.Error(), "invalid domain: region of a named key must be empty for global domain, got non-empty value for \"named1\"")

	})

	t.Run("validate regional domain with too many named keys", func(t *testing.T) {
		pb := &splitterpb.Domain{
			Name:    model.MustParseQualifiedDomainNameStr("tenant1/service1/domain1").ToProto(),
			Type:    model.Regional,
			State:   model.DomainActive,
			Created: timestamppb.New(testTime),
			Config: &splitterpb.Domain_Config{
				ShardingPolicy: &splitterpb.ShardingPolicy{
					Shards: 4,
				},
				Regions: []string{"us-west-2"},
				Named:   make([]*splitterpb.NamedDomainKey, model.MaxNamedKeysPerDomain+1),
			},
		}

		for i := 0; i < model.MaxNamedKeysPerDomain+1; i++ {
			pb.Config.Named[i] = &splitterpb.NamedDomainKey{
				Name: "name" + string(rune(i)),
				Key: &splitterpb.DomainKey{
					Key:    uuid.New().String(),
					Region: "us-west-2",
				},
			}
		}

		_, err := model.ParseDomain(pb)
		requirex.Equal(t, err.Error(), "invalid domain: number of named keys cannot exceed 12, got 13")

	})

	t.Run("validate regional domain with invalid region in named key", func(t *testing.T) {
		pb := &splitterpb.Domain{
			Name:    model.MustParseQualifiedDomainNameStr("tenant1/service1/domain1").ToProto(),
			Type:    model.Regional,
			State:   model.DomainActive,
			Created: timestamppb.New(testTime),
			Config: &splitterpb.Domain_Config{
				ShardingPolicy: &splitterpb.ShardingPolicy{
					Shards: 4,
				},
				Regions: []string{"us-west-2"},
				Named: []*splitterpb.NamedDomainKey{
					{
						Name: "named1",
						Key: &splitterpb.DomainKey{
							Key:    uuid.New().String(),
							Region: "us-east-1",
						},
					},
				},
			},
		}

		_, err := model.ParseDomain(pb)
		requirex.Equal(t, err.Error(), "invalid domain: invalid region for named key, us-east-1, allowed regions [us-west-2]")
	})

	t.Run("validate successful regional domain", func(t *testing.T) {
		key := uuid.New().String()
		pb := &splitterpb.Domain{
			Name:    model.MustParseQualifiedDomainNameStr("tenant1/service1/domain1").ToProto(),
			Type:    model.Regional,
			State:   model.DomainActive,
			Created: timestamppb.New(testTime),
			Config: &splitterpb.Domain_Config{
				ShardingPolicy: &splitterpb.ShardingPolicy{
					Shards: 4,
				},
				Regions: []string{"us-west-2"},
				Named: []*splitterpb.NamedDomainKey{
					{
						Name: "named1",
						Key: &splitterpb.DomainKey{
							Key:    key,
							Region: "us-west-2",
						},
					},
				},
			},
		}

		domain, err := model.ParseDomain(pb)
		require.NoError(t, err)

		requirex.Equal(t, model.Regional, domain.Type())
		requirex.Equal(t, model.DomainActive, domain.State())
		requirex.Equal(t, "tenant1/service1/domain1", domain.Name().String())

		config := domain.Config()
		requirex.Equal(t, 4, config.ShardingPolicy().Shards())

		namedKeys := config.NamedDomainKeys()
		requirex.Equal(t, "named1", namedKeys[0].Name)
		requirex.Equal(t, "us-west-2", string(namedKeys[0].Key.Region))
		requirex.Equal(t, key, namedKeys[0].Key.Key.String())
	})

	t.Run("validate successful global domain", func(t *testing.T) {
		key := uuid.New().String()
		pb := &splitterpb.Domain{
			Name:    model.MustParseQualifiedDomainNameStr("tenant1/service1/domain1").ToProto(),
			Type:    model.Global,
			State:   model.DomainActive,
			Created: timestamppb.New(testTime),
			Config: &splitterpb.Domain_Config{
				ShardingPolicy: &splitterpb.ShardingPolicy{
					Shards: 4,
				},
				Named: []*splitterpb.NamedDomainKey{
					{
						Name: "named1",
						Key: &splitterpb.DomainKey{
							Key: key,
						},
					},
				},
			},
		}

		domain, err := model.ParseDomain(pb)
		require.NoError(t, err)

		requirex.Equal(t, model.Global, domain.Type())
		requirex.Equal(t, model.DomainActive, domain.State())
		requirex.Equal(t, "tenant1/service1/domain1", domain.Name().String())

		config := domain.Config()
		requirex.Equal(t, 4, config.ShardingPolicy().Shards())

		namedKeys := config.NamedDomainKeys()
		requirex.Equal(t, "named1", namedKeys[0].Name)
		requirex.Equal(t, string(namedKeys[0].Key.Region), "")
		requirex.Equal(t, key, namedKeys[0].Key.Key.String())
	})
}
