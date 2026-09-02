package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.atoms.co/splitter/pkg/model"
)

const (
	customShardKeyA = "00000000-0000-0000-0000-000000000000"
	customShardKeyB = "80000000-0000-0000-0000-000000000000"
	customShardKeyC = "ffffffff-ffff-ffff-ffff-ffffffffffff"
)

func TestRemoveCustomShardCommand(t *testing.T) {
	cmd := makeRemoveCustomShardCmd()

	require.Equal(t, "remove-custom-shard <tenant>/<service>/<domain> <key_name> <from_key>", cmd.Use)
	require.NotNil(t, cmd.Flags().Lookup("region"))
	require.NotNil(t, cmd.Flags().Lookup("to-key"))
	require.NoError(t, cmd.Args(cmd, []string{"infra/test/domain", "customer", customShardKeyA}))
	require.Error(t, cmd.Args(cmd, []string{"infra/test/domain", "customer"}))
}

func TestParseCustomShardKeys(t *testing.T) {
	from := model.MustParseKey(customShardKeyA)

	t.Run("single key shard", func(t *testing.T) {
		actualFrom, actualTo, err := parseCustomShardKeys(customShardKeyA, "")
		require.NoError(t, err)
		require.Equal(t, from, actualFrom)
		require.Equal(t, from.Inc(), actualTo)
	})

	t.Run("explicit range", func(t *testing.T) {
		actualFrom, actualTo, err := parseCustomShardKeys(customShardKeyA, customShardKeyB)
		require.NoError(t, err)
		require.Equal(t, from, actualFrom)
		require.Equal(t, model.MustParseKey(customShardKeyB), actualTo)
	})

	t.Run("invalid range", func(t *testing.T) {
		_, _, err := parseCustomShardKeys(customShardKeyB, customShardKeyA)
		require.EqualError(t, err, "from_key must be less than to_key [from=80000000-0000-0000-0000-000000000000, to=00000000-0000-0000-0000-000000000000]")
	})
}

func TestCustomShardAndNamedKey(t *testing.T) {
	from := model.MustParseKey(customShardKeyA)
	to := model.MustParseKey(customShardKeyB)
	domain, err := model.NewDomain(
		model.MustParseQualifiedDomainNameStr("infra/test/domain"),
		model.Regional,
		time.Time{},
		model.WithDomainConfig(model.NewDomainConfig(
			model.WithDomainShardingPolicy(model.NewShardingPolicy(4)),
			model.WithDomainRegions("centralus"),
		)),
	)
	require.NoError(t, err)

	namedKey, shard, err := customShardAndNamedKey(domain, "customer-a", from, to, "centralus")
	require.NoError(t, err)
	require.Equal(t, model.NamedDomainKey{
		Name: "customer-a",
		Key: model.DomainKey{
			Region: "centralus",
			Key:    from,
		},
	}, namedKey)
	require.Equal(t, model.NewShardingPolicyShard(from, to, "centralus"), shard)

	_, _, err = customShardAndNamedKey(domain, "customer-a", from, to, "eastus")
	require.EqualError(t, err, "invalid shard region eastus, expected one of: [centralus]")
}

func TestRemoveCustomShardAndNamedKey(t *testing.T) {
	fromA := model.MustParseKey(customShardKeyA)
	fromB := model.MustParseKey(customShardKeyB)
	toC := model.MustParseKey(customShardKeyC)
	targetNamedKey := model.NamedDomainKey{
		Name: "customer-a",
		Key: model.DomainKey{
			Key: fromA,
		},
	}
	targetShard := model.NewShardingPolicyShard(fromA, fromB, "")
	otherNamedKey := model.NamedDomainKey{
		Name: "customer-b",
		Key: model.DomainKey{
			Key: fromB,
		},
	}
	otherShard := model.NewShardingPolicyShard(fromB, toC, "")

	t.Run("removes matching shard and named key", func(t *testing.T) {
		namedKeys := []model.NamedDomainKey{targetNamedKey, otherNamedKey}
		shards := []model.ShardingPolicyShard{targetShard, otherShard}

		remainingNamedKeys, remainingShards, err := removeCustomShardAndNamedKey(namedKeys, shards, targetNamedKey, targetShard)
		require.NoError(t, err)
		require.Equal(t, []model.NamedDomainKey{otherNamedKey}, remainingNamedKeys)
		require.Equal(t, []model.ShardingPolicyShard{otherShard}, remainingShards)
		require.Len(t, namedKeys, 2)
		require.Len(t, shards, 2)
	})

	t.Run("removes final shard and named key", func(t *testing.T) {
		remainingNamedKeys, remainingShards, err := removeCustomShardAndNamedKey(
			[]model.NamedDomainKey{targetNamedKey},
			[]model.ShardingPolicyShard{targetShard},
			targetNamedKey,
			targetShard,
		)
		require.NoError(t, err)
		require.Empty(t, remainingNamedKeys)
		require.Empty(t, remainingShards)
	})

	t.Run("named key does not exist", func(t *testing.T) {
		_, _, err := removeCustomShardAndNamedKey(nil, []model.ShardingPolicyShard{targetShard}, targetNamedKey, targetShard)
		require.EqualError(t, err, "named key does not exist: customer-a")
	})

	t.Run("named key does not match", func(t *testing.T) {
		mismatchedNamedKey := targetNamedKey
		mismatchedNamedKey.Key.Key = fromB
		_, _, err := removeCustomShardAndNamedKey([]model.NamedDomainKey{mismatchedNamedKey}, []model.ShardingPolicyShard{targetShard}, targetNamedKey, targetShard)
		require.EqualError(t, err, "named key does not match custom shard: customer-a")
	})

	t.Run("custom shard does not exist", func(t *testing.T) {
		_, _, err := removeCustomShardAndNamedKey([]model.NamedDomainKey{targetNamedKey}, nil, targetNamedKey, targetShard)
		require.EqualError(t, err, "custom shard does not exist [region=, from=00000000-0000-0000-0000-000000000000, to=80000000-0000-0000-0000-000000000000]")
	})
}

func TestDomainConfigWithoutCustomShard_RemovesFinalShard(t *testing.T) {
	from := model.MustParseKey(customShardKeyA)
	to := model.MustParseKey(customShardKeyB)
	namedKey := model.NamedDomainKey{
		Name: "customer-a",
		Key: model.DomainKey{
			Key: from,
		},
	}
	shard := model.NewShardingPolicyShard(from, to, "")
	domain, err := model.NewDomain(
		model.MustParseQualifiedDomainNameStr("infra/test/domain"),
		model.Global,
		time.Time{},
		model.WithDomainConfig(model.NewDomainConfig(
			model.WithDomainShardingPolicy(model.NewShardingPolicy(4, model.WithShardingPolicyShards([]model.ShardingPolicyShard{shard}))),
			model.WithDomainNamedKeys(namedKey),
		)),
	)
	require.NoError(t, err)

	config, err := domainConfigWithoutCustomShard(domain, namedKey, shard)
	require.NoError(t, err)
	require.Empty(t, config.NamedDomainKeys())
	shards, err := config.ShardingPolicy().GetShardingPolicyShards()
	require.NoError(t, err)
	require.Empty(t, shards)
}
