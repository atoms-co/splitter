package model_test

import (
	"context"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/requirex"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/testing/prefab"
	"fmt"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var (
	g0A           = prefab.NewGrantInfo("g0A", "t/s/d", model.Global, "", "0", "a", model.ActiveGrantState)
	gAD           = prefab.NewGrantInfo("gAD", "t/s/d", model.Global, "", "a", "d", model.ActiveGrantState)
	shard0A       = g0A.Shard()
	shardAD       = gAD.Shard()
	defaultDomain = "t/s/d"

	id = model.ClusterID{Origin: prefab.Instance1.Instance(), Version: 1}
)

const (
	testcasesPath = "../../testing/testcases/cluster"
)

func TestCluster(t *testing.T) {
	t.Run("domain shards", func(t *testing.T) {
		g21 := prefab.NewGrantInfo("g21", "t/s/d2", model.Global, "", "0", "a", model.ActiveGrantState)
		g22 := prefab.NewGrantInfo("g22", "t/s/d2", model.Global, "", "a", "d", model.ActiveGrantState)
		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, g0A, gAD), model.NewAssignment(prefab.Instance2, g21, g22)))

		actual := model.DomainShards(cluster, prefab.QDN("t/s/d"))
		sort.Slice(actual, func(i, j int) bool {
			return actual[i].From.Less(actual[j].From)
		})
		assertx.Equal(t, actual, slicex.New(shard0A, shardAD))

		actual = model.DomainShards(cluster, prefab.QDN("t/s/d2"))
		sort.Slice(actual, func(i, j int) bool {
			return actual[i].From.Less(actual[j].From)
		})
		assertx.Equal(t, actual, slicex.New(g21.Shard(), g22.Shard()))

		require.Empty(t, model.DomainShards(cluster, prefab.QDN("t/s/d3")))
	})
}

// TestClusterWithTestCases runs all test cases in the testcases directory located at testcasesPath
func TestClusterWithTestCases(t *testing.T) {
	p, err := os.Getwd()
	require.NoError(t, err)

	root := path.Join(p, testcasesPath)
	err = filepath.WalkDir(root, func(path string, info os.DirEntry, err error) error {
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".yaml") {
			content, err := os.ReadFile(path)
			require.NoError(t, err)
			t.Run(strings.TrimPrefix(path, root+"/"), func(t *testing.T) {
				runTestFromTestcase(t, content)
			})
		}
		return nil
	})
	require.NoError(t, err)
}

func runTestFromTestcase(t *testing.T, testcase []byte) {
	var actions Actions
	err := yaml.Unmarshal(testcase, &actions)
	require.NoError(t, err)

	var c *model.ClusterMap
	for _, action := range actions.Actions {
		c = action.Execute(t, c)
	}
}

type Actions struct {
	Actions []Action `yaml:"actions"`
}

// Action represents a single action in a test case. A test case involves a single cluster and a series of actions
// that are executed on the cluster in the order they are defined.
type Action struct {
	Action      string             `yaml:"action"`
	Version     int                `yaml:"version"`
	Shards      []ShardInfo        `yaml:"shards"`
	Assignments []Consumer         `yaml:"assignments"`
	Lookups     []Lookup           `yaml:"lookups"`
	Updated     []Grant            `yaml:"updated"`
	Unassigned  []model.GrantID    `yaml:"unassigned"`
	Removed     []model.ConsumerID `yaml:"removed"`
}

func (a Action) GetShards(t *testing.T) []model.Shard {
	return slicex.Map(a.Shards, func(s ShardInfo) model.Shard { return s.ShardDescription.Shard(t) })
}

func (a Action) Execute(t *testing.T, c *model.ClusterMap) *model.ClusterMap {
	t.Logf("Executing action %v", a.Action)
	switch a.Action {
	case "create":
		return a.create(t)
	case "snapshot":
		return a.snapshot(t, c)
	case "change":
		return a.change(t, c)
	case "compare":
		a.compare(t, c)
		return c
	case "lookup":
		a.lookup(t, c)
		return c
	default:
		require.Failf(t, "unknown action", "unknown action: %s", a.Action)
	}
	return c
}

func (a Action) create(t *testing.T) *model.ClusterMap {
	return model.NewClusterMap(id, a.GetShards(t))
}

func (a Action) snapshot(t *testing.T, c *model.ClusterMap) *model.ClusterMap {
	assignments := slicex.Map(a.Assignments, func(c Consumer) model.Assignment { return c.Assignment(t) })
	snapshot := model.NewClusterSnapshot(c.ID().Next(time.Time{}), assignments, a.GetShards(t))
	c, err := model.UpdateClusterMap(context.Background(), c, snapshot)
	require.NoError(t, err)
	return c
}

func (a Action) change(t *testing.T, c *model.ClusterMap) *model.ClusterMap {
	assignments := slicex.Map(a.Assignments, func(c Consumer) model.Assignment { return c.Assignment(t) })
	updated := slicex.Map(a.Updated, func(g Grant) model.GrantInfo { return g.Grant(t) })
	change := model.NewClusterChange(c.ID().Next(time.Time{}), assignments, updated, a.Unassigned, a.Removed)
	c, err := model.UpdateClusterMap(context.Background(), c, change)
	require.NoError(t, err)
	return c
}

func (a Action) compare(t *testing.T, c *model.ClusterMap) {
	requirex.Equal(t, c.ID().Version, a.Version, "cluster ID mismatch")
	requirex.EqualProtobuf(t, location.UnwrapInstance(c.ID().Origin), location.UnwrapInstance(prefab.Instance1.Instance()), "cluster origin mismatch")

	// Compare shard grants
	var expectedShards []model.Shard
	domains := map[model.QualifiedDomainName][]model.Shard{}
	for _, s := range a.Shards {
		expectedShard := s.ShardDescription.Shard(t)
		expectedShards = append(expectedShards, expectedShard)
		domains[expectedShard.Domain] = append(domains[expectedShard.Domain], expectedShard)
		actualGrants, ok := c.ShardGrants(expectedShard)
		require.True(t, ok, "shard not found: %s", expectedShard)
		actual := slicex.NewSet(actualGrants...)
		expected := slicex.NewSet(s.Grants...)
		requirex.Equal(t, actual, expected, "grants mismatch for shard %s", expectedShard)
	}
	// Compare shards
	requirex.Equal(t, slicex.NewSet(expectedShards...), slicex.NewSet(c.Shards()...), "total shards mismatch")

	// Check domain shards
	for domain, shards := range domains {
		actualShards := model.DomainShards(c, domain)
		requirex.Equal(t, slicex.NewSet(shards...), slicex.NewSet(actualShards...), "shards mismatch for domain %v", domain)
	}

	// Compare consumers and their assignments
	var expectedConsumers []model.Consumer
	var expectedAssignments []model.Assignment
	for _, consumer := range a.Assignments {
		expectedConsumer := consumer.Consumer()
		expectedConsumers = append(expectedConsumers, expectedConsumer)

		actualConsumer, actualGrants, ok := c.Consumer(consumer.ID)
		require.True(t, ok, "consumer not found: %s", actualConsumer.ID())
		requireConsumersEqual(t, actualConsumer, expectedConsumer, "consumer mismatch: %s", actualConsumer.ID())
		requirex.Equal(t, len(actualGrants), len(consumer.Grants), "total grants mismatch for consumer: %s", actualConsumer.ID())

		sort.Slice(actualGrants, func(i, j int) bool {
			return actualGrants[i].ID() < actualGrants[j].ID()
		})
		sort.Slice(consumer.Grants, func(i, j int) bool {
			return consumer.Grants[i].ID < consumer.Grants[j].ID
		})

		var expectedGrants []model.GrantInfo
		for i, grant := range consumer.Grants {
			expectedGrant := grant.Grant(t)
			expectedGrants = append(expectedGrants, expectedGrant)
			requireGrantsEqual(t, actualGrants[i], expectedGrant, "grant mismatch: %s", grant.ID)

			grantConsumer, actualGrant, ok := c.Grant(grant.ID)
			require.True(t, ok, "grant not found: %s", grant.ID)
			requireGrantsEqual(t, actualGrant, expectedGrant, "grant mismatch: %s", grant.ID)
			requireConsumersEqual(t, grantConsumer, expectedConsumer, "consumer mismatch: %s", actualConsumer.ID())
		}

		expectedAssignments = append(expectedAssignments, model.NewAssignment(expectedConsumer, expectedGrants...))
	}

	// Compare consumers
	sort.Slice(expectedConsumers, func(i, j int) bool {
		return expectedConsumers[i].ID() < expectedConsumers[j].ID()
	})
	actualConsumers := c.Consumers()
	sort.Slice(actualConsumers, func(i, j int) bool {
		return actualConsumers[i].ID() < actualConsumers[j].ID()
	})
	requirex.Equal(t, len(actualConsumers), len(expectedConsumers), "total consumers mismatch")
	for i := range actualConsumers {
		requireConsumersEqual(t, actualConsumers[i], expectedConsumers[i], "total consumers mismatch")
	}

	// Compare assignments
	actualAssignments := c.Assignments()
	sort.Slice(actualAssignments, func(i, j int) bool {
		return actualAssignments[i].Consumer().ID() < actualAssignments[j].Consumer().ID()
	})
	requirex.Equal(t, len(actualAssignments), len(expectedAssignments), "total assignments mismatch")
	for i, actualAssignment := range actualAssignments {
		requireConsumersEqual(t, actualAssignment.Consumer(), expectedAssignments[i].Consumer(), "consumer mismatch for assignment: %s", actualAssignment.Consumer().ID())

		expectedAssignment := expectedAssignments[i]
		grants := actualAssignment.Grants()
		sort.Slice(grants, func(i, j int) bool {
			return grants[i].ID() < grants[j].ID()
		})

		expectedGrants := expectedAssignment.Grants()
		requirex.Equal(t, len(grants), len(expectedGrants), "total grants mismatch for consumer: %s", actualAssignment.Consumer().ID())
		for i := range expectedGrants {
			requireGrantsEqual(t, grants[i], expectedGrants[i], "grants mismatch for consumer: %s", actualAssignment.Consumer().ID())
		}
	}
}

func (a Action) lookup(t *testing.T, c *model.ClusterMap) {
	for _, lookup := range a.Lookups {
		key := lookup.DomainKey(t)
		t.Logf("Looking up key %v and states %v", key, lookup.LookupStates)
		consumer, grant, ok := c.Lookup(key, lookup.States(t)...)

		if lookup.Result.Found == nil || *lookup.Result.Found {
			require.True(t, ok, "lookup failed: %s", key)
			require.Equal(t, lookup.Result.Consumer, consumer.ID(), "consumer mismatch")
			require.Equal(t, lookup.Result.Grant, grant.ID(), "grant mismatch")
		} else {
			require.False(t, ok, "unexpected lookup success: %s", key)
		}
	}
}

type Consumer struct {
	ID     model.ConsumerID `yaml:"consumer"`
	Grants []Grant          `yaml:"grants"`
}

func (c Consumer) Consumer() model.Consumer {
	return prefab.NewInstance("centralus", location.Node(fmt.Sprintf("node-%v", c.ID)), c.ID, time.Time{})
}

func (c Consumer) Assignment(t *testing.T) model.Assignment {
	grants := slicex.Map(c.Grants, func(g Grant) model.GrantInfo { return g.Grant(t) })
	return model.NewAssignment(c.Consumer(), grants...)
}

type ShardInfo struct {
	ShardDescription ShardDescription `yaml:"shard"`
	Grants           []model.GrantID  `yaml:"grants"`
}

type GrantState string

func (s GrantState) State(t *testing.T) model.GrantState {
	switch s {
	case "allocated":
		return model.AllocatedGrantState
	case "loaded":
		return model.LoadedGrantState
	case "active":
		return model.ActiveGrantState
	case "revoked":
		return model.RevokedGrantState
	case "unloaded":
		return model.UnloadedGrantState
	default:
		require.Failf(t, "unknown grant state", "unknown grant state: %s", s)
		return model.InvalidGrantState
	}
}

type Grant struct {
	ID               model.GrantID    `yaml:"id"`
	ShardDescription ShardDescription `yaml:"shard"`
	State            GrantState       `yaml:"state"`
}

func (g Grant) Grant(t *testing.T) model.GrantInfo {
	return model.NewGrantInfo(g.ID, g.ShardDescription.Shard(t), g.State.State(t))
}

// ShardDescription represents a shard description in the format domain=domain,type=type,range=(from:to),region=region
type ShardDescription string

func (s ShardDescription) Shard(t *testing.T) model.Shard {
	t.Helper()

	domain := defaultDomain
	var shardRange, region string
	var dt model.DomainType
	for _, part := range strings.Split(string(s), ",") {
		props := strings.Split(part, "=")
		require.Len(t, props, 2, "invalid shard property: %s", part)

		switch strings.TrimSpace(props[0]) {
		case "domain":
			domain = strings.TrimSpace(props[1])
		case "type":
			switch strings.TrimSpace(props[1]) {
			case "G":
				dt = model.Global
			case "R":
				dt = model.Regional
			case "U":
				dt = model.Unit
			default:
				require.Failf(t, "unknown domain type", "unknown domain type: %s", props[1])
			}
		case "range":
			shardRange = strings.TrimSpace(props[1])
		case "region":
			region = strings.TrimSpace(props[1])
		default:
			require.Failf(t, "unknown shard property", "unknown shard property: %s", props[0])
		}
	}

	requirex.Equal(t, shardRange[0], '(', "range must be in format (from:to)")
	requirex.Equal(t, shardRange[len(shardRange)-1], ')', "range must be in format (from:to)")

	rangeParts := strings.Split(shardRange[1:len(shardRange)-1], ":")
	require.Len(t, rangeParts, 2, "range must be in format (from:to)")

	qdn, ok := model.ParseQualifiedDomainNameStr(domain)
	require.True(t, ok, "invalid domain: %s", domain)

	from, err := prefab.PadToUUID(rangeParts[0])
	require.NoError(t, err, "invalid range: %s", shardRange)
	to, err := prefab.PadToUUID(rangeParts[1])
	require.NoError(t, err, "invalid range: %s", shardRange)

	return model.Shard{
		Region: model.Region(region),
		Domain: qdn,
		Type:   dt,
		To:     model.Key(to),
		From:   model.Key(from),
	}
}

type Lookup struct {
	Key          string       `yaml:"key"`
	Region       model.Region `yaml:"region"`
	Domain       string       `yaml:"domain"`
	LookupStates string       `yaml:"states"`
	Result       LookupResult `yaml:"expected"`
}

func (l Lookup) DomainKey(t *testing.T) model.QualifiedDomainKey {
	if l.Domain == "" {
		l.Domain = defaultDomain
	}
	key, err := prefab.PadToUUID(l.Key)
	require.NoError(t, err, "invalid key: %s", l.Key)

	qdn, ok := model.ParseQualifiedDomainNameStr(l.Domain)
	require.True(t, ok, "invalid domain: %s", l.Domain)

	return model.QualifiedDomainKey{
		Domain: qdn,
		Key:    model.DomainKey{Region: l.Region, Key: model.Key(key)},
	}
}

func (l Lookup) States(t *testing.T) []model.GrantState {
	if l.LookupStates == "" {
		return nil
	}
	return slicex.Map(strings.Split(l.LookupStates, ","), func(s string) model.GrantState { return GrantState(strings.TrimSpace(s)).State(t) })
}

type LookupResult struct {
	Found    *bool            `yaml:"found"`
	Grant    model.GrantID    `yaml:"grant"`
	Consumer model.ConsumerID `yaml:"consumer"`
}

func newCluster(t *testing.T, assignments []model.Assignment, shards ...model.Shard) *model.ClusterMap {
	t.Helper()
	cluster := model.NewClusterMap(id, nil)
	return applySnapshot(t, cluster, assignments, shards...)
}

func applySnapshot(t *testing.T, c *model.ClusterMap, assignments []model.Assignment, shards ...model.Shard) *model.ClusterMap {
	t.Helper()
	cluster, err := model.UpdateClusterMap(context.Background(), c, model.NewClusterSnapshot(id.Next(time.Now()), assignments, shards))
	require.NoError(t, err)
	return cluster
}

func requireConsumersEqual(t *testing.T, c1, c2 model.Consumer, args ...any) {
	requirex.EqualProtobuf(t, model.UnwrapInstance(c1), model.UnwrapInstance(c2), args...)
}

func requireGrantsEqual(t *testing.T, g1, g2 model.GrantInfo, args ...any) {
	requirex.EqualProtobuf(t, model.UnwrapGrantInfo(g1), model.UnwrapGrantInfo(g2), args...)
}
