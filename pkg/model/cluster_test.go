package model_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/requirex"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/testing/prefab"
)

var (
	defaultDomain = "t/s/d"

	id = model.ClusterID{Origin: prefab.Instance1.Instance(), Version: 1}
)

const (
	testcasesPath = "testing/testcases/cluster"
)

func TestCluster(t *testing.T) {
	t.Run("domain shards", func(t *testing.T) {
		g0A := prefab.NewGrantInfo(t, "g0A", "t/s/d", model.Global, "", "0", "a", model.ActiveGrantState)
		gAD := prefab.NewGrantInfo(t, "gAD", "t/s/d", model.Global, "", "a", "d", model.ActiveGrantState)
		g21 := prefab.NewGrantInfo(t, "g21", "t/s/d2", model.Global, "", "0", "a", model.ActiveGrantState)
		g22 := prefab.NewGrantInfo(t, "g22", "t/s/d2", model.Global, "", "a", "d", model.ActiveGrantState)
		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, g0A, gAD), model.NewAssignment(prefab.Instance2, g21, g22)), g0A.Shard(), gAD.Shard(), g21.Shard(), g22.Shard())

		actual := model.DomainShards(cluster, prefab.QDN("t/s/d"))
		sort.Slice(actual, func(i, j int) bool {
			return actual[i].From.Less(actual[j].From)
		})
		assertx.Equal(t, actual, slicex.New(g0A.Shard(), gAD.Shard()))

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

	tcpath := testcasesPath
	if len(os.Getenv("BAZEL")) == 0 {
		tcpath = fmt.Sprintf("../../%s", tcpath)
	}
	root := path.Join(p, tcpath)
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
	decoder := yaml.NewDecoder(bytes.NewReader(testcase))
	decoder.KnownFields(true)
	err := decoder.Decode(&actions)
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
	Error       string             `yaml:"expected_error"`
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
		require.Failf(t, "unknown action", "unknown action: %v", a.Action)
	}
	return c
}

func (a Action) create(t *testing.T) *model.ClusterMap {
	return model.NewClusterMap(id, a.GetShards(t))
}

func (a Action) snapshot(t *testing.T, c *model.ClusterMap) *model.ClusterMap {
	assignments := slicex.Map(a.Assignments, func(c Consumer) model.Assignment { return c.Assignment(t) })
	snapshot := model.NewClusterSnapshot(a.id(c), assignments, a.GetShards(t))
	upd, err := model.UpdateClusterMap(context.Background(), c, snapshot)
	if a.Error != "" {
		require.EqualError(t, err, a.Error, "expected error")
	} else {
		require.NoError(t, err)
		c = upd
	}
	return c
}

func (a Action) change(t *testing.T, c *model.ClusterMap) *model.ClusterMap {
	assignments := slicex.Map(a.Assignments, func(c Consumer) model.Assignment { return c.Assignment(t) })
	updated := slicex.Map(a.Updated, func(g Grant) model.GrantInfo { return g.Grant(t) })
	change := model.NewClusterChange(a.id(c), assignments, updated, a.Unassigned, a.Removed)
	upd, err := model.UpdateClusterMap(context.Background(), c, change)
	if a.Error != "" {
		require.EqualError(t, err, a.Error, "expected error")
	} else {
		require.NoError(t, err)
		c = upd
	}
	return c
}

func (a Action) id(c *model.ClusterMap) model.ClusterID {
	id := c.ID().Next(time.Time{})
	if a.Version > 0 {
		id.Version = a.Version
	}
	return id
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
		require.True(t, ok, "shard not found: %v", expectedShard)
		actual := slicex.NewSet(actualGrants...)
		expected := slicex.NewSet(s.Grants...)
		requirex.Equal(t, actual, expected, "grants mismatch for shard %v", expectedShard)
	}
	// Compare shards
	requirex.Equal(t, slicex.NewSet(c.Shards()...), slicex.NewSet(expectedShards...), "total shards mismatch")

	// Check domain shards
	for domain, shards := range domains {
		actualShards := model.DomainShards(c, domain)
		requirex.Equal(t, slicex.NewSet(actualShards...), slicex.NewSet(shards...), "shards mismatch for domain %v", domain)
	}

	// Compare consumers and their assignments
	var expectedConsumers []model.Consumer
	var expectedAssignments []model.Assignment
	for _, consumer := range a.Assignments {
		expectedConsumer := consumer.Consumer()
		expectedConsumers = append(expectedConsumers, expectedConsumer)

		actualConsumer, actualGrants, ok := c.Consumer(consumer.ID)
		require.True(t, ok, "consumer not found: %v", consumer.ID)
		requireConsumersEqual(t, actualConsumer, expectedConsumer, "consumer mismatch: %v", consumer.ID)
		requirex.Equal(t, len(actualGrants), len(consumer.Grants), "total grants mismatch for consumer: %v", consumer.ID)

		version, _ := model.ConsumerRetainedVersion(c, consumer.ID)
		require.Equal(t, version, consumer.Version, "version mismatch for consumer: %v", consumer.ID)

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
			requireGrantsEqual(t, actualGrants[i], expectedGrant, "grant mismatch: %v", grant.ID)

			grantConsumer, actualGrant, ok := c.Grant(grant.ID)
			require.True(t, ok, "grant not found: %v", grant.ID)
			requireGrantsEqual(t, actualGrant, expectedGrant, "grant mismatch: %v", grant.ID)
			requireConsumersEqual(t, grantConsumer, expectedConsumer, "consumer mismatch for grant %v: %v", grant.ID, consumer.ID)

			version, _ := model.GrantRetainedVersion(c, grant.ID)
			require.Equal(t, version, grant.Version, "version mismatch for grant: %v", grant.ID)
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
		requireConsumersEqual(t, actualConsumers[i], expectedConsumers[i], "consumer mismatch: %v", actualConsumers[i].ID())
	}

	// Compare assignments
	actualAssignments := c.Assignments()
	sort.Slice(actualAssignments, func(i, j int) bool {
		return actualAssignments[i].Consumer().ID() < actualAssignments[j].Consumer().ID()
	})
	requirex.Equal(t, len(actualAssignments), len(expectedAssignments), "total assignments mismatch")
	for i, actualAssignment := range actualAssignments {
		expectedAssignment := expectedAssignments[i]
		requireConsumersEqual(t, actualAssignment.Consumer(), expectedAssignment.Consumer(), "consumer mismatch for assignment: %v", actualAssignment.Consumer().ID())

		grants := actualAssignment.Grants()
		sort.Slice(grants, func(i, j int) bool {
			return grants[i].ID() < grants[j].ID()
		})

		expectedGrants := expectedAssignment.Grants()
		requirex.Equal(t, len(grants), len(expectedGrants), "total grants mismatch for consumer: %v", actualAssignment.Consumer().ID())
		for i := range expectedGrants {
			requireGrantsEqual(t, grants[i], expectedGrants[i], "grants mismatch for consumer: %v", actualAssignment.Consumer().ID())
		}
	}
}

func (a Action) lookup(t *testing.T, c *model.ClusterMap) {
	for _, lookup := range a.Lookups {
		key := lookup.DomainKey(t)
		t.Logf("Looking up key %v and states %v", key, lookup.LookupStates)
		consumer, grant, ok := c.Lookup(key, lookup.States(t)...)

		if lookup.Result.Found == nil || *lookup.Result.Found {
			require.True(t, ok, "lookup failed for key %v and states %v", key, lookup.LookupStates)
			require.Equal(t, lookup.Result.Consumer, consumer.ID(), "unexpected consumer for key %v and states %v", key, lookup.LookupStates)
			require.Equal(t, lookup.Result.Grant, grant.ID(), "unexpected grant for key %v and states %v", key, lookup.LookupStates)
		} else {
			require.False(t, ok, "unexpected lookup success for key %v and states %v", key, lookup.LookupStates)
		}
	}
}

type Consumer struct {
	ID      model.ConsumerID `yaml:"consumer"`
	Grants  []Grant          `yaml:"grants"`
	Version int              `yaml:"origin_cluster_version"`
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
		require.Failf(t, "unknown grant state", "unknown grant state: %v", s)
		return model.InvalidGrantState
	}
}

type Grant struct {
	ID               model.GrantID    `yaml:"id"`
	ShardDescription ShardDescription `yaml:"shard"`
	State            GrantState       `yaml:"state"`
	Version          int              `yaml:"origin_cluster_version"`
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
		require.Len(t, props, 2, "invalid shard property: %v", part)

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
				require.Failf(t, "unknown domain type", "unknown domain type: %v", props[1])
			}
		case "range":
			shardRange = strings.TrimSpace(props[1])
		case "region":
			region = strings.TrimSpace(props[1])
		default:
			require.Failf(t, "unknown shard property", "unknown shard property: %v", props[0])
		}
	}

	requirex.Equal(t, shardRange[0], '(', "range must be in format (from:to)")
	requirex.Equal(t, shardRange[len(shardRange)-1], ')', "range must be in format (from:to)")

	rangeParts := strings.Split(shardRange[1:len(shardRange)-1], ":")
	require.Len(t, rangeParts, 2, "range must be in format (from:to)")

	qdn, ok := model.ParseQualifiedDomainNameStr(domain)
	require.True(t, ok, "invalid domain: %v", domain)

	return model.Shard{
		Region: model.Region(region),
		Domain: qdn,
		Type:   dt,
		To:     model.Key(prefab.PadToUUID(t, rangeParts[1])),
		From:   model.Key(prefab.PadToUUID(t, rangeParts[0])),
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

	qdn, ok := model.ParseQualifiedDomainNameStr(l.Domain)
	require.True(t, ok, "invalid domain: %v", l.Domain)

	return model.QualifiedDomainKey{
		Domain: qdn,
		Key:    model.DomainKey{Region: l.Region, Key: model.Key(prefab.PadToUUID(t, l.Key))},
	}
}

func (l Lookup) States(t *testing.T) []model.GrantState {
	if l.LookupStates == "" {
		return nil
	}
	return slicex.Map(strings.Split(l.LookupStates, ","), func(s string) model.GrantState {
		return GrantState(strings.TrimSpace(s)).State(t)
	})
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
