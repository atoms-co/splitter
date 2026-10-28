# Splitter load balancing simulator

Runs the real service coordinator against synthetic consumers in one Go process.
Go's `testing/synctest` advances virtual time: no Docker, Splitter server, network,
database, or external metrics system is required. The consumer protocol models
leases, retained grants on reconnect, unloaded/loaded notifications, promotion,
and immediate initialization/draining. Production coordinator timing is unchanged.

## Run

From the repository root:

```sh
bazel test //testing/simulator:simulator_test \
  --test_filter='^TestScenario$' --nocache_test_results --test_output=streamed \
  --test_arg='-scenario={"consumers":16,"shards":128,"warmup":"5m","duration":"1h","drain_rate":0.1,"reconnect_rate":0.02,"quit_rate":0.01}'
```

The JSON object overrides defaults; unknown fields and invalid configurations fail
before the simulation starts. Durations are strings accepted by `time.ParseDuration`.
Without `-scenario`, the runner simulates a stable population for one hour, after
five minutes of warmup.

Reports are retained in Bazel's test outputs:

```
bazel-testlogs/testing/simulator/simulator_test/test.outputs/
```

Depending on Bazel's output settings, files may be in `outputs.zip` there.
`report.html` is a standalone interactive chart and movement summary;
`report.json` includes configuration, events, per-transfer outcomes, and samples;
`samples.csv` includes one row per consumer per sample. The test log includes a
summary and actual wall-clock runtime. Add `--test_arg=-coordinator_logs` to include verbose protocol logs. Report generation occurs outside virtual time.

## Configuration

| Field | Default | Meaning |
|---|---|---|
| `consumers` | 16 | Initial consumer slots |
| `shards` | 128 | Total shard count across preference groups |
| `consumer_regions` | omitted | Groups of `{region, consumers}`; counts must sum to `consumers`; default all `centralus` |
| `shard_region_preferences` | omitted | Groups of `{shards, regions}`; counts must sum to `shards`; default all prefer `centralus` |
| `shard_load_type` | `Fixed` | `Fixed` (50 per shard), `EvenDistribution` (1–99), or `CustomizedDistribution` |
| `shard_load_distribution` | omitted | Ten bucket percentages summing to 100; required for `CustomizedDistribution` |
| `seed` | 42 | Seed for shard assignment and lifecycle scheduling |
| `warmup` | `5m` | Startup phase, before random churn |
| `duration` | `1h` | Churn phase |
| `drain_rate` | 0 | Shard drain attempts per consumer slot per minute |
| `relinquish_rate` | 0 | Attempts to immediately release one random serving shard per consumer slot per minute |
| `reconnect_rate` | 0 | Reconnect attempts per consumer slot per minute |
| `reconnect_delay` | `5s` | Transport downtime; processing continues until lease expiry |
| `quit_rate` | 0 | Abrupt quit attempts per consumer slot per minute |

Each drain event requests a handoff of one active shard. The former
`drain_shards` batch-size setting has been removed; saved presets all used one.
Random rates count attempts per slot, including attempts skipped while disconnected.

Report snapshots use the code constant `sampleInterval` (one virtual minute),
plus initial and final snapshots. Lifecycle events and shard transfers are recorded
independently of snapshots. Loading and drain acknowledgements complete without
an artificial delay; native coordinator suspension and lease timing still apply.
Runs end at `warmup + duration`. There is no recovery phase: outstanding transfers
remain pending and replacements/reconnects beyond the cutoff do not run.

Configuration keeps population totals separate from optional region groups so the
same basic scenario works without regional configuration and grouped totals can be
checked. ReconnectDelay configures retained-identity downtime. The code constant
`replacementDelay = 5 * time.Second` fixes fresh-identity replacement downtime.
Scenarios use random lifecycle rates; scripted event configuration is unsupported.
Remove `recovery`, `sample_interval`, `load_delay`, `drain_delay`, `events` and `replacement_delay` from
older scenario JSON; unknown fields are rejected.

Select a load type with `"shard_load_type":"EvenDistribution"` in the
scenario JSON. `Fixed` assigns 50 to every shard. `EvenDistribution` builds the
multiset `1 + round(98 * index / (shards - 1))`, including both endpoints, and
randomly assigns those scores to synthetic shards using the configured seed.
A single shard uses 50. Five shards use 1, 26, 50, 75, 99 in shuffled order;
more than 99 shards repeat some integer scores. Loads stay attached to their
shards through moves, reconnects, and replacements.

`CustomizedDistribution` first samples a bucket according to its percentage,
then samples an integer score uniformly within that bucket. Bucket 0 is `[1, 10)`,
bucket 1 is `[10, 20)`, through bucket 9 `[90, 100)`. The array must have exactly
ten finite, nonnegative percentages summing to 100 (rounding tolerance `1e-6`).
Zero-percentage buckets are never selected. Sampling is independent per shard,
so percentages describe probabilities rather than exact population counts.
A single shard is sampled normally. For example:

```json
{
  "shard_load_type": "CustomizedDistribution",
  "shard_load_distribution": [60, 10, 5, 5, 3, 3, 3, 3, 4, 4]
}
```

For different regional score distributions, add an optional `shard_load_distribution`
to each `shard_region_preferences` group. Each override uses the same ten-bucket
validation and requires `CustomizedDistribution`. Groups without an override use
the top-level distribution, which remains required. For example (illustrative
scores, not a conversion of production raw workload metrics):

```json
{
  "shard_load_type": "CustomizedDistribution",
  "shard_load_distribution": [60, 10, 5, 5, 3, 3, 3, 3, 4, 4],
  "shard_region_preferences": [
    {"shards": 2456, "regions": ["centralus"],
     "shard_load_distribution": [70, 10, 5, 3, 2, 2, 2, 2, 2, 2]},
    {"shards": 641, "regions": ["northcentralus"],
     "shard_load_distribution": [30, 10, 10, 10, 5, 5, 5, 5, 10, 10]},
    {"shards": 21, "regions": ["eastus2"]}
  ]
}
```

The override applies to every shard in the preference group, including groups
with multiple equally preferred regions or no preference. It follows native
preference rather than the serving consumer: moving a shard does not resample
its load. Overrides use a separate seeded random stream, preserving fallback
scores, region assignments, and lifecycle event scheduling. Reports retain the
configuration and each shard's sampled score.

Region-preference groups are independently shuffled across synthetic shard
indices, preserving each group's exact count and complete preference set. Load
assignment, region assignment, and lifecycle events use separate seeded random
streams, so setup does not consume the churn RNG. All load types use identical
region assignments with the same seed. Different seeds change both
assignments. Each JSON `shard_loads` record includes a zero-based `index` for the
synthetic shard and its native coordinator shard identifier. Native domains still
group shards by their region preference set; synthetic indices are independent
of that adapter's domain/range ordering.

These are **allocator load scores**, initialized through the coordinator's existing
published-quantile restoration path. The simulator does not wait for load tracking
to warm up or change the balancing algorithm. JSON reports list every shard's load;
CSV samples include `active_load` and `serving_load`; the HTML chart can switch
between serving shard counts and load totals.

### Regions

Each consumer slot belongs to one region for its lifetime, including transport
reconnects and replacement identities. `consumer_regions` assigns consecutive
slots in group order. `shard_region_preferences` assigns each shard a set of
**equally preferred** regions; list order does not establish priority. An empty
`regions` list means any region. A group may contain one shard for individual
preferences. Regions are arbitrary nonblank names; a preferred region does not
need to have a consumer initially.

```json
{
  "consumers": 50,
  "shards": 2000,
  "consumer_regions": [
    {"region": "centralus", "consumers": 25},
    {"region": "eastus", "consumers": 25}
  ],
  "shard_region_preferences": [
    {"shards": 900, "regions": ["centralus"]},
    {"shards": 900, "regions": ["eastus"]},
    {"shards": 200, "regions": ["centralus", "eastus"]}
  ]
}
```

These use the real coordinator's **soft region affinity**, including its existing
cross-region penalty. They are preferences, not hard placement constraints:
shards can run outside their preferred regions when capacity is absent or load
balancing favors doing so. Total shards remain 2000 in this example, not 2000 per
region. Each explicit preference group maps to a native global domain (`shards-0`,
`shards-1`, etc.) whose domain region list supplies the affinity. Configurations
without shard groups retain the original `shards` regional domain and shard IDs.
Region preferences and loads are randomly assigned to synthetic shard indices
using independent random streams, preserving the configured distribution totals.

Group names must be unique for consumer groups and within each shard group's
region list. Counts must be positive
and sum to the corresponding total; invalid configuration fails before execution.

Reports record each consumer's region, each shard's preferred regions, and each
transfer's source/destination regions. The movement table adds **Cross-region
moves**, a subset of initiated moves with different source and destination regions.
The sample slider updates a regional table of connected consumers, serving shards,
load, and shards outside every preferred region. This last metric counts only
serving shards; unserved shards are counted separately. CSV adds `region` and
`out_of_region`; JSON adds per-sample and final `out_of_region` counts.

Random event intervals are exponential. Each slot has an independent event stream
for each configured rate. An attempt during a disconnect is skipped and recorded;
a drain with no active shards is also skipped. Thus requested rates describe the
attempt process, and executed/skipped counts show the realized churn. A replacement
occupies the same slot with a new identity. Consumers stay eligible for assignment
after shard drain; a shard can return to the same consumer.

Relinquish events stop serving one uniformly selected active, serving shard and send
`Released` directly to the coordinator. The consumer stays connected and keeps its
other shards. This differs from `drain`, which requests revocation and performs a
graceful handoff. Splitter delays reassignment of an unexpectedly released active grant
until its previous lease expires, so relinquish can create a temporary serving gap.
The shard may return to the same consumer; that is recorded as a same-consumer
reacquisition rather than a cross-consumer move.

Enable random relinquish with `"relinquish_rate":0.05`. Attempts without a
serving active shard are skipped. JSON lifecycle records include the selected
shard and grant ID. The HTML lifecycle table counts executed/skipped events
separately from shard moves; transfers to another consumer appear under
`churn/relinquish`.

## Regional example scenarios

Three 12-hour scenarios share the same topology and lifecycle rates:

- [Fixed](scenarios/regional-fixed.json): every shard scores 50, with load tracking disabled.
- [EvenDistribution](scenarios/regional-evendistribution.json): scores are evenly distributed over 1–99.
- [CustomizedDistribution](scenarios/regional-customizeddistribution.json): scores follow the configured ten-bucket histogram, sampled uniformly inside each bucket.

Each uses 50 consumers (28 centralus, 22 northcentralus), 3,118 shards,
seed 42 and five minutes of warmup. Shard groups prefer centralus (2,456),
northcentralus (641), or eastus2 (21); eastus2 has no consumers. Per-consumer
rates are 0.05 drains, 0.05 relinquishes, 0.02 reconnects and 0.0002 quits
per minute. Reconnect and replacement downtime are five seconds.

These are synthetic scenarios with a topology derived from a production snapshot.
Preferences are inferred from observed ownership, domains/ranges are grouped,
loads are static, and the unit leader is approximated as an ordinary shard.
The custom histogram does not preserve within-bucket concentration or regional
score correlations. These settings do not reproduce production event rates.

Run a scenario from the repository root:

```sh
bazel test //testing/simulator:simulator_test \
  --test_filter='^TestScenario$' --nocache_test_results --test_output=streamed \
  --test_arg="-scenario=$(cat testing/simulator/scenarios/regional-fixed.json)"
```

Substitute either of the other files to compare load types. The invoking shell
reads the file and passes its contents as an argument; the test has no runtime
filesystem dependency on a scenario file. Unknown fields and invalid values
are rejected before simulation starts.

## Interpreting results

Each allocator-initiated transfer includes `balance_diff: {load, place, colo}` in
JSON, matching the production move log's adjusted-load delta. To compare with
production load-driven moves, filter `reason == "balance"`, `place == 0`, and
`colo == 0`, then deduplicate by `(source_grant, destination_grant)` over the same
measurement window. Negative `load` identifies the penalty-reduction path (A);
zero `load` identifies the intrinsic-balancing fallback (B). Zero does not mean
no improvement: path B applies separate source/destination load checks and leaves
this delta field zero. Existing reports generated before this field was added
have no allocator delta; they must not be interpreted as zero-delta moves.

The production-derived presets are synthetic experiments, not traffic replays.
The ten-bucket distribution preserves bucket probabilities with uniform integer
sampling inside each bucket; it loses within-bucket concentration and score
correlations with domain/region. Five minutes of startup does not guarantee that
a heterogeneous constrained allocation has settled. Calibrate initial ownership,
physical-shard scores/defaults, lifecycle event types/rates/selection, and effective
placement rules before using move counts to predict production. Production's
reported-score histogram alone does not provide all those inputs.


The simulator captures the coordinator's existing `Initiated grant move` structured
logs in memory. TestMain installs the capture logger once, even when verbose logging
is disabled. The coordinator creates its own background contexts, so scenario runs
serialize and the logger filters by tenant/service before posting records to the
active simulation's event loop. Parallel callers wait outside virtual time.
Source/destination worker and grant IDs link each record to the simulated handoff.
The load delta is parsed from the existing `load` field; malformed move records fail
the simulation rather than silently undercounting. Logs are emitted after handoff
messages are sent, so a late record enriches the existing transfer without resetting
its completion/interruption state or counting it twice. No coordinator API changes
or prerequisite PR are needed. Completion requires an initialized active
destination. Failed destination grants are interrupted/expired; outstanding grants
remain pending when the scenario ends.

Summary movement counts are grouped by **decision phase and reason**. Warmup
balancing is separate from churn. Initial assignments and retained
grants are excluded. Same-consumer reacquisitions are traced separately and excluded
from cross-consumer movement counts. The `Moves` column counts initiated transfers; moving the same shard twice adds
two moves. Completed, interrupted, and pending columns show their outcomes. `reassignment` covers ownership changes without a
recorded explicit drain, disconnect, quit, or balancing decision.

`active`, `allocated`, and `revoked` in samples are consumer-observed grant states,
not an instantaneous coordinator snapshot. `serving` counts initialized active
grants and revoked sources still participating in a handoff. Disconnected consumers
can serve until lease expiration; quit consumers stop immediately. `unserved` is a
synthetic availability measure, not a prediction of application request failures.

The seed fixes static load/region assignments and generated lifecycle events.
Coordinator jitter, Go map iteration,
and equal-priority allocation choices can change exact assignments between runs.
Compare aggregates across repeated runs; do not expect byte-identical traces.

V1 covers one service, regional consumers and shard preferences, fixed or evenly distributed static shard loads, transport reconnect,
single-shard drain, random single-shard relinquish, and abrupt quit with replacement. It does not model
application traffic, consumer process restart with retained identity, coordinator
failover, region-wide outage events, dynamic application load tracking, or graceful whole-consumer shutdown.

Run all simulator regression scenarios and coordinator tests:

```sh
bazel test //testing/simulator:simulator_test \
  //pkg/service/coordinator:coordinator_test
```

### Drain rates per consumer region

Each `consumer_regions` entry accepts an optional `drain_rate` in attempts per
consumer slot per minute. Omitted rates inherit the top-level `drain_rate`;
explicit zero disables drains in that region. Overrides must be finite and
nonnegative. A slot keeps its regional rate through reconnects and quit
replacements. This is based on the consumer region, not shard preference.

For example, two consumer regions can use different rates:

```json
"consumer_regions": [
  {"region": "centralus", "consumers": 28, "drain_rate": 0.03},
  {"region": "northcentralus", "consumers": 22, "drain_rate": 0.0002}
]
```

These values are attempts per consumer per minute, not regional totals. Multiply
by the region's consumer count and 1,440 for expected daily attempts. Actual
counts vary with Poisson sampling, and attempts can be skipped while disconnected
or without an active grant. Drains select the oldest active grant on the consumer.

### Independent lifecycle random streams

Each consumer slot now has separate seeded PCG generators for drain,
relinquish, reconnect and quit timing. Relinquish shard selection has a fifth
stream, so changing shard availability cannot advance an event clock. Streams
are identified by stable slot and explicit event-purpose IDs, independent of
initialization order and other consumers. They persist through reconnects and
fresh-identity quit replacements. No configuration fields are added.

With the same seed, changing CU drain rates now preserves every other slot's
unchanged event-type attempt times, including NCU drains, quits and reconnects.
Skipped attempts still advance their own clock. Attempt schedules are stable;
selected shards, outcomes, grant identities and coordinator choices can still
change as allocation changes. Altering quit/reconnect settings can therefore
still change whether a separately scheduled drain executes or is skipped.
Coordinator jitter and equal-priority choices remain unseeded.

Tests compare projected attempt schedules when enabling/increasing CU drains,
changing shard population, adding a consumer and changing the seed, including
relinquish selection and quit/reconnect activity.

### Reconnect and transfer accounting

Lease extensions apply only to grants confirmed by Assign in the current
connection generation. Retained grants that are absent from the coordinator's
reconnect assignment keep their old leases and expire, including sources whose
Revoke was lost on the disconnected transport. If reattachment restores an
expired destination grant with the same ID, its existing transfer becomes pending
again and can complete; it is not counted as another initiated move.

Active assignment provenance is recorded before the queued load callback, so a
quit between Assign and first serving still produces the correct replacement
transfer. Quitting identities interrupt every pending incoming transfer, including
ones whose Assign was never received. Retired identity tracking also interrupts
late move-log records. Completed transfers remain completed.

Five regressions reproduce these protocol/queue boundaries, including immediate
drain plus zero-downtime reconnect against the real coordinator. They failed
before the fixes and pass afterward. The simulator suite also checks late/duplicate move logs, malformed records, and
parallel callers, normally and with Go race detection in the local sandbox.
