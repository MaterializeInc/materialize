# Hedged blob gets

Persist reads batch parts and rollups, including those of the txn-wal txns
shard, through `Blob::get` calls against an object store. A single slow get on a
hot path can stall a whole replica, because everything downstream waits on
that fetch. This document describes a hedging mechanism for those gets: if
a get has not completed within a short delay, persist fires a second
request for the same key on an isolated connection and takes whichever
succeeds first.

## Goals

- Cap the freshness impact of the dead-connection failure class (described
  below) at roughly the hedge delay plus one normal get, whenever a healthy
  connection is reachable. When it is not (both requests stall), behavior
  falls back to today's, with `retry_external` (persist's retry wrapper)
  as the backstop.
- Change nothing about persist's semantics, error surface, or write paths.

## Non-Goals

- Hedging writes, deletes, or lists. Writes and deletes have side effects.
  Lists are idempotent like gets but not latency-critical: they serve
  usage collection and admin paths, not the dataflow read path that a
  stalled get freezes.
- Consensus. Stalls of the CockroachDB-backed consensus layer are a
  different failure class and are untouched by this design.
- Improving tail latency in the healthy case. The delay is chosen so that
  hedges are rare, and the budget (see Bounding amplification) bounds the
  exceptions.

## The Problem

Established connections to S3 occasionally die in ways that surface only
after several seconds. The observed pattern is a pooled connection that
stops responding, then delivers a TCP reset 5 to 15 seconds later. The AWS
SDK then marks the connection as unusable ("Connection encountered an issue
and should not be re-used") and the request is retried on a fresh
connection, either by the SDK within the same call or by `retry_external`,
persist's retry-with-backoff wrapper around every external call. That
fresh-connection retry succeeded promptly in every event we examined. The
cost is the hang before the reset, which nothing bounds tightly today.

Persist's client-side timeouts do not catch the hang. Persist configures
the SDK with `persist_blob_read_timeout` (10s, but it only limits time to
the first response byte), `persist_blob_connect_timeout` (7s, but it only
applies to connection establishment), and a 90s per-attempt timeout that is
far too long to help at freshness timescales. In the events we
characterized, the TCP reset arrived before any of these fired, so those
hung requests never incremented the corresponding timeout counters
(`mz_persist_s3_read_timeouts` and friends). A different, smaller set of
events fleet-wide does trip those counters at a low background rate.

We characterized three manifestations of the class:

- A background rate of roughly two connection events per hour per pod.
  Almost all are invisible, absorbed by the retry off any hot path.
- A single hung get on the part fetch path of a high-fan-in shard (one
  whose data feeds many dataflows, such as the txns shard), which starved
  an entire replica for about 10 seconds. Such visible starvations
  occurred roughly once per day in the busy environment where we
  characterized the class, each a multi-second freshness stall for a
  workload with single-digit-second freshness expectations.
- A regionally synchronized burst, in which the table shards of at least 17
  environments stalled 7 to 15 seconds simultaneously (each environment's
  single txns shard fanning the stall out to all of its tables) while S3
  itself served normally.

The evidence says a hedge on a separate connection would have won almost
all of these races. During one event, 58 of 67 storage collections on the
same pod (each a table or source backed by its own shard) stayed at their
normal latency, and the stalled nine were the collections behind the
affected fetch path, so the other established connections were fine at
that instant.
A peer replica of the affected one read the same shard with normal latency
at the same moment. Fleet-wide latency histograms showed no elevated gets
during the regional burst. And every observed recovery was itself a
successful fresh-connection retry, which is exactly a hedge that fired
late. One caveat feeds the design below: during the correlated bursts the
fleet's connect-timeout counters also stepped, so brand-new connections
were sometimes slow to establish mid-event, and a useful hedge therefore
needs an already-established connection waiting.

Hedging slow requests is what the object-store vendors recommend. AWS's
S3 performance guidance advises aggressively retrying slow operations on
a new connection with a fresh DNS lookup, and suggests a 2 second
threshold for small gets. Google Cloud Storage documentation recommends
hedged requests for latency-sensitive applications, and Microsoft ships
a first-class hedging strategy in its official resilience stack.

## The Design

### The wrapper

`HedgedBlob` (`src/persist/src/hedge.rs`) is a decorator implementing the
`Blob` trait. It holds two `Arc<dyn Blob>` handles on the same durable
store: the primary, and a second handle we call the sibling, on which hedge
requests run. Only `get` is hedged. Every other trait method forwards to
the primary untouched.

Because the wrapper works against the `Blob` trait object, one
implementation covers every backend: S3, GCS through the S3 interop mode,
and Azure.

### The race

A hedged get runs up to two requests, called legs below. The primary leg
starts immediately and races a timer set to the configured delay. If the
primary completes first (the full blob, not just its first byte: `Blob::get`
returns a complete result), its result is returned verbatim, success or
error.
A fast error therefore still takes today's path into `retry_external`'s
retries. Hedging targets hangs, not failures.

If the delay elapses, the hedge leg fires on the sibling, subject to the
two admission guards described under Bounding amplification below. If the
guards refuse, the get simply continues waiting on the primary, exactly
today's behavior.

Once both legs are running, the first success wins and the losing future is
dropped, which cancels its request. An error on one leg does not end the
race, and the two error cases are asymmetric. If the hedge leg errors, the
get keeps waiting on the primary alone, so a fast-failing hedge cannot turn
a get that was about to succeed into an error. If the primary errors after
the hedge fired, the get waits a delay-sized grace window for the hedge and
then returns the primary's error, so a slow hedge cannot hold the get past
the point where handing the error to `retry_external` is the better move.
Either way, when the get does fail, the caller sees the primary's error
object, unchanged. This matters because `ExternalError::is_timeout` matches
on the error string, so even attaching the hedge's error as context could
change how a caller classifies the failure.

The race operates entirely within one `retry_external` attempt, leaving
the existing retry machinery untouched as the backstop.

### Where the wrapper sits

`PersistClientCache::open_blob` (in `mz_persist_client::cache`) composes
the production blob stack. With `HedgedBlob` in place, from the outside in:

- `BlobMemCache`, an in-memory cache of recently fetched blobs,
- `Tasked`, which runs each call in a spawned tokio task so that
  Timely-polled callers cannot stall blob calls by polling them lazily,
- `MetricsBlob`, which records per-operation metrics,
- `HedgedBlob`,
- the backend (`S3Blob` or `AzureBlob`).

Each neighbor constrains that position ("below" meaning closer to the
backend). `HedgedBlob` must be below `BlobMemCache` so that cache hits
never hedge. It must be below `MetricsBlob` so that a hedged get is
recorded as one `blob_get` operation at the winner's latency, rather than
as two operations, one of which would carry the loser's hang into the very
latency histogram used to detect this failure class.

It must also be below `Tasked`, so that the race runs inside the spawned
task rather than around it. That buys two properties. First, cancellation
is real: dropping the losing leg aborts its request in flight, whereas a
`Tasked` boundary between the race and the backend would merely detach a
spawned task that keeps running, holding its socket and buffers. Second,
the hedge timer is driven by tokio. Outside the spawned task it would be
polled by Timely operators, which can poll futures arbitrarily late, and a
timer that fires late is a hedge that does not fire.

### Pool isolation

The sibling shares no connection state with the primary, so a hedge
cannot be handed a connection dying in the same event that stalled the
primary. The full case for isolation, including what a shared pool
forecloses, is under Alternatives. Three facts make the isolation work.

First, isolation cannot come from cloning: the AWS SDK client embedded in
`S3BlobConfig` is reference-counted, so cloning the config shares the HTTP
connection pool. The sibling is instead built from a second, from-scratch
config. Every `mz_aws_util::defaults()` call installs a fresh hyper client
with its own pool, and hyper resolves DNS per connection establishment,
which also satisfies the fresh-DNS guidance. Azure behaves the same way,
since `AzureBlobConfig::new` builds its own client per call. (One shared
piece remains: the SDK keeps process-global retry bookkeeping per service
and region. It never gates first attempts, only the SDK's own internal
retries.)

Second, a from-scratch config also carries its own credential provider
chain, so the sibling authenticates independently of the primary. That is
a small robustness gain and a new failure mode to watch (see
Observability).

Third, per-backend knowledge lives in one place, `open_hedge_sibling` in
`mz_persist::cfg`. For S3 and Azure it opens the independent second
handle. For file, mem, and turmoil (persist's deterministic
network-simulation backend for tests) it returns the primary instance
itself, because a second open of those would observe an independent store:
a hedged get against a different store can return `Ok(None)`, a legitimate
success, for data that exists, winning races and reporting live batch
parts as missing. Sharing the instance eliminates the hazard by
construction while still exercising the race path in tests.

Opening the sibling is best-effort. On failure, persist logs a warning and
runs without hedging for the process lifetime, visible in metrics as
`hedges_skipped{reason="unavailable"}` and as `hedge_armed` staying 0.
Persist startup must not regress for this feature.

### Keeping the sibling warm

A cold hedge can pay a connection handshake of up to the 7 second connect
timeout, and the burst evidence above shows fresh connects are sometimes
slow exactly during the correlated events. So the sibling's pool must hold
already-established connections. A background task issues concurrent
liveness gets on the sibling (fetching a reserved key that never exists, so
each ping is a cheap not-found response) immediately at startup and then
every 20 seconds, well inside hyper's eviction of connections idle for 90
seconds. (NOTE: that 90 is coincidentally equal to, and unrelated to, the
SDK's 90 second attempt timeout.) The number of concurrent pings follows
the hedge concurrency cap, because HTTP/1.1 allows one in-flight request
per connection: N concurrent pings force N warm sockets, so hedges the cap
admits are normally served warm. (A raised cap grows the warm pool at the
next cycle.)

Each warm cycle is bounded by a timeout: an unbounded hung ping would block
warming past the idle eviction, going cold exactly during the correlated
events warming exists for, and the timeout also drops the hung request,
which closes its dying socket. A successful cycle records its round-trip
time in a gauge. A failed or timed-out cycle increments a warm-error
counter and leaves the gauge alone, so a fast-failing sibling (for
example, one whose credentials rotted) cannot masquerade as a fast healthy
one.

Because the killed ping closes its socket, the warm interval bounds how
long a dead sibling socket lingers in the pool: about two intervals (one
until a ping lands on it, one more until the cycle timeout kills it).
Until that purge a hedge can check out the dead socket, hang until the
read timeout while contributing nothing, and hold its concurrency slot
the whole time, so an event's first hedges can pin both default slots
and leave later gets unhedged with
`hedges_skipped{reason="concurrency"}` climbing. At 20 seconds, hedging
therefore survives a correlated event that also hits the sibling's
sockets only if the sibling escaped it.

A single-digit interval would cut that exposure to a few seconds and let
the pool heal mid-event, and its restarted handshakes probe better than
one patient handshake: each resets TCP's exponential SYN backoff (about
one probe per second) and re-resolves DNS, which can retarget a rotated
S3 front-end address once the record's short TTL lapses. Nothing waits
on a warming handshake, so aborting a viable-but-slow one costs nothing.
The interval nevertheless stays at 20 seconds on cost: there the warmer
is a modest fraction of the fleet's organic blob gets, while at a few
seconds it would rival or exceed the fleet's entire organic blob-get
request volume. The evidence that would justify that spend is correlated
events still visible in freshness after enablement, hedges erroring on
dead sibling sockets, concurrency skips clustering at event times, and
the warm-error counter spiking alongside. The interval is a dyncfg, so
lowering it needs no release.

The warmer runs only while hedging is enabled, re-checking the flag at its
normal cadence while idle. While hedging is disabled, the sibling is
therefore fully idle: no requests, no credential refreshes (SDK providers
refresh lazily, on use), and its startup sockets idle out. The trade-off
is a short cold window after a runtime enablement: until the warmer's
first cycle, up to one warm interval plus a handshake later, a hedge can
land on an empty pool and pay a cold connect (bounded by the connect
timeout). Hedges in that window are merely no better than no hedge, never
worse, since the primary keeps racing regardless. Setting the warm
interval to zero stops warmer traffic while keeping hedging on.

### Bounding amplification

Two admission guards bound how much extra load hedging can create, each
protecting a different resource. Every hedge must pass both.

A concurrency cap (default 2) bounds memory. Batch parts can be up to the
128 MiB blob target size, and the hedge leg's buffers are invisible to the
fetch-path memory accounting (the fetch semaphore sizes its memory budget
before the blob layer runs, and on cc replicas that memory budget is tied to
the process memory limit), so the cap is what keeps unaccounted transient
buffers to about two parts. The warmer holds one warm socket per admitted
hedge (see above), so the cap also sizes the warm pool.

A token bucket, called the budget, bounds request rate and egress. The
bucket holds up to 32 tokens, a hedge costs one token, and every completed
get adds `budget_ratio` tokens (default 0.01), so under sustained slowness
hedging settles at one percent of gets. Without it, a store-wide brownout
that pushes every get past the delay would deterministically double the
request rate onto an already-degraded dependency. The same guard keeps
large gets that legitimately exceed the delay (a 128 MiB part on a
bandwidth-constrained pod) from settling into permanent double egress.
The bucket starts full, so a low-traffic process can still hedge the
rare event that motivates the feature, and 32 is an order of magnitude
above the handful of rescues one event needs per process. The blind spot
mirrors the protection: where more than one percent of gets are
legitimately slow, the drained bucket also refuses the occasional
genuine dead-connection hang, visible as `hedges_skipped{reason="budget"}`.

### Configuration

Five dyncfgs, all readable per call so LaunchDarkly changes apply live:

| Name | Default | Purpose |
| --- | --- | --- |
| `persist_blob_hedged_get_enabled` | `false` | Master switch for hedging. |
| `persist_blob_hedged_get_delay` | `2s` | Time in flight before the hedge fires. |
| `persist_blob_hedged_get_max_concurrent` | `2` | Memory bound. |
| `persist_blob_hedged_get_budget_ratio` | `0.01` | Rate bound. |
| `persist_blob_hedged_get_warm_interval` | `20s` | Warm interval. `0` disables the warmer without disabling hedging. |

The bucket capacity (32) is hard-coded: it is the bucket's shape rather
than an operational lever, and fewer knobs means less to review and fewer
LaunchDarkly entries. Note that `budget_ratio = 0` is not a kill switch,
because the bucket starts full. `enabled` is the kill switch for hedging.

Retuning the delay trades two effects. Lowering it moves the rescue
floor (a hedge win arrives no
earlier than the delay plus one round trip) but grows the false-fire
population: the race triggers on full-blob completion, not first byte,
so the gets that legitimately outlast the delay are large parts on
constrained bandwidth, and each false fire both duplicates that part's
egress and spends a budget token, so a lower delay can drain the bucket
with healthy traffic and leave a real event refused. Raising it has a
wide plateau in the other direction: a rescue stays useful as long as
the delay plus one normal get fits the freshness target, which holds up
to several seconds. Any retune should start by re-measuring the
would-be fire rate at the candidate delay (the rate in Rollout is
workload-dependent, dominated by part sizes and pod bandwidth).

What the kill switch does not cover: `enabled = false` leaves the
sibling idle (see Keeping the sibling warm), but its client and
credential chain are constructed at process start regardless, which is
what keeps enablement restart-free. The disabled standing costs are one
extra SDK client's memory per process, one extra credential resolution,
and a doubled blob-open (including the backend's own health-check get)
at startup. Removing the sibling machinery is a rollback, not a flag
flip.

### Testing

Unit tests drive the race deterministically on tokio's paused test clock:

- the hedge winning, with the get resolving at exactly the hedge delay
  while the primary is still pending (cancellation),
- the primary winning after the hedge fired,
- a fast-failing hedge not failing a get whose primary later succeeds,
- the primary failing after the hedge fired, with the hedge winning inside
  the grace window in one test and the grace window expiring in another,
- both legs failing, returning the primary's error verbatim,
- budget exhaustion and refill, the concurrency cap, and release of the
  concurrency slot when a hedged get is dropped mid-race,
- the warmer's cadence, and its absence for a same-instance sibling.

The existing `Blob` conformance suite runs against `HedgedBlob` with the
delay at zero and the primary's gets artificially slowed, so a hedge fires
and wins on every get through the full set/get/delete/list matrix over one
shared store (the test asserts hedges actually fired and won). An
additional test runs the same suite against real S3 with two genuinely
independent clients. Like the existing external-storage S3 tests it is
ignored by default and runs on demand against the external test bucket, so
true pool isolation has manual but not continuous coverage.

In CI the feature is on everywhere (repo convention for new feature
flags, wired through the mzcompose system-parameter defaults) at a 10ms
delay rather than the production 2s, so hedges genuinely fire in every
run. CI's system-parameters randomization mode can additionally set the
delay to 0s, making every blob get hedge under real workloads, and the
parallel-workload suite flips `enabled` and the delay mid-workload.
Benchmarks are the exception: the harnesses pin the planned production
configuration (hedging on, production delay and budget) through the
shared benchmarking parameter overrides, so benchmark history
accumulates against what production will run.

## Correctness

Racing two gets is sound because blobs are write-once and modify-never, and
the store is linearizable (`BlobMemCache` already relies on both). Both
legs start after the caller invokes `get`, so any result either leg returns
is a correct answer for some point during the call, including `Ok(None)`
for a key that does not exist. The one way this argument breaks is if the
two handles observe different stores, which is exactly what
`open_hedge_sibling`'s per-backend contract prevents.

Write-once is a statement about blob contents, not key existence: a
concurrent delete can legitimately change the answer between the two legs,
and the hedge leg can observe the store up to the delay later than the
primary would have. What makes that irrelevant in practice is the same
thing that protects sequential gets today: persist only deletes blobs that
no live reader can reference, enforced by seqno leases. (This is also why
a runtime cross-check that both legs agree was rejected, see Alternatives.)

The error surface is unchanged: callers see a success or the primary's
error exactly as they would have without hedging, so the error text, the
timeout heuristic, and the determinate versus indeterminate
classification are all preserved.

## Observability

New metrics, all prefixed `mz_persist_blob_` (elided in prose below):

| Metric | Type | Meaning |
| --- | --- | --- |
| `hedges_fired` | counter | Gets that fired a hedge request. |
| `hedges_won` | counter | Gets the hedge request won. |
| `hedge_won_seconds` | histogram | End-to-end latency of gets the hedge won. |
| `hedges_skipped` | counter, by `reason` | Hedges refused: `budget`, `concurrency`, or `unavailable`. |
| `hedge_errors` | counter | Hedge legs (not primaries) that completed with an error. |
| `hedge_warm_errors` | counter | Warm cycles that failed or timed out. |
| `hedge_armed` | gauge | 1 if the process opened a sibling and can hedge. |
| `hedge_rtt_latency` | gauge | Round-trip time of the last successful warm cycle. |

Enabling hedging makes the old detection signals go quiet. The hung
primary is cancelled at the delay, so the background increments of
`mz_persist_s3_read_timeouts`, the SDK's connection-poisoning log lines,
and spikes in the persist-observed blob round-trip gauge
(`mz_persist_external_rtt_latency`, which measures through the hedged
stack) largely stop occurring for this class. `hedges_won` replaces them
as the detector.

Two further notes. On accounting: the pre-existing S3 request counters
(`mz_persist_s3_operations`, for example its `get_part` label) count both
legs of a hedged get plus the warmer's pings, so they can exceed the logical
`blob_get` operation count. On health: `hedge_warm_errors` and
`hedge_errors` are the signals that the sibling's independent credential
chain has rotted, which would otherwise silently turn the feature into a
no-op. Both only move while hedging is enabled, so expect any rot to
surface within the first warm cycles after enablement. `hedge_armed` only certifies
that the sibling opened at process start.

## Alternatives

### Hedging on the primary's pool

Hedging on the primary's own connection pool would need no sibling
machinery, and in the dominant event shape (one connection dies while
its pool-mates stay healthy, which is why the same-pool retry succeeds
promptly today) it would usually work. Isolation is for the residual
minority of correlated, path-scoped events, where connection kills
clustered within a second on one pod and fresh connects stalled
fleet-wide, so a shared pool is most likely to hand the hedge a sick
connection exactly when the hedge matters most. A shared pool also
forecloses levers that matter regardless of fate sharing: it pins the
hedge to the pool's cached DNS answer and front-end address, lets a
checkout queue behind the very jam the hung get is part of, and cannot
be kept warm without distorting the primary's pool.

### Just lowering the client timeouts

Lowering the client timeouts instead of hedging was the 2023 answer to
this class (connect and read were cut from 30 and 60 seconds to today's
7 and 10), and the residual hang is what that lever left behind.
Tightening further runs into the structural limits The Problem lists,
and a mid-body hang stays bounded only by the 90 second attempt timeout,
which cannot come down without killing legitimately long large-part
fetches. A timeout is also sequential and forces a trade: it converts a
possibly-about-to-succeed request into an error, pays backoff and
restart, and retries on the same pool with no fresh connection or DNS
resolution guaranteed, so its threshold must stay conservative. A hedge
is concurrent, so firing early costs a bounded duplicate rather than a
lost success, which is why hedging affords a 2 second trigger where a 2
second timeout could not.

### Per-chunk hedging

Hedging per multipart chunk inside `S3Blob::get` is a possible refinement:
`S3Blob::get` already fetches large blobs in 8 MiB parts, so a per-chunk
hedge would duplicate one chunk instead of the whole blob and could trigger
on missing first bytes rather than wall clock. It is S3-only, duplicates
the race logic per backend, and is a natural follow-up once the generic
version has proven itself, not the first version.

### A pool-less sibling

Disabling pooling on the sibling client (max idle connections of zero)
would guarantee isolation and fresh DNS with no warmer machinery, but it
puts a full TCP and TLS handshake on the critical path of every hedge,
which is up to 7 seconds during exactly the correlated events the feature
targets.

### A both-legs debug mode

A debug mode that awaits both legs and asserts they agree was rejected as
unsound, not merely unnecessary: as Correctness explains, two gets of the
same key are only required to agree for keys protected by a live lease, so
the assertion would misfire on legitimate delete races.

### A hedging-aware Blob trait

Extending the `Blob` trait with a hedging-aware method was rejected because
it pushes a transport concern into a five-method correctness-critical trait
that most implementations have no answer for.

### Hedging in maelstrom

A configuration for maelstrom was considered and deferred. Maelstrom's
blob is neither S3 nor Azure, so its sibling would be the same instance,
and `UnreliableBlob` injects errors but not delays, so hedges would
essentially never fire. The
configuration becomes worthwhile together with delay injection in
`UnreliableBlob`.

## Rollout

The feature ships dark: the code path is present everywhere, but hedging
is off by default in production (and on in CI, see Testing) until enabled
at runtime. Enablement happens per environment through LaunchDarkly, which
requires the LD flags (at minimum `persist_blob_hedged_get_enabled` and
the delay) to be created first: until then the dyncfgs exist only with
their code defaults. On enablement,
expect the old detection signals to fade (see Observability),
`hedges_fired` to run at a low background rate (measured on a busy
reference environment: gets over the 2s delay ran at roughly 4 per day
on the busiest replica and a few per hour environment-wide, so
single-digit fires per day per process is the expected order, with
hydration bursts of large parts as the exception the budget caps), and
`hedges_won` to step where the old signals would have fired.
Success is the dead-connection class becoming sub-breach: each
instance should cap near the hedge delay plus one normal get, and the
roughly-daily visible freshness stalls attributed to the class in a busy
environment should disappear from per-minute freshness data. The known
residual is the case where both legs stall (a correlated event defeating
the warm pool, or a drained budget), which falls back to today's behavior
and stays visible as `hedge_won_seconds` outliers and
`hedges_skipped` increments.
