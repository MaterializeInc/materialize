# Subscribe SDKs: canonical clients for `SUBSCRIBE` and a framework for external sinks

- Associated:
  - (TBD: link the Subscribe SDK program epic once filed)
  - [MaterializeIncLabs/mz-redis-sync](https://github.com/MaterializeIncLabs/mz-redis-sync) (prior art)
  - [MaterializeIncLabs/novu-materialize-integration](https://github.com/MaterializeIncLabs/novu-materialize-integration) (prior art)
  - [Durable subscriptions pattern](https://materialize.com/docs/transform-data/patterns/durable-subscriptions/) (documented protocol this work packages)
  - [database-issues#5182](https://github.com/MaterializeInc/database-issues/issues/5182) (errors poison subscribe dataflows, relevant server limitation)

## The Problem

`SUBSCRIBE` is the primitive that turns Materialize from a database you query
into a database that drives your systems. It is how users push incrementally
maintained results into caches, notification services, feature stores,
websockets, and anything else that is not reachable by `CREATE SINK`. Today we
give users the primitive and a pattern document, and then every user rebuilds
the same client-side machinery from scratch.

That machinery is genuinely hard to get right. The correct protocol for
consuming `SUBSCRIBE` durably is:

1. Subscribe `WITH (PROGRESS, SNAPSHOT true)`.
2. Buffer updates until a progress message proves a timestamp is closed.
3. Apply the closed batch and persist its frontier atomically.
4. On restart, resume `WITH (PROGRESS, SNAPSHOT false) AS OF frontier - 1`,
   because `SNAPSHOT false` emits updates strictly *after* the `AS OF`
   (`src/compute/src/sink/subscribe.rs`, the `should_emit` closure), so
   subtracting one is what makes the resume gap-free.
5. Keep `RETAIN HISTORY` on the subscribed object wider than your worst-case
   downtime, and handle the compaction-horizon error when it is not.

Each step has a failure mode that is invisible in testing and destructive in
production. We know this because every existing implementation we could find,
including our own demos, gets a different step wrong:

| Codebase | Commit discipline | Resume | Idempotency | Retractions |
| --- | --- | --- | --- | --- |
| mz-redis-sync (Labs) | Correct: commits data and frontier atomically at progress rows | Omits `SNAPSHOT` on resume, which defaults to `true`, so every restart replays the full snapshot (contradicting its own commit message) | Inherent (keyed writes) | Delegated to `ENVELOPE UPSERT`, but `key_violation` crashes the process and poisons restarts |
| novu-materialize-integration (Labs) | Persists the timestamp of the *first row* of a batch. A crash mid-batch permanently loses the rest of that timestamp | Wall-clock guess of whether the checkpoint is still inside the retention window | Content-hash keys (false-dedups identical payloads across time) | Maps `mz_diff = -1` to a notification revoke, a great idea, implemented with two bugs |
| novu-materialize-poc (Labs) | Correct idea: buffer until the timestamp advances | No `PROGRESS`, so batch boundaries are a heuristic | None, duplicates on replay | Ignored, deletes trigger spurious notifications |

(One fairness note on the first row: because that sink writes keyed upserts
transactionally, the snapshot replay is wasteful rather than incorrect,
every restart rewrites the full view into Redis. It still contradicts the
commit message that claims to avoid it, which is its own kind of evidence.)

None of the three reconnects on failure. None has a single test. One declared
a retry library as a dependency and never imported it. These are demos,
written quickly, some AI-assisted, and that is precisely the point: this is
what subscribe consumers look like when they are written the way real
integrations get written, by whoever shows up with a deadline. The
correctness burden currently sits on the application side of an API this
subtle, so this is the result. Throughout this document, the demos serve
only as evidence of the failure modes. Every claim about correct behavior is
anchored in the Materialize codebase and documentation, which are the ground
truth this design is built on.

Beyond correctness, there are stability hazards that clients must be built
around and currently are not:

- The coordinator buffers subscribe output in an unbounded channel with no
  backpressure (`src/adapter/src/active_compute_sink.rs`, acknowledged
  TODO). A slow consumer translates into `environmentd` memory growth.
  Separately, subscribe output is subject to `max_result_size` in the
  compute-client layer (`PendingSubscribe::stash`), so an initial snapshot
  larger than that limit terminates the subscription with a "total result
  exceeds max size" error, a sizing hazard no demo handles.
- Once a subscribe dataflow produces an error it repeats that error forever
  (database-issues#5182). Recovery is always reconnect-shaped, and clients
  that do not know this retry into the same error.
- The compaction-horizon error is distinguishable only by matching the error
  message text, under a generic SQLSTATE (`DATA_EXCEPTION`, shared with
  other failures). Worse, the text is not stable: the timestamp-selection
  rework (#34712) replaced "Timestamp (X) is not valid for all inputs" with
  "could not find a valid timestamp for the query". mz-redis-sync regexes
  the old text, so its one bespoke error handler is already broken against
  current Materialize, and its non-matching branch silently swallows the
  error and re-enters `FETCH` inside an aborted transaction, an infinite
  loop. This is what message-text dispatch does over time.

Finally, our docs teach the naive version. Every per-language client page
(`doc/user/content/integrations/client-libraries/*.md`) shows a bare
`DECLARE`/`FETCH` loop with no progress handling, no resume, and no
reconnection. The durable-subscriptions pattern doc is correct and complete,
but it is prose describing a protocol, and prose does not compose into
applications.

The problem, stated in one line: **we make users hand-implement a consistency
protocol that we already know how to implement, and they predictably get it
wrong.**

## Success Criteria

A solution is successful if:

1. **Correct by construction.** A user following the happy path of the SDK
   cannot commit an unclosed timestamp, cannot get the `AS OF` boundary
   arithmetic wrong, and cannot silently lose or duplicate updates across a
   restart. The resume token is opaque. The batch is the unit of consumption.
2. **Minutes to first stream.** `install`, paste a connection string, write
   five lines, see typed updates. The SDK works against Materialize Cloud,
   self-managed, and the emulator without configuration differences.
3. **Stable under failure.** Network blips, cluster restarts, and process
   crashes are recovered automatically or surfaced as one of a small set of
   typed, documented errors with remediation guidance. Memory use is bounded
   and documented, including during snapshots.
4. **General over destinations.** The sink framework cleanly supports the two
   families of destinations, keyed-state stores (Redis, DynamoDB, Postgres
   upsert tables, search indexes, caches) and event receivers (webhooks,
   queues, notification services). Redis ships first as the reference
   implementation, not as a special case.
5. **Consistent across languages.** TypeScript, Python, and Rust expose the
   same model with idiomatic surfaces, verified by a shared conformance suite
   rather than by hope. Behavior is specified in a written protocol document.
6. **Honest about guarantees.** The SDK names its delivery guarantees
   precisely, exactly-once state application for transactional sinks and
   at-least-once with idempotency keys for side-effecting sinks, and makes
   the user pick one explicitly.
7. **Testable by users.** Sink authors get a testing story from us, recorded
   stream fixtures and an emulator harness, so their sinks can be tested
   without a live environment.

Quantitative proxies: replace mz-redis-sync with an SDK-based equivalent at
functional parity plus the known bugs fixed. A chaos suite that kills the
process at arbitrary points and verifies destination convergence passes for
every language. The per-language docs pages can be rewritten on top of the SDK
with fewer lines than the naive loops they replace.

## Out of Scope

- **Rewriting subscribe server internals.** The SDK is buildable on today's
  semantics, and a scoped set of server-side companion changes is planned as
  part of this program (see "Materialize-side workstream"). Anything beyond
  that scoped list, such as a general subscribe protocol redesign, is out of
  scope.
- **Exactly-once side effects.** No client library can make an HTTP call
  exactly once. We provide at-least-once delivery plus idempotency-key
  helpers, and we say so plainly.
- **A browser/edge SDK.** The WebSocket endpoint (`/api/experimental/sql`)
  does support `SUBSCRIBE`, but it is experimental. A browser client is
  future work gated on stabilizing that surface.
- **Go, Java, and other languages in v1.** The spec and conformance suite are
  designed to make additional languages cheap, but the v1 program is Rust,
  Python, and TypeScript, in that order. GA can precede the TypeScript
  release (see the Delivery plan).
- **Private-preview subscribe features.** `ENVELOPE DEBEZIUM` and
  `WITHIN TIMESTAMP ORDER BY` are behind feature flags. The SDK's model
  leaves room for them but v1 does not depend on them.
- **Scale-out consumption.** Sharding one subscription's updates across
  multiple parallel workers has no server-side support and is not attempted.
  High availability itself is in scope (fencing in v1, active-passive
  failover shortly after, see Layer 2), the exclusion here is only parallel
  fan-out of a single subscription for throughput.
- **General driver features.** The SDK consumes `SUBSCRIBE` (and the small
  amount of SQL needed around it). It is not a query builder, ORM, or general
  Materialize client.

## Solution Proposal

Ship a family of client libraries, one written behavior spec, and one shared
conformance suite, under a working title of the **Materialize Subscribe SDK**:

```
                        +--------------------------------------+
                        |  Layer 3: sink kit + built-in sinks  |
                        |  (Redis reference sink, webhook sink)|
                        +--------------------------------------+
                        |  Layer 2: durable subscribe          |
                        |  (checkpoints, guarantees, policies) |
                        +--------------------------------------+
                        |  Layer 1: subscribe client           |
                        |  (typed stream of closed batches)    |
                        +--------------------------------------+
                        |  native pg driver per language       |
                        |  (node-postgres / psycopg / tokio-pg)|
                        +--------------------------------------+
```

The layers are strictly separated so each is independently useful. A user who
only wants live updates in a websocket handler uses Layer 1 and never sees a
checkpoint. A user syncing Redis uses Layer 3 and never sees a `FETCH`.

### The model: five opinions

The SDK's UX comes from five opinions applied uniformly across languages:

1. **The unit of consumption is the closed timestamp, never the bare row.**
   The stream yields consistent batches. Each batch contains every update for
   an interval of timestamps that a progress message has proven complete,
   plus the frontier and a resume token. Users physically cannot observe a
   half-delivered timestamp. (An advanced `raw()` mode exposes per-row
   delivery for power users, clearly documented as forfeiting the batch
   guarantees.)
2. **Resume tokens are opaque.** A token encapsulates the frontier, the
   `SNAPSHOT false AS OF frontier - 1` arithmetic, the query fingerprint, and
   a fencing epoch. Users store bytes and hand them back. There is no
   timestamp arithmetic in user code.
3. **Guarantees are named and chosen, not implied.** Durable consumption
   requires the user to pick `transactional` (checkpoint commits atomically
   with effects, exactly-once state application) or `at_least_once` (effects
   first, checkpoint after, idempotency keys provided). There is no default
   that quietly picks one.
4. **Reconnection is the SDK's job.** Transient failures are retried with
   jittered backoff and automatic resume. Everything else surfaces as one of
   a small typed error set, each carrying remediation guidance.
5. **One spec, one conformance suite, three implementations.** Language
   surfaces are idiomatic, behavior is identical, and identical is checked by
   machines.

### Layer 1: the subscribe client

Layer 1 wraps the ecosystem-native PostgreSQL driver and runs the
`DECLARE`/`FETCH` loop, presenting a typed stream.

Illustrative TypeScript (all sketches in this doc are illustrative, not final
API commitments):

```typescript
import { SubscribeClient } from "@materializeinc/subscribe";

const client = await SubscribeClient.connect(process.env.MZ_URL!);

const stream = client.subscribe({
  query: "SELECT id, amount FROM winning_bids",
  envelope: { upsert: { key: ["id"] } },
});

for await (const batch of stream) {
  // batch.frontier: bigint            -- every update with timestamp < this is present
  // batch.resumeToken: ResumeToken    -- opaque, serializable
  // batch.isSnapshot: boolean
  // batch.updates: Upsert<Row>[] | Delete<Key>[] | KeyViolation<Key>[]
  render(batch.updates);
}
```

Illustrative Python:

```python
from materialize_subscribe import connect, UpsertEnvelope

client = connect(dsn)
for batch in client.subscribe(
    "SELECT id, amount FROM winning_bids",
    envelope=UpsertEnvelope(key=["id"]),
):
    for update in batch.updates:
        ...
```

Illustrative Rust:

```rust
let client = mz_subscribe::Client::connect(&dsn).await?;
let mut stream = client
    .subscribe(Subscribe::query("SELECT id, amount FROM winning_bids")
        .envelope_upsert(["id"]))
    .await?;
while let Some(batch) = stream.try_next().await? {
    apply(batch.updates());
}
```

Responsibilities and design points:

- **Batching by progress.** The client requests `PROGRESS` always. It buffers
  updates and emits a batch when the frontier advances. Progress-only
  advancement (idle periods) is surfaced as an empty batch carrying a fresh
  resume token, so downstream checkpoints keep advancing during quiet hours.
  This directly fixes the failure mode where an idle subscription's
  checkpoint ages out of the retention window.
- **Envelope decoding.** Three modes map to typed events:
  - default diff envelope: `Insert{row, diff}` / `Retract{row, diff}` with
    multiplicities preserved (a `diff` of -3 is three retractions and the
    type says so),
  - `ENVELOPE UPSERT`: `Upsert{key, value}` / `Delete{key}` /
    `KeyViolation{key}`. Key violations are a first-class event, not an
    exception, because a crash-restart loop cannot fix them.
- **Snapshot streaming with bounded memory.** The initial snapshot is one
  giant timestamp and can exceed client memory if buffered whole (the
  mz-redis-sync failure mode). The client streams it as chunked batches
  flagged `partial: true`, with the closing chunk carrying the resume token.
  Consumers that need atomic snapshot visibility get help from Layer 3.
  Server-side, a snapshot exceeding `max_result_size` terminates the
  subscription with a "total result exceeds max size" error regardless of
  client behavior. The SDK surfaces that as a typed error with sizing
  remediation rather than as an opaque stream failure.
- **Fetch pacing.** `FETCH <n> c WITH (timeout ...)` with adaptive sizing.
  The client always drains the server promptly and applies backpressure to
  the application from its own bounded buffer, because unread results buffer
  without limit in `environmentd`. If the application cannot keep up, the
  documented options are a larger bound, spill-to-disk (opt-in), or letting
  the buffer block the FETCH loop and accepting server-side growth. The SDK
  makes the trade-off visible instead of implicit.
- **Typed errors.** All failures map to a small taxonomy:

  | Error | Meaning | Default behavior |
  | --- | --- | --- |
  | `Transient` | network blip, cluster restart, replica loss | auto-reconnect and resume |
  | `CompactionHorizon` | resume point older than retained history | surface with policy hook (see Layer 2) |
  | `DependencyDropped` | subscribed object or its cluster dropped | surface, policy hook can `refollow` recreated objects (see Layer 2) |
  | `StreamPoisoned` | dataflow error (e.g. division by zero in the view), repeats forever per database-issues#5182 | surface with explanation, reconnect will not fix until the data or view changes |
  | `SchemaMismatch` | resumed subscription's columns differ from checkpoint fingerprint | surface with policy hook |
  | `Fatal` | auth, TLS, SQL errors in the user's query | surface immediately |

  The mapping rules key on structured error codes, which is the first item
  in the Materialize-side workstream, sequenced to land before the SDK's
  stable release. Because the SDK must also work against server versions
  that predate those codes, a stable release may carry a message-text
  fallback in exactly one internal function, gated on server version,
  covered by conformance tests pinned to each legacy message (the text has
  already changed once across releases, see the Problem section), and
  removed when pre-code versions age out of support. No other
  message-string behavior ships anywhere.
- **One direct connection per subscription.** A subscription owns a dedicated
  connection for its lifetime. The SDK documents, and detects where it can,
  that transaction-mode poolers (PgBouncer and friends) break the
  `DECLARE`/`FETCH` loop. Applications running many subscriptions get an
  explicit connection budget instead of a surprise.
- **Cluster targeting and resume cost.** The subscribe options include the
  target cluster (`SET cluster`), and the docs shipped with the SDK
  recommend a dedicated serving cluster for subscriptions. The SDK surfaces
  the cost asymmetry on resume: `SUBSCRIBE <object>` on a materialized view
  or table resumes cheaply from storage, while `SUBSCRIBE (SELECT ...)`
  rebuilds a dataflow over retained history. Guidance: materialize the view
  you sink.
- **Ordering is per timestamp, not within it.** Updates within a closed batch
  are unordered (server-side `WITHIN TIMESTAMP ORDER BY` exists but is
  private preview). Keyed-state sinks are insensitive to this. Event sinks
  that need deterministic order within a timestamp can sort client-side via
  a batch option, and the docs say when that matters.
- **Bounded subscriptions.** `UP TO` is exposed as a first-class option, which
  makes deterministic reads of a timestamp window possible. This is useful in
  its own right and is how much of the SDK's own test suite drives itself.
- **Session hygiene.** Sets `application_name`, suppresses the welcome notice
  where drivers require it, exposes the connection's cluster and role in a
  startup log line, and pre-validates the query with
  `SELECT * FROM (<query>) WHERE FALSE LIMIT 0` for fail-fast schema and
  permission errors (an mz-redis-sync trick worth canonizing).
- **Type mapping contract.** Each language documents a total mapping from
  Materialize types to SDK values, including `numeric` precision, temporal
  types, arrays, `jsonb`, and NULLs. The conformance vectors include every
  type.

### Layer 2: durable subscribe

Layer 2 adds checkpointing and delivery guarantees on top of Layer 1.

```typescript
import { durableSubscribe, redisCheckpoints } from "@materializeinc/subscribe";

await durableSubscribe(client, {
  name: "bids-to-webhook",                    // checkpoint identity
  query: "SELECT id, amount FROM winning_bids",
  checkpoints: redisCheckpoints(redis),       // pluggable store
  guarantee: "at_least_once",
  onCompactionHorizon: "resnapshot",          // or "fail", or callback
  handler: async (batch, ctx) => {
    for (const event of batch.updates) {
      await sendWebhook(event, { idempotencyKey: ctx.idempotencyKey(event) });
    }
  },
});
```

Design points:

- **Checkpoint stores are pluggable and tiny.** The interface is
  `load(name) -> ResumeToken | null` and `store(name, token)`. Ships with:
  the destination itself (the strongly recommended default, see Layer 3),
  a Materialize table (zero extra infrastructure, the Novu demos' good idea),
  Postgres, and a local file (development).
- **Two named guarantees.**
  - `transactional`: the handler receives the batch and the token and must
    commit both atomically (Layer 3 sinks do this for you). Result:
    exactly-once state application. The destination always equals the source
    at some real Materialize timestamp.
  - `at_least_once`: effects run first, the checkpoint commits after the
    handler returns. Result: replays possible after a crash, and
    `ctx.idempotencyKey(event)` provides a stable key derived from
    subscription name, `mz_timestamp`, key columns, and an ordinal within
    the batch. This fixes both observed idempotency failures: content-only
    hashes that false-dedup distinct events, and no key at all.
- **Retraction policy for event sinks.** Side-effecting consumers declare
  what a retraction means: `ignore`, or `compensate(fn)` (the Novu demo's
  retraction-to-revoke mapping, promoted to a supported concept).
  Compensation needs a correlation identity that is stable between an
  insert and its later retraction, which the delivery idempotency key
  deliberately is not (it includes the timestamp and ordinal). The SDK
  therefore provides `ctx.correlationKey(event)`, derived from the row's
  key columns only, for exactly this purpose. The Novu demo collapsed both
  identities into one content hash, which made revokes work but false-dedups
  distinct events. Separating the two identities is the fix.
- **Compaction-horizon policy.** When the checkpoint is older than retained
  history, policy decides: `fail` (default, explicit), `restart_from_now`
  (accept a gap, for notification-style consumers), or `resnapshot` (rebuild
  destination state, meaningful mainly for Layer 3 keyed sinks which know how
  to reconcile). The SDK detects the condition from the typed error, not from
  wall-clock guessing against `RETAIN HISTORY` config.
- **Query fingerprinting.** The token embeds a fingerprint of the query text
  and output schema. Resuming with a changed query surfaces `SchemaMismatch`
  and the policy hook chooses between failing and re-snapshotting. No demo
  handled this. Real deployments hit it on their first schema change. Tokens
  themselves carry a format version so stored checkpoints survive SDK
  upgrades.
- **Object swaps and blue/green deployments.** Recreating or swapping the
  subscribed object (the recommended zero-downtime deploy pattern, and what
  tooling like mz-deploy automates) terminates the subscription with
  `DependencyDropped`. The policy hook offers `refollow`: re-resolve the
  object by name, resume from the checkpoint if the new object's schema
  fingerprint and retained history allow it, otherwise fall through to the
  re-snapshot policy. Without this, every view deploy is a paging incident
  for whoever runs the sink.
- **Fencing and high availability.** The token carries an epoch. Checkpoint
  stores implement compare-and-set on `(epoch, frontier)`, refusing writes
  from a stale epoch and refusing frontier regression. In v1 this makes the
  two-instances mistake loud instead of silently corrupting. The production
  HA story builds on the same primitive: active-passive failover where
  standby instances contend for a lease in the checkpoint store, the lease
  holder bumps the epoch on acquisition, and the fencing rule guarantees at
  most one writer even across partitions. This is the Kafka Connect and
  Debezium recovery model, no consensus service required beyond the
  checkpoint store's compare-and-set. If real deployments show the
  client-side lease is not enough, the Materialize-side workstream leaves
  room for a server-assisted primitive (for example, named subscriptions
  with server-enforced single ownership), designed properly rather than
  worked around.
- **Liveness watchdog.** Optional max-staleness on progress. If the frontier
  stops advancing (hung connection, wedged cluster) the SDK reconnects or
  surfaces, with correct units, unlike the demo whose watchdog compared
  seconds to milliseconds and could never fire.
- **Graceful shutdown.** On signal: finish the in-flight batch, checkpoint,
  close the cursor, disconnect.

### Layer 3: the sink kit and built-in sinks

Layer 3 answers "I want this view synced into X" with a small interface per
destination family and the hard problems solved once.

Positioning: the SDK core (Layers 1 through 3's interfaces) is the fully
supported product surface with a day-one stability commitment. Built-in
sinks are supported reference implementations of those interfaces, Redis
first. The framework is the product, destinations are instances of it, and
users building their own sinks are as much the audience as users running
ours.

**Keyed-state sinks** (Redis, DynamoDB, Postgres tables, search indexes).
Consume `ENVELOPE UPSERT`. The contract:

```python
class KeyedStateSink(Protocol):
    def load_token(self) -> ResumeToken | None: ...
    def apply(self, batch: UpsertBatch, token: ResumeToken) -> None:
        """Apply updates and persist token atomically. Must be idempotent."""
    def resync_begin(self, generation: int) -> None: ...
    def resync_end(self, generation: int) -> None:
        """Make generation current and sweep keys from older generations."""
```

The kit drives the state machine: initial snapshot, incremental batches,
resume, and re-snapshot after compaction-horizon or state loss. The
`resync_*` hooks solve the two problems every demo either hit or documented
away:

- *Atomic snapshot visibility.* Large snapshots arrive as partial batches. A
  sink can stage them under a generation marker and flip visibility at
  `resync_end`, or accept eventually-visible snapshots. The kit supports
  both, the sink declares which.
- *Orphan reconciliation.* Re-snapshotting after state loss only upserts
  currently-live keys. Keys deleted while offline linger forever unless swept
  (mz-redis-sync's README admits exactly this gap). Generation-tagged sweep
  at `resync_end` closes it.

**Event sinks** (webhooks, queues, notification services). Consume the diff
envelope through Layer 2's `at_least_once` mode with idempotency keys and
retraction policy. The kit ships a generic webhook sink as the reference for
this family.

**The Redis reference sink.** First concrete sink, the successor to
mz-redis-sync:

- Data models: string (`SET key value`), hash (row columns as fields), and
  JSON value encoding. Multi-column keys via a documented key template.
  NULLs handled per a documented rule instead of crashing the driver.
- Writes: one `MULTI`/`EXEC` per closed batch containing the data commands
  plus the checkpoint `SET`. This is the atomic
  data-plus-progress-in-one-transaction rule that the durable-subscriptions
  pattern doc prescribes, applied with the destination as the transaction
  boundary (mz-redis-sync instantiates the same idea). Large batches chunk
  under a generation marker with a sweep, trading atomic visibility for
  bounded transactions, per the sink's declared mode.
- Checkpoint key namespaced under the sink's prefix (the demo's frontier key
  bypassed its own prefixing and could collide with data).
- Fencing via a Lua compare-and-set on `(epoch, frontier)`.
- `key_violation` events surface through a policy hook (log-and-skip or
  fail) instead of crashing into a poison-restart loop.

**Standalone runner as a reference example.** The repo ships a small
config-file-driven runner built on the Rust implementation (working name
`mz-sink`), positioned as an example application, not a core deliverable:
it lives in `examples/`, demonstrates end-to-end sink deployment for
non-Rust users, replaces mz-redis-sync as the thing we point demos at, and
serves as the long-running soak target for the chaos suite. The SDK is the
product people build with however they please, the runner shows one good
way.

### Language strategy: native implementations, one spec

Three native implementations, not a shared Rust core with bindings:

- TypeScript on `pg` (node-postgres), Python on `psycopg` (v3, async-capable,
  sync facade), Rust on `tokio-postgres`.
- The hard, subtle part of this project is a small protocol state machine.
  It is precisely the kind of logic a written spec plus shared test vectors
  can pin down across implementations.
- The expensive part of a shared-core approach is everything else: TLS,
  auth, connection lifecycle, event-loop integration in Node, asyncio
  integration in Python, packaging native modules for every platform. The
  ecosystem drivers already solve all of it, natively and idiomatically, and
  users already trust them.

The spec (working name **Subscribe Consumption Protocol**, versioned,
`spec/` in the SDK repo) defines: batching and progress semantics, resume
token contents and boundary arithmetic, guarantee modes and their crash
matrices, error taxonomy and mapping rules, envelope decoding, and the type
mapping. Each language's README links its conformance report.

This is the proven model for exactly this kind of SDK. MongoDB drivers are
the canonical example: the major drivers are per-language native
implementations, unified by the public `mongodb/specifications` repo of
prose specs plus JSON test files, and MongoDB change streams use an opaque
resume token with the same role as ours. Kafka demonstrates both paths at
once: the Java client is the spec-bearing reference, native implementations
displaced the C-binding clients where binding pain was highest (kafka-go
over cgo bindings, KafkaJS over node-rdkafka), while librdkafka bindings
persist where they work well enough. The lesson is that bindings are a
per-ecosystem cost gamble, native is uniformly safe. Debezium shows the
cost of skipping multi-language entirely, its embedded engine is JVM-only,
which is a large part of why non-JVM teams never adopted it directly.

Delivery order: **Rust and Python first.** Rust is the reference
implementation, written against the spec as the spec is written, and it is
the language of the team that must vouch for the semantics. Python is the
fastest path to validating the UX with real integration builders and
carries the prototype. **TypeScript follows** once the spec has survived two
implementations, **Go later**. The spec makes each additional language a
mechanical, conformance-checked project.

Suggested packaging (open question below for final naming):

| Language | Core | Redis sink |
| --- | --- | --- |
| TypeScript | `@materializeinc/subscribe` | `@materializeinc/sink-redis` |
| Python | `materialize-subscribe` | extra: `materialize-subscribe[redis]` |
| Rust | `mz-subscribe` (crates.io) | feature: `mz-subscribe/redis`, binary `mz-sink` |

One monorepo (`MaterializeInc/subscribe-sdk` or Labs equivalent) holding
`spec/`, `conformance/`, and the three implementations, so a spec change and
its cross-language fallout land in one PR.

### Testing

The demos shipped zero tests between them. The SDK inverts this, and the test
infrastructure is a deliverable users get too:

1. **Conformance vectors.** Language-agnostic recorded subscribe streams
   (JSON) covering: progress interleavings, multiplicities beyond one,
   upsert/delete/key_violation, partial snapshots, idle progress,
   every mapped type, and error frames. Every implementation must replay
   them to identical decoded events and tokens.
2. **Emulator integration suite.** Docker-based (testcontainers) scenarios
   against the Materialize emulator: snapshot then stream, resume across
   client restart, resume across emulator restart, `RETAIN HISTORY` expiry
   producing `CompactionHorizon`, dropped view producing `DependencyDropped`,
   poisoned dataflow producing `StreamPoisoned`.
3. **Chaos suite (the flagship).** Kill the sink process with SIGKILL at
   randomized points (mid-batch, between apply and checkpoint, mid-snapshot),
   restart, repeat, then assert the destination equals
   `SELECT ... AS OF <frontier>` at the checkpointed frontier. Run for both
   guarantee modes, asserting convergence for `transactional` and
   convergence-with-duplicates-absorbed for `at_least_once`.
4. **Fencing test.** Two instances against one checkpoint, assert the stale
   epoch is refused and the destination stays consistent.
5. **Property tests.** Random diff streams through envelope decoding and
   batching, asserting consolidation and frontier invariants.
6. **User-facing test kit.** The fixture player from (1) is exported so sink
   authors can unit-test their `apply` implementations offline.

### Observability

Built-in, consistent across languages: frontier lag (wall clock minus
frontier, the one metric every operator wants and no demo had), batches and
updates applied, reconnect count, checkpoint age, buffer occupancy. Exposed
as callbacks/hooks in the libraries and as Prometheus metrics plus health
endpoint in `mz-sink`. OpenTelemetry spans for connect, snapshot, batch
apply, and checkpoint, so a sink shows up in the same traces as the
application it feeds.

### Documentation integration

The per-language client pages in `doc/user/content/integrations/` currently
teach the naive loop. Once the SDK exists, each page leads with the SDK and
keeps the raw `DECLARE`/`FETCH` version as an appendix for driver-only
environments. The durable-subscriptions pattern doc becomes the conceptual
explanation behind the SDK's design, linking to it as the implementation.

### Materialize-side workstream (planned with the SDK, done properly)

The SDK must not paper over server gaps with client-side workarounds. Where
the correct behavior needs the database's help, the database change is part
of this program's plan, sequenced so the SDK's stable release builds on real
primitives. Each item below gets its own issue and, where non-trivial, its
own design doc. In sequence:

1. **Structured error codes** (dedicated SQLSTATEs) for the
   compaction-horizon error, dependency-dropped, and dataflow errors. Today
   the compaction-horizon failure surfaces as generic `DATA_EXCEPTION`
   shared with other errors, and its message text has already changed once
   (#34712), which silently broke the one existing client that dispatched
   on it. Small, high leverage, and a hard prerequisite for SDK GA: the
   typed error taxonomy must key on codes, never on message text.
2. **Docs**: cross-link the durable-subscriptions pattern from every
   client-library page. Can land immediately, independent of everything
   else.
3. **Subscribe output backpressure** in the coordinator (the existing
   unbounded-channel TODO in `active_compute_sink.rs`). Protects
   `environmentd` from slow consumers regardless of which client they use.
   The SDK's prompt-drain design reduces exposure but only the server can
   bound it.
4. **Progress cadence control**, so consumers can trade update granularity
   for faster checkpoint advancement and cheaper idle streams.
5. **Non-poisoning subscribe errors** (database-issues#5182), so a transient
   dataflow error does not permanently wedge a subscription.
6. **WebSocket `SUBSCRIBE` stabilization**, the gate for the browser SDK.
7. **Server-assisted subscription ownership** (named subscriptions with
   single-writer enforcement), contingent on evidence from HA deployments
   that the client-side lease is insufficient.

Items 1 and 2 are cheap and land before or with SDK GA. Items 3 through 5
improve every subscribe consumer and proceed on their own track with the SDK
as the motivating consumer. Items 6 and 7 are demand-gated.

## Minimal Viable Prototype

Prototype = **Python Layer 1 + Layer 2 + the Redis sink, chaos-tested against
the emulator**, living in the SDK monorepo from day one:

1. Layer 1 client on psycopg with batching, envelopes, typed errors,
   reconnect-and-resume.
2. Checkpoints in Redis, `transactional` mode only.
3. The Redis sink at functional parity with mz-redis-sync plus the known
   fixes: correct resume statement, `key_violation` handling, orphan sweep on
   resync, namespaced checkpoint key, NULL handling.
4. The kill-at-random-points chaos test asserting convergence against
   `SELECT ... AS OF`.

This validates the three riskiest claims early: that the batch/token model is
pleasant to use, that exactly-once state application holds under crash
testing, and that the emulator is a sufficient CI target. The Rust reference
implementation starts from the same spec as soon as the prototype stabilizes
the batch and token model, and the first conformance vectors are extracted
from the prototype's test suite. The Novu integration is then rebuilt on
Layer 2's `at_least_once` mode as the second validation, exercising
idempotency keys and retraction policy with a real side-effecting
destination.

## Delivery plan

Phased, each phase with an exit criterion, and no phase starts before the
previous one's criterion passes.

**Phase 0, foundations.** The SDK monorepo skeleton (`spec/`,
`conformance/`, `python/`, `rust/`, `examples/`), CI wiring, and the
emulator test harness. Spec v0 drafted from this design. In parallel, the
two cheap Materialize-side items land: the structured error code change and
the docs cross-links.

**Phase 1, the Python MVP.** As described under Minimal Viable Prototype,
Layers 1 and 2 plus the Redis sink. Exit: the chaos suite passes (the
destination equals `SELECT ... AS OF <frontier>` under arbitrary kill
schedules), the fencing test passes, and every typed error is reproducible
in integration tests.

**Phase 2, the spec becomes real.** Conformance vectors extracted from
Phase 1's tests, then the Rust reference implementation built against spec
and vectors. Exit: both implementations pass identical vectors and emulator
scenarios, and spec v1 is published. The Novu-style at-least-once rebuild
validates the event-sink surface in this phase.

**Phase 3, breadth.** TypeScript, the generic webhook sink (pending open
question 3), the `mz-sink` example runner, and the docs-site integration
that replaces the naive per-language loops.

**GA gate.** A stable release requires: Phase 2 complete, the
structured-error-code change released in Materialize (with the
version-gated fallback for older servers), and the public docs rewritten on
the SDK. HA failover (the lease design, open question 6) ships in the first
post-GA minor release.

## Future work

Deliberately excluded from v1, recorded so reviewers can see the growth path:

- **Fan-out helper.** One subscription demultiplexed to many in-process
  consumers by key (the websocket-server pattern, thousands of clients fed
  from one `SUBSCRIBE`). Layer 1's batch model supports it, a first-class
  helper makes it a five-line feature.
- **Per-key coalescing and debounce.** Event sinks often want "at most one
  webhook per key per interval" to absorb flapping. A Layer 2 option with a
  max-delay bound, at the cost of intermediate updates, which the diff model
  makes safe to drop.
- **Browser SDK** on the WebSocket endpoint once it stabilizes, implementing
  the same spec.
- **Additional languages** (Go first, given existing docs coverage) and
  additional built-in sinks chosen by demand, with adoption data feeding the
  case for native server-side sinks for the top destinations.
- **Serverless guidance.** Long-lived subscriptions do not fit
  function-per-request platforms. The pattern doc for bridging (a small
  always-on consumer feeding a queue) is docs work once the SDK exists.

## Alternatives

**A shared Rust core with native bindings (napi-rs, PyO3).** Single
implementation of the state machine, mechanical bindings. Rejected because
the state machine is the small part. The core would own connection
management, TLS, and auth in three runtimes, integrate with two foreign
async runtimes, and complicate packaging and debugging for every user. The
per-language ecosystem drivers are more battle-tested than anything we would
ship. A spec plus conformance vectors pins down cross-language behavior at a
fraction of the cost, which is how the database-driver ecosystem itself
works.

**Native server-side sinks (`CREATE SINK ... INTO REDIS`).** The
strongest alternative. It is the best eventual UX for the specific,
high-volume destinations, and this SDK does not preclude it. Rejected as the
*first* move because: each destination becomes a server feature with a
release train, storage/compute team ownership, and years of long-tail
destination requests (the Kafka Connect catalog is hundreds of connectors
deep). The SDK serves the long tail by construction, ships without touching
`environmentd`, and its adoption data tells us which destinations deserve
native sinks. The consistency model (frontier-atomic commits) is the same
one a native sink would implement, so the concepts transfer.

**Sink to Kafka, use the Kafka Connect ecosystem.** Works today for users
who run Kafka. Rejected as the answer because it taxes every user with a
Kafka deployment plus Connect operational burden to reach a cache, adds a
hop of latency, and loses the direct mapping between destination state and a
Materialize timestamp unless the connector is consistency-aware. It remains
the right answer for organizations already deep in Connect, and the docs
should keep saying so.

**Docs only, no code.** The durable-subscriptions pattern doc already
describes the protocol. The evidence section above shows what documentation
alone achieves, three implementations, three different correctness bugs, by
authors closer to Materialize than any customer will be.

**Harden mz-redis-sync as a one-off product.** Fixes one destination in one
language, leaves every other consumer where they are, and keeps the protocol
logic welded to Redis specifics. The layered SDK subsumes it, and `mz-sink`
delivers the same operational artifact.

## Decisions taken so far

Settled during initial review, recorded so the remaining questions are
crisp:

- **Official from day one.** This ships under `MaterializeInc` with a
  stability commitment, not as a Labs experiment with a graduation path.
- **All three layers in scope.** The SDK core is the fully supported main
  focus. Built-in sinks (Redis first) are supported reference
  implementations of the framework.
- **Language order: Rust and Python first, TypeScript next, Go later.**
- **HA is planned, not deferred.** Fencing in v1, lease-based active-passive
  failover shortly after. Production use is the assumption.
- **No shortcut workarounds for server gaps.** Where correct behavior needs
  the database's help, the server change is planned as part of this program
  (see the Materialize-side workstream), sequenced ahead of the SDK
  behavior that depends on it.
- **The standalone runner is a reference example**, not a core deliverable.

## Open questions

1. **Final package naming.** `subscribe` as the noun
   (`@materializeinc/subscribe`, `materialize-subscribe`, `mz-subscribe`)
   versus a broader name that leaves room for the SDK to grow into general
   client duties later. Day-one stability makes renaming expensive, so this
   needs deciding before the first publish.
2. **Guarantee vocabulary.** Are we comfortable publicly branding
   `transactional` mode as "exactly-once state application"? It is accurate
   for destinations where checkpoint and data commit atomically, but the
   term invites misreading as exactly-once side effects. Candidate framing:
   "consistent sinks" versus "delivery sinks".
3. **v1 sink surface.** Redis plus the generic webhook sink, or Redis only?
   Webhook is cheap on Layer 2 and exercises the second destination family
   early, which argues for including it.
4. **Checkpoint store default for event sinks.** Destination-embedded
   checkpoints are the opinionated default for keyed-state sinks, but no
   destination-embedded store exists for a webhook. Materialize-table store
   as the event-sink default?
5. **Conformance gate.** Block releases of any language on the full emulator
   chaos suite, or vectors only for patch releases?
6. **HA lease parameters.** Lease duration, heartbeat cadence, and whether
   the lease lives only in the checkpoint store or also surfaces in
   `mz_internal.mz_subscriptions` for observability. Needs a short design of
   its own before the failover release.
