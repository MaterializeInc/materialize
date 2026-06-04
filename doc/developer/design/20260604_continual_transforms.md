# Continual Transforms: Recorded, Non-Deterministic, At-Least-Once Transformations

- Associated:
  - #4527 (consume a collection's change stream as a relation) — read side, addressed by `CHANGES`
  - #36869 (`CHANGES` table function) — the trigger/read primitive this builds on
  - database-issues#9694 (Delete continual tasks) — the removed predecessor
  - PR #35967 (Remove continual tasks feature)
  - PR #35347 (standing queries) — adjacent prior art
  - Prior design: `20260210_incremental_occ_read_then_write.md` (the OCC commit substrate)
  - Internal: "Continual Tasks via Diffs" and "Continual Tasks Decision Log" (WG Continual Tasks, Notion)
  - Prototype: `BEGIN CONTINUAL TRANSACTION` (Aljoscha, Feb 2026)

## The Problem

Materialize maintains views as **definite, deterministic functions** of their
inputs: a view's contents at logical time `T` are fully determined by its
inputs at `T`, and are recomputed identically on restart or on a fresh replica.
This is the foundation of our correctness story, and it is the right default.

But a class of high-value workloads does not fit that model. They require
**recording a decision made at processing time and never recomputing it**:

- **Stream-table / enrichment joins.** Join a large event stream against a
  small dimension table using the dimension's value *at the moment the event
  arrived*, and never backfill when the dimension later changes. A normal join
  re-derives output when the dimension changes; there is no way to express
  "freeze the looked-up value."
- **Finalization.** "Write down answers while deleting the upstream state" —
  stop paying to maintain data the user knows will not change. Today customers
  emulate this with `REFRESH EVERY` (General Mills), external Kafka loopbacks
  (Notion), or round-tripping through Postgres (expensive at scale). Chainalysis
  and others have asked for a first-class pattern.
- **Upsert in compute.** Turn an append-only or custom-CDC input into an
  upsert collection without unbounded rehydration growth — especially when a
  temporal filter means the *active* keyspace is far smaller than all keys
  ever seen. Wanted for custom CDC formats (custom INSERT/UPDATE/DELETE over
  Kafka reassembled with `top k`) and partial CDC (Salesforce sends only
  changed columns). This is the single most frequently requested item ("how do
  I write an UPSERT in Materialize?").
- **Non-deterministic / external enrichment (UDFs).** Geocode, fraud-score,
  currency-convert, classify-with-an-LLM, or otherwise call a function whose
  result is non-deterministic and/or side-effecting, exactly once per row, and
  persist the answer so a restart reuses it rather than re-calling.
- **Idempotency / dedup within a window, metrics downsampling, tumbling/session
  window finalization, audit logs (internal sinks), webhook demultiplexing,
  stateless source transforms** too large to maintain as an MV.

The common shape: a transformation that is **triggered by input changes**,
whose output **depends on when it ran** (so it is not a deterministic function
of inputs at a logical time), and whose result must be **durably recorded**.
Recomputation-from-scratch is not just unnecessary here — for the
non-deterministic cases it would produce a *different* answer, so it must be
avoided.

We shipped a predecessor for exactly this — **Continual Tasks (CTs)** — in
`v0.127.0`, flag-gated it off in `v26.21.0`, and removed it in `v26.23.0`
(PR #35967). The removal rationale (database-issues#9694) was explicit:
*"Implementation isn't done, and there is no consensus whether it's the right
design or not,"* alongside a general push to burn down codebase complexity.
**It was not removed because the model is unsound or impossible.** The use cases
remain, the demand is real, and — critically — two new primitives now exist that
did not when CTs were first designed: the `CHANGES` table function (#36869) and
the OCC read-then-write commit substrate (`20260210_incremental_occ_read_then_write.md`).
This document proposes reviving the capability on top of those primitives, in a
layering designed specifically to avoid the lack-of-consensus that killed CTs.

## Success Criteria

A solution is successful if:

- A user can express a **stream-table/enrichment join** that freezes the
  dimension value as of processing time, append-only, without indexing the
  stream.
- A user can express **finalization** — record a result and stop maintaining
  the inputs that produced it — without leaving Materialize (no Kafka loopback,
  no external Postgres).
- A user can express **upsert in compute** with bounded state, recovering exact
  semantics, without unbounded rehydration cost.
- A transformation body may invoke **non-deterministic / side-effecting
  functions (UDFs)**, with each invocation's result **recorded once** and reused
  across restarts and replicas rather than recomputed.
- The system provides a clear, documented **correctness ladder**: exactly-once
  into Materialize-owned persist where achievable; at-least-once with an
  idempotent recovery path otherwise.
- The feature does **not** reintroduce the open-ended complexity that blocked
  consensus on CTs: the high-demand cases must be expressible in a layer that is
  free of the hardest semantic questions (self-reference / read-your-own-writes
  / input reclocking).

## Out of Scope

- **Exactly-once delivery to external systems.** Effects that leave Materialize
  are at-least-once with idempotency keys; true 2PC with external systems is not
  a goal.
- **High write throughput under heavy contention.** Like the OCC read-then-write
  work, this serializes conflicting commits via retries; it is not a
  high-contention OLTP write path.
- **Event-time / partially-ordered time semantics.** We assume the existing
  totally-ordered logical-time model. (Event-time imports — e.g. via an Iceberg
  source — are a separate, complementary effort.)
- **General multi-output atomic tasks** beyond what the commit substrate
  naturally supports; the first increment targets a single output collection.
- **Replacing materialized views.** This is for transformations that must record
  a non-deterministic/processing-time decision; deterministic IVM stays in MVs.

## Solution Proposal

### Summary

Build a **write-side complement to `CHANGES`**. `CHANGES` already turns a
collection's changelog into a first-class, definite relation (rows carry
`mz_timestamp`/`mz_diff` as ordinary columns). The missing half is a primitive
that **consumes such a changelog, applies a (possibly non-deterministic) SQL
transformation, and durably records the result at the commit timestamp** —
computing in the data plane and committing in the control plane, reusing the OCC
timestamped-write machinery.

We expose this single engine at **two co-equal altitudes**:

1. **Declarative — a recorded derived collection.** An object whose definition
   is a query over `CHANGES(input)` (joined against arbitrary references),
   written append-only (or upsert) to an output collection it owns, and
   **never recomputed**. This is the safe, high-demand surface.
2. **Imperative — `BEGIN CONTINUAL TRANSACTION`.** Aljoscha's prototype: bundle
   arbitrary non-interactive read-then-write statements (`INSERT … SELECT`,
   `UPDATE`, `DELETE`) into a transaction that commits continually when there is
   work to do (`COMMIT EVERY '1s'`). The general, power-user surface over the
   same engine.

Both are layered over a correctness ladder (below) so that the most-demanded
cases land first without paying the hardest semantic costs.

### Why this layering — and why `CHANGES` as the input dissolves the CT pain

The original CT design (and its Decision Log) spent nearly all of its effort on
two problems, both of which trace to a single mechanism: **reclocking the task's
input shards from `T` to `T-1`** so a task could read-then-write at one logical
time while reading its own output.

1. **Input/reference inconsistency.** Inputs were reclocked (read at `T-1`),
   "references" were not (read at `T`). A transaction that committed to both
   halves of a stream-table join would see the input change but not the
   reference change. An unavoidable wart of the reclocking.
2. **UPSERT impossibility under `reads@T-1`.** Reading the output at `T-1` made
   a correct in-place upsert inexpressible, which forced the Decision Log's
   pivot to "controlled iteration" (round-robin statements, WMR-like
   read-your-own-writes) — a substantial added mechanism.

**`CHANGES` removes the reclocking entirely.** It exposes the changelog as a
relation in which the timestamp is *data* (`mz_timestamp`), not the task's read
frontier. So if the engine's input is `CHANGES(input)` rather than a reclocked
input shard:

- The body reads references at the genuine commit time `T` — no `T-1` skew
  between inputs and references.
- It consumes changelog rows that each carry their own `mz_timestamp`.
- It writes the output at `T` via the OCC timestamped commit.
- "Consumed through `T`" is recorded by the **output's own progress frontier**,
  not by a manual `DELETE FROM holding_pen`. (This also closes the atomicity gap
  the imperative prototype hit — there is no `DELETE`+`INSERT` pair to make
  atomic; the engine just commits the output at `T`.)

In short: `CHANGES` (read side, #36869) + the continual-transaction engine
(write side) + OCC timestamped commit (substrate) are three separable pieces.
CTs fused trigger+reclock+commit into one object and paid for the fusion; we now
keep them separate.

### The correctness ladder

| Tier | What it covers | Semantics | Mechanism |
|---|---|---|---|
| **1. Recorded append** | enrichment/stream-table join, internal sink/audit, stateless transform, webhook demux, non-det/UDF enrichment | **exactly-once into persist** | single timestamped output write; group-commit rejects duplicate `T` |
| **2. General read-then-write** | arbitrary bundled `INSERT/UPDATE/DELETE` | exactly-once into persist (per commit) | OCC loop, control-plane commit |
| **3. Eventual (self-referential)** | upsert, dedup-in-window, metrics rollup, finalization | **at-least-once / eventual**, exact answer recovered downstream | compacting output + an MV on top |
| **4. External effects** | reverse-ETL, outbox, webhooks out | **at-least-once + idempotency key** | persisted "delivered-through-`T`" frontier |

Non-determinism and UDFs are safe at every tier for the same reason: the body
runs at commit time `T` and its result is **persisted, never recomputed**. Each
invocation is frozen the instant it is written. Multi-replica execution is
handled as in the original design — replicas race to commit and losers discard
(compare-and-append) — so the logic need not be definite (`now()` is allowed).

### Tier 1: recorded append (the high-demand, low-risk core)

A declarative object, roughly:

```sql
CREATE CONTINUAL TRANSFORM enriched
  FROM CHANGES(orders)            -- the trigger: orders' changelog
  AS SELECT o.*, c.tier, geocode(o.addr) AS geo   -- references + UDFs ok
     FROM changes o JOIN customers c ON o.cust_id = c.id
  INTO append enriched_out;       -- output owned by the transform
```

(Syntax is a placeholder; see Open Questions.) Properties:

- **No self-reference.** The body reads `CHANGES(input)` and arbitrary
  references, and writes a *distinct* output. This structurally avoids
  read-your-own-writes, input reclocking, and every Decision-Log question.
- **Exactly-once into persist.** Each input change yields a single timestamped
  output write at `T`; the group-commit timestamp check rejects a duplicate
  produced by a racing replica.
- **Append-only inputs need no snapshot.** As the via-Diffs design noted, since
  we consume the changelog (listen), not a snapshot, there is no rehydration
  spike — this is what makes it cheaper than an equivalent MV for large,
  high-churn inputs.
- Covers: stream-table join, internal sink/audit (replacing Notion's Kafka
  loopback), stateless source transforms, webhook demux (multi-output as a
  follow-up), and all non-deterministic/UDF enrichment.

### Tier 2: `BEGIN CONTINUAL TRANSACTION` (the general surface)

The same engine, exposed imperatively (Aljoscha's prototype), but fed from
`CHANGES` rather than a hand-rolled holding pen:

```sql
BEGIN CONTINUAL TRANSACTION;
  INSERT INTO output
    SELECT hp.customer_id, dim.a_thing
    FROM CHANGES(events) hp JOIN dim ON hp.customer_id = dim.customer_id;
COMMIT EVERY '1s';
```

This generalizes CTs (which were "fixed function": one `INSERT` + optional
`DELETE`) to arbitrary bundled read-then-write statements. The data-plane /
control-plane split is exactly the OCC design's. Semantics, as in the prototype:
"as if you ran these statements in a loop as fast as you can." The
`COMMIT EVERY` cadence and the "commit only when there is work" trigger are the
knobs. Self-referential bodies are permitted but carry documented footguns (the
`INSERT INTO t SELECT * FROM t` family); read-your-own-writes within one commit
is the ordinary multi-statement-transaction problem, not a novel reclocking one.

### Tier 3: self-referential / eventual (incl. UPSERT)

The hardest, most-demanded case — upsert — is tractable **under the relaxed
bar** because of an observation from the via-Diffs design: upsert **cannot be
done in place** (a retraction must be computed before it is first written), so
it inherently needs **two shards** — a compacting output the transform maintains
*eventually*, plus an ordinary **MV on top that recovers exact semantics**
("eventual upsert"):

```sql
-- transform keeps the working set bounded (eventually correct)
CREATE CONTINUAL TRANSFORM upsert_ct FROM CHANGES(append_only) AS (
    DELETE FROM compacted WHERE key IN (SELECT key FROM changes);
    INSERT INTO compacted SELECT key, max(value) FROM changes GROUP BY key;
) INTO compacted;

-- MV recovers the exact answer
CREATE MATERIALIZED VIEW upserted AS
  SELECT key, max(value) FROM compacted GROUP BY key;
```

Because the MV recovers exactness, **the transform itself only needs to be
eventually / at-least-once correct** — precisely the bar we accept here. The
self-reference exists to bound state, not for correctness. The same shape
covers dedup-in-window, metrics rollup, and tumbling/session-window
finalization (capture the rows an MV *deletes* into a closed-window shard).

### Tier 4: external effects

For reverse-ETL / outbox, the effect target is external, so the natural home is
the **sink** path fed by `CHANGES`. At-least-once is the ceiling;
effectively-once via an idempotency key plus a durable "delivered-through-`T`"
marker — which is just a Tier-1 recorded collection.

### Dependencies / things that may change

- Builds directly on `CHANGES` (#36869); benefits from its planned follow-ups
  (one-off mode with session-length holds; arbitrary-expression `CHANGES`).
- Builds on the OCC read-then-write commit substrate; needs the
  "commit a bundle of statements atomically at `T`" capability the prototype
  flagged as not-yet-available.
- Needs an owned-output / progress-frontier concept so "consumed-through-`T`" is
  recorded by the output rather than a manual drain.
- Multi-replica race-to-commit (compare-and-append, losers discard) — already
  contemplated by the original CT design.

## Minimal Viable Prototype

Two prototypes already substantially de-risk this:

1. **`CHANGES`** (#36869) — the read/trigger side, implemented and tested
   (one-off and maintained sliding-window MV modes), including restart-exact
   reproduction.
2. **`BEGIN CONTINUAL TRANSACTION`** (Aljoscha, Feb 2026) — a working demo of
   the imperative surface and the data-plane/control-plane commit split,
   including a live stream-table join via a holding pen.

The MVP for *this* design is to connect them: a Tier-1 recorded-append
transform whose input is `CHANGES(input)` and whose output is a transform-owned
collection committed via the OCC timestamped write, demonstrated end-to-end on
the stream-table-join and UDF-enrichment use cases, plus the Tier-3 eventual
upsert (transform + MV-on-top) to validate the relaxed-correctness story.

## Alternatives

- **Revive Continual Tasks as-is.** Rejected: it was removed for lack of
  consensus and incompleteness, and its core mechanism (input reclocking to
  `T-1`) is the source of the input/reference inconsistency and the UPSERT
  impossibility. Reintroducing it reintroduces those problems and the complexity
  that motivated removal.
- **Imperative-only (`BEGIN CONTINUAL TRANSACTION` alone).** Rejected as the
  *sole* surface: it is session-scoped (not a durable object), and its
  generality exposes self-reference footguns to every user. We keep it as the
  power-user altitude but not the only one.
- **Declarative-only (recorded collection alone).** Rejected as the sole
  surface: it cannot express the general bundled read-then-write cases (upsert,
  rollup) without awkward contortions. We keep it as the safe default altitude.
- **Solve finalization with Iceberg source + sink (+ append-only sink mode).**
  Genuinely complementary and worth doing (temporal filter on the *input*,
  teeing hot data to IVM and cold data to Iceberg, per Frank's note), but it
  addresses *archival/offload*, not in-Materialize recorded transforms or
  non-deterministic enrichment. Not a substitute.
- **External Postgres / Kafka loopback (status quo workarounds).** Rejected:
  expensive at scale (Postgres) and operationally heavy (Kafka); the entire
  point is to keep this inside Materialize.
- **Distributed locking instead of OCC commit.** Rejected for the same reasons
  as in the OCC read-then-write design (latency, brittleness, scalability).

## Open questions

- **Syntax / object model.** `CREATE CONTINUAL TRANSFORM`? Reuse/extend
  `CONTINUAL TASK` naming? How do the declarative object and
  `BEGIN CONTINUAL TRANSACTION` share grammar and semantics so they are visibly
  two altitudes of one engine? How is the trigger cadence expressed
  declaratively (vs `COMMIT EVERY`)?
- **Commit-timestamp policy.** "Every timestamp" vs "timestamps where input is
  non-empty" (the Decision Log's open question) — the `INSERT … VALUES`
  footgun, exposing millisecond granularity, and whether time-driven (not
  input-driven) bodies (`DELETE … WHERE ts < mz_now() - '1d'`) are allowed.
- **Read-your-own-writes within a commit.** How much of the Decision Log's
  "controlled iteration" is needed for Tier 2/3, given Tier 1 avoids it
  entirely and Tier 3 upsert is recovered by an MV? Can we ship Tiers 1–2
  before resolving it?
- **Owned outputs & multi-output.** Output ownership rules; multi-output
  transforms (needed for webhook demux); interaction with `RETAIN HISTORY`.
- **Multi-replica & at-least-once observability.** Surfacing
  delivered/committed-through frontiers; behavior under replica churn.
- **Relationship to standing queries (PR #35347)** — overlap or composition?
- **Non-deterministic function story.** Do we need an explicit `VOLATILE`/
  non-deterministic function marker, and engine-side memoization, or is
  "recorded once in the output" sufficient?
