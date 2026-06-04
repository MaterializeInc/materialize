# Continual Transforms: Recorded, Non-Deterministic, At-Least-Once Transformations

- Associated:
  - #4527 (consume a collection's change stream as a relation) — read side, addressed by `CHANGES`
  - #36869 (`CHANGES` table function) — the read-side primitive this builds on
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
avoided. And because we give up recomputation, **bounding the growth** of the
recorded output (and the engine's internal state) becomes a first-class problem,
not an afterthought.

We shipped a predecessor for exactly this — **Continual Tasks (CTs)** — in
`v0.127.0`, flag-gated it off in `v26.21.0`, and removed it in `v26.23.0`
(PR #35967). The removal rationale (database-issues#9694) was explicit:
*"Implementation isn't done, and there is no consensus whether it's the right
design or not,"* alongside a general push to burn down codebase complexity.
**It was not removed because the model is unsound or impossible.** The use cases
remain, the demand is real, and — critically — two new primitives now exist that
did not when CTs were first designed: the `CHANGES` table function (#36869) and
the OCC read-then-write commit substrate (`20260210_incremental_occ_read_then_write.md`).
This document proposes reviving the capability by **distilling it to a minimal
set of orthogonal primitives** designed to avoid the lack-of-consensus that
killed CTs.

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
- **The output collection has a bounded size** under an explicit retention or
  keying contract, and **the engine's internal/working state and the underlying
  persist shards are physically reclaimed** (no unbounded growth from
  superseded rows whose retractions never consolidate).
- **A recorded row's value can be frozen at processing time while its existence
  stays tied to a referenced entity** — so deleting a user **physically erases**
  their recorded rows (compliance), even though the recorded values do not
  recompute when the source changes.
- The design **reduces to a small set of orthogonal primitives** that compose to
  cover every use case; richer behaviors (stream-table join, upsert, retention,
  compliance cascade, the correctness tiers, the surface syntaxes) are
  compositions, not bespoke features.
- The feature does **not** reintroduce the open-ended complexity that blocked
  consensus on CTs: the high-demand cases must be free of the hardest semantic
  questions (self-reference / read-your-own-writes / input reclocking).

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

The feature reduces to **three orthogonal primitives**, plus `CHANGES` (the
already-shipped read-side primitive, #36869) and ordinary SQL/dataflow. Every
use case — and every richer concept below (stream-table join, upsert, retention,
compliance cascade, the correctness tiers, the surface syntaxes) — is a
**composition** of these, not a bespoke feature:

- **P1 — RECORD**: a derived collection that is *definite by persistence*, never
  recomputed. Its contents are the bytes that were written; it survives restart
  and replicas as data, not as a re-evaluated function. (Every MV today is
  definite-by-*recomputation*; this is the new dual.)
- **P2 — COMMIT@T**: an input-triggered, timestamped write. It runs a body when
  the input advances and atomically commits the resulting diffs into a RECORD
  output at the commit time `T` (racing replicas resolved by compare-and-append).
  This binds a computation to *when it ran* and makes that binding the durable
  fact — the only source of processing-time dependence, and the only writer of
  P1. (Reuses the OCC timestamped-write substrate.)
- **P3 — FROZEN(expr)**: a read-side cut. Evaluate `expr` once at processing
  time and treat the result as constant thereafter; later changes to its inputs
  do not flow. This severs *value*-dependence on a source while leaving the
  row's relational *existence* live.

A fourth concept, **DELIVERED-THROUGH-`T`** (governing at-least-once external
effects), is itself P1+P2 applied to a frontier value (a recorded
"emitted-through-`T`" marker), so the irreducible core is the three above.

### The basis in one picture: stream/table duality + sample-and-hold

The three primitives are the missing operators of stream/table duality:

- `CHANGES` is table→stream (`differentiate`): turn a collection's diffs into
  data (`mz_timestamp`/`mz_diff` as columns).
- **RECORD (P1)** is stream→table (`integrate`) **made durable and
  authoritative**: the integral *is* the state, not a recomputable view.
- **FROZEN (P3)** is the `sample-and-hold` that other engines (Flink's
  temporal/lookup join, Kafka Streams' stream-table join) bury *inside* a join
  operator. Pulling it out as a first-class, per-value scalar is the key move: it
  lets one row freeze some columns while keeping others (and the row's existence)
  live — which a join-level freeze cannot express, and which makes us strictly
  more expressive than Flink/Kafka (the currency-conversion-with-compliance case)
  with *fewer* operators.

**COMMIT@T (P2)** binds a result to the instant it was produced.

### The determinism boundary (why composition stays sound)

Materialize's soundness rests on definiteness. The basis confines
non-definiteness to exactly two primitives and then *absorbs* it:

- **P2 chooses the instant** (commit `T`, replica race, `now()`); **P3 freezes
  what was true at that instant.** Together they are the entire non-determinism
  boundary — `JOIN`/`reduce`/temporal-filter introduce none.
- **P1 absorbs it**: the moment a non-definite result is written it becomes
  definite-by-persistence. Downstream MVs read a *recorded, definite* collection
  and never see the non-determinism, so ordinary IVM composes on top without
  taint.
- The one place this weakens is the external boundary
  (DELIVERED-THROUGH-`T`): the effect has already left Materialize, so
  absorption is at-least-once, not exactly-once.

This yields a single enforceable rule that keeps non-determinism in the box:
**`FROZEN(...)` and processing-time writes are legal only inside a RECORD
output.** A `FROZEN()` in a plain MV taints it and is rejected at plan time.

### Everything else is composition

`Δ(X)` = `CHANGES(X)`. Body = ordinary SQL. (Reference freshness defaults are
below.)

| Concept / use case | Composition |
|---|---|
| **Stream-table / enrichment join** | RECORD ← COMMIT@T over `Δ(stream)`, `FROZEN` on the looked-up dimension values. `STREAM JOIN` is *sugar* for exactly this. |
| **Currency-conversion + compliance** | as above with `FROZEN(convert(amt, fx.rate))`, `FROZEN(u.name)`, but **no** `FROZEN` on the `users` join ⇒ existence stays live ⇒ a `users` delete cascades a retraction; output indexed by `user_id`. |
| **Upsert in compute** | RECORD ← COMMIT@T, body `SELECT DISTINCT ON (key) … ORDER BY seq DESC` over `Δ(input)`. No new primitive — `top-1` is plain SQL; the "transform-ness" is P1+P2 (listen-only input, output = rehydrated reduce state). |
| **Dedup-in-window** | upsert pattern + temporal filter in the live structure. |
| **Retention window** | RECORD + temporal filter (`mz_now() < ts + W`); no `DELETE`, self-finalizing. |
| **Metrics rollup / finalization** | RECORD + reducing body; window close = react to `mz_diff = -1` in `Δ(input)`. |
| **Internal sink/audit, stateless transform, webhook demux** | RECORD ← COMMIT@T, body is a projection/route. |
| **Non-det / UDF enrichment, recorded once** | RECORD + COMMIT@T + `FROZEN(udf(...))` — P2 ensures one write per `T`, P3 ensures the value never re-derives. |
| **Reverse-ETL / outbox** | DELIVERED-THROUGH-`T` (= RECORD frontier) gating emission. |

No use case needs a primitive outside `{P1, P2, P3}` (+ the derived
DELIVERED-THROUGH-`T`). Reclassifying the concepts this doc went through:
**CHANGES** is a boxed dependency; **RECORD** and **COMMIT@T** are primitive;
**"frozen value vs. live existence"** is the *single* primitive `FROZEN` plus
ordinary joins (live existence = the *absence* of `FROZEN` on the join);
**eviction rules**, **`STREAM JOIN`**, the **surface altitudes**, and the
**correctness ladder** are all derived (below).

### Reference freshness defaults

Because `FROZEN` is a per-value scalar, a body needs a default for references it
does not mark. Inside a stream context (`FROM CHANGES(...)`) the default is
**frozen**: a referenced dimension is looked up once at `T` and not maintained —
the cheap stream-table-join behavior that never indexes the stream. **Live**
behavior (existence cascade, or a value that tracks its source) is the explicit
opt-in, and it is what costs an index on the output's liveness key. This keeps
the common case cheap and makes the expensive case visible.

### Keeping P1 and P2 orthogonal: why this avoids the CT pain

The removed CTs fused trigger + reclock + commit into one object. Their Decision
Log spent nearly all its effort on problems that came *from the fusion* —
specifically from **reclocking the input from `T` to `T-1`** so a task could
read-then-write while reading its own output:

1. **Input/reference inconsistency** — inputs read at `T-1`, references at `T`.
2. **UPSERT impossibility under `reads@T-1`**, which forced the "controlled
   iteration" mechanism.

Keeping the primitives orthogonal dissolves both. `CHANGES` feeds the body the
changelog as data (the timestamp is a *column*, not a read frontier), so the body
reads references at the genuine `T` (no skew), COMMIT@T writes the output at `T`,
and "consumed through `T`" is the output's own progress frontier — no holding-pen
`DELETE`+`INSERT` to make atomic. The reclocking simply does not exist.

### Bounding output growth is a P1 invariant

The central hard problem — keeping a never-recomputed output and its working
state bounded — is a property **P1 must guarantee**, not a separate feature.
*Record-don't-recompute* wants to keep data; *bounded resources* wants to forget
it; they reconcile only if forgetting is **explicit, part of the output's
contract, and (because the result is non-deterministic) irreversible**. That is
exactly "finalization": keep the answers, drop the working state.

Eviction is "a row's lifetime condition ceased to hold", composed from the
primitives + boxed SQL:

- **Age** — temporal filter; clock-driven, self-finalizing, no index.
- **Referential / ownership** (compliance) — *absence* of `FROZEN` on a join ⇒
  IVM retracts the row when the parent is deleted; a **physical** retraction, not
  a read-time filter; costs an index on the output's liveness key.
- **Supersession** — a `top-k` body; the reduce emits the exact
  retract-old/insert-new diffs (O(live keys)).
- **Arbitrary predicate** — escape hatch: explicit `DELETE` in the imperative
  surface, documented as a read-then-write.

Two physical invariants P1 must hold for space to actually be reclaimed:

1. **Retractions must consolidate with their inserts** — a `-1` cancels a `+1`
   only if it is the *exact same row*. The `top-k` reduce and the IVM cascade
   emit exact retractions by construction; hand-written `DELETE`s may not (e.g.
   retracting `max(value)` without the stored value), which is the "careful
   re-integration" hazard.
2. **Bounded growth ⇔ bounded `since` lag ⇔ bounded retention window** — space is
   reclaimed only once compaction advances `since` past a retraction, so
   `RETAIN HISTORY` trades off directly against bounded growth. The engine must
   own the read policy / compaction on its P1 output.

The reason this is a *transform* and not a plain `top-k` MV (which would also
bound the output) is the **input** side: P1+P2 consume the input as a listen-only
changelog and persist the reduce's state *as* the output (rehydrated on restart),
so the upstream input can be forgotten without a rehydration snapshot. The only
"self-reference" is standard operator-state persistence.

### Surfaces (derived ergonomics, not capability)

The same three primitives are exposed at altitudes that trade safety for
generality; none adds capability.

1. **Declarative recorded collection** — the safe default. No self-reference;
   structurally free of every Decision-Log question.

    ```sql
    CREATE CONTINUAL TRANSFORM enriched
      FROM CHANGES(orders)
      AS SELECT o.*, c.tier, FROZEN(geocode(o.addr)) AS geo
         FROM changes o JOIN customers c ON o.cust_id = c.id
      INTO append enriched_out;
    ```

2. **`BEGIN CONTINUAL TRANSACTION` … `COMMIT EVERY`** — Aljoscha's prototype:
   arbitrary bundled read-then-write statements over the same engine, fed from
   `CHANGES` rather than a holding pen. Power-user surface; self-referential
   bodies allowed with documented footguns.

    ```sql
    BEGIN CONTINUAL TRANSACTION;
      INSERT INTO output
        SELECT e.id, FROZEN(dim.v) FROM CHANGES(events) e JOIN dim ON …;
    COMMIT EVERY '1s';
    ```

3. **`STREAM JOIN`** (candidate) — sugar for `JOIN over CHANGES` with `FROZEN`
   on the looked-up side; worth a keyword only to make the asymmetry/cost
   explicit and to *imply* a RECORD output (it is non-definite and would taint an
   MV). See Open Questions.

### The correctness ladder is a classification of compositions

The tiers are not primitives; they fall out of *which* primitives a body uses:

| Tier | Composition | Semantics |
|---|---|---|
| **1. Recorded append** | P1 + P2 (+ `FROZEN`) | **exactly-once into persist** |
| **2. General read-then-write** | P2 with a self-referential body | exactly-once into persist per commit |
| **3. Eventual / stateful** | P1 + P2 + `reduce`/`top-k` | **at-least-once / eventual**; exact once caught up |
| **4. External effects** | DELIVERED-THROUGH-`T` (= P1+P2) | **at-least-once + idempotency key** |

### Dependencies / things that may change

- Builds on `CHANGES` (#36869) and the OCC timestamped-write substrate
  (`20260210_incremental_occ_read_then_write.md`); needs the "commit a bundle of
  diffs atomically at `T`" capability the prototype flagged as missing.
- Needs an owned-output / progress-frontier concept (P1), and the engine to own
  the read policy / compaction on it.
- Time-based retention reuses the temporal-filter / lagged-read-hold mechanism
  from the CHANGES maintained-MV mode.
- Multi-replica race-to-commit (compare-and-append, losers discard).

## Minimal Viable Prototype

Two prototypes already substantially de-risk this:

1. **`CHANGES`** (#36869) — the read side, implemented and tested (one-off and
   maintained sliding-window MV modes), including restart-exact reproduction.
2. **`BEGIN CONTINUAL TRANSACTION`** (Aljoscha, Feb 2026) — a working demo of
   the imperative surface and the data-plane/control-plane commit split,
   including a live stream-table join via a holding pen.

The MVP for *this* design is to connect them as the three primitives: a RECORD
output written by COMMIT@T over `CHANGES(input)`, demonstrated end-to-end on
(a) the stream-table join / UDF enrichment with `FROZEN` (Tier 1), and (b) the
`top-1` eventual upsert with input forgetting (Tier 3). The
currency-conversion-with-compliance case (frozen value + live-existence cascade)
is the stretch goal that validates per-value `FROZEN` and the lint rule.

## Alternatives

- **Bespoke features per use case** (a separate stream-join feature, an upsert
  feature, a retention feature). Rejected: this is what bloated and stalled CTs.
  Distilling to `{P1, P2, P3}` makes the surface small and the features fall out
  as compositions.
- **Revive Continual Tasks as-is.** Rejected: removed for lack of consensus and
  incompleteness, and its core mechanism (input reclocking to `T-1`) is the
  source of the input/reference inconsistency and the UPSERT impossibility —
  exactly what keeping P1/P2 orthogonal avoids.
- **Imperative-only (`BEGIN CONTINUAL TRANSACTION` alone).** Rejected as the
  *sole* surface: session-scoped (not a durable object), and its generality
  exposes self-reference footguns to every user. Kept as the power-user altitude.
- **Declarative-only (recorded collection alone).** Rejected as the sole
  surface: cannot express general bundled read-then-write (upsert, rollup)
  without contortions. Kept as the safe default altitude.
- **`FROZEN` as a join-level modifier instead of a scalar.** Rejected: it cannot
  express "this column frozen, that column live in the same row" — the
  currency-conversion-with-compliance case. The scalar is the orthogonal basis
  vector; a `STREAM JOIN` keyword can still expand to it.
- **Solve finalization with Iceberg source + sink (+ append-only sink mode).**
  Genuinely complementary (temporal filter on the *input*, teeing hot data to IVM
  and cold data to Iceberg, per Frank's note), but it addresses *archival/offload*,
  not in-Materialize recorded transforms or non-deterministic enrichment.
- **External Postgres / Kafka loopback (status quo).** Rejected: expensive at
  scale and operationally heavy; the point is to keep this inside Materialize.
- **Distributed locking instead of OCC commit.** Rejected for the same reasons as
  in the OCC read-then-write design (latency, brittleness, scalability).

## Open questions

- **P1 object model & syntax.** `CREATE CONTINUAL TRANSFORM`? Reuse/extend
  `CONTINUAL TASK` naming? How is the trigger cadence expressed declaratively
  (vs `COMMIT EVERY`)? How do the declarative object and `BEGIN CONTINUAL
  TRANSACTION` visibly share the same primitives?
- **P2 commit-timestamp policy.** "Every timestamp" vs "timestamps where the
  input is non-empty" (the Decision Log's open question) — the `INSERT … VALUES`
  footgun, exposing millisecond granularity, and whether time-driven (not
  input-driven) bodies are allowed.
- **P3 `FROZEN` surface & cascade atomicity.** Confirm scalar `FROZEN(expr)` with
  frozen-by-default in a stream context. Can the same dimension supply a frozen
  *value* and also anchor *lifetime*? How is the liveness-key index costed and
  made explicit? What is the cascade's atomicity story (a dimension delete and the
  resulting recorded-row retractions committed at one `T`)?
- **`STREAM JOIN` keyword — yes or no?** It is sugar (`JOIN over CHANGES` +
  `FROZEN`), but the asymmetry/cost want to be explicit, with prior art (Flink
  temporal/lookup join `FOR SYSTEM_TIME AS OF`, Kafka stream-table join). The
  classic stream-table join freezes *everything* (no cascade), which we extend
  with per-value `FROZEN` + live existence. Open: adopt the keyword as the
  fully-frozen floor, and is it body-only or a standalone operator that *implies*
  a RECORD output?
- **`DELETE` vs. automatic phase-out.** Proposal: age → temporal filter;
  ownership → cascade; supersession → `top-k`; arbitrary → explicit `DELETE`
  (imperative only). Right split? Should the declarative surface accept a
  restricted predicate-eviction policy? Should time-predicate `DELETE` be linted?
- **Retention / keying syntax & compaction SLO.** How is the contract expressed
  (`RETAIN '30 days'`, count-based `KEEP LAST n PER key`)? How do we guarantee
  compaction keeps up so reclamation happens, and surface it when it falls behind?
- **Rehydrating reduce state from the output.** What must the output retain (and
  at what `since`) for a `top-k`/reducing body to resume *without* re-snapshotting
  the input, and how does that interact with a time-based retention on the same
  output?
- **Read-your-own-writes within a commit.** How much of "controlled iteration" is
  needed for Tier 2, given Tiers 1/3 avoid it? Can we ship Tiers 1 + 3 first?
- **Multi-output & observability.** Multi-output P1 (webhook demux); surfacing
  committed/delivered-through frontiers; behavior under replica churn.
- **Relationship to standing queries (PR #35347)** — overlap or composition?
- **Non-deterministic function story.** Do we need an explicit `VOLATILE` marker
  and engine-side memoization, or is "recorded once via P1+P3" sufficient?
