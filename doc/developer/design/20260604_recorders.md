# Recorders: Recorded, Non-Deterministic, At-Least-Once Transformations

- Associated:
  - #4527 (consume a collection's change stream as a relation) — read side, addressed by `CHANGES`
  - #36869 (`CHANGES` table function) — the `differentiate` primitive this builds on
  - database-issues#9694 (Delete continual tasks) — the removed predecessor
  - PR #35967 (Remove continual tasks feature)
  - PR #35347 (standing queries) — adjacent prior art
  - Prior design: `20260210_incremental_occ_read_then_write.md` (the OCC commit substrate)
  - Internal: "Continual Tasks via Diffs" and "Continual Tasks Decision Log" (WG Continual Tasks, Notion)
  - Prototype: `BEGIN CONTINUAL TRANSACTION` (Aljoscha, Feb 2026)

## The Problem

Materialize maintains views as **definite, deterministic functions** of their
inputs: a view's contents at logical time `T` are fully determined by its inputs
at `T`, and are recomputed identically on restart or on a fresh replica. This is
the foundation of our correctness story, and the right default.

But a class of high-value workloads does not fit that model. They require
**recording a decision made at processing time and never recomputing it**:

- **Stream-table / enrichment joins.** Join a large event stream against a small
  dimension table using the dimension's value *at the moment the event arrived*,
  and never backfill when the dimension later changes.
- **Finalization.** "Write down answers while deleting the upstream state" — stop
  paying to maintain data the user knows will not change. Emulated today with
  `REFRESH EVERY` (General Mills), Kafka loopbacks (Notion), or Postgres
  round-trips (expensive at scale). Chainalysis and others have asked for it.
- **Upsert in compute.** Turn an append-only or custom-CDC input into an upsert
  collection without unbounded rehydration growth — the single most frequently
  requested item ("how do I write an UPSERT in Materialize?").
- **Non-deterministic / external enrichment (UDFs).** Geocode, fraud-score,
  currency-convert, classify-with-an-LLM: call a function whose result is
  non-deterministic and/or side-effecting, once per row, and persist the answer.
- **Idempotency / dedup within a window, metrics downsampling, tumbling/session
  window finalization, audit logs, webhook demultiplexing, stateless source
  transforms** too large to maintain as an MV.

The common shape: a transformation **triggered by input changes**, whose output
**depends on when it ran** (not a deterministic function of inputs at a logical
time), and whose result must be **durably recorded** — recomputation would
produce a *different* answer, so it must be avoided. And because we give up
recomputation, **bounding the growth** of the recorded output is a first-class
problem, not an afterthought.

We shipped a predecessor — **Continual Tasks (CTs)** — in `v0.127.0`, flag-gated
it off in `v26.21.0`, and removed it in `v26.23.0` (PR #35967). The removal
rationale (database-issues#9694) was explicit: *"Implementation isn't done, and
there is no consensus whether it's the right design or not,"* plus a push to burn
down complexity. **It was not removed because the model is unsound.** The use
cases remain, and two new primitives now exist that did not when CTs were
designed: the `CHANGES` table function (#36869) and the OCC timestamped-write
substrate (`20260210_incremental_occ_read_then_write.md`). This document revives
the capability by **distilling it to a small calculus** (`differentiate` /
`integrate` / `record` / `freeze`) and a concrete surface (a **`RECORDER`**
object writing **`DELTA TABLE`s**).

## Success Criteria

- A user can express a **stream-table/enrichment join** that freezes the
  dimension value as of processing time, without indexing the stream.
- A user can express **finalization** — record a result and stop maintaining its
  inputs — without leaving Materialize.
- A user can express **upsert in compute** with bounded state, recovering exact
  semantics, without unbounded rehydration cost.
- A body may invoke **non-deterministic / side-effecting functions (UDFs)**, each
  result **recorded once** and reused across restarts/replicas, not recomputed.
- **The recorded output and the engine's internal state stay bounded** under an
  explicit retention contract, and the underlying persist shards are **physically
  reclaimed**.
- **A recorded row's value can be frozen at processing time while its existence
  stays tied to a referenced entity** — deleting a user **physically erases**
  their recorded rows (compliance), without the values recomputing.
- The design **reduces to a small set of orthogonal operations** that compose;
  richer behaviors (stream-table join, upsert, retention, cascade, the tiers, the
  surfaces) are compositions, not bespoke features.
- It does **not** reintroduce the complexity that blocked CTs: the high-demand
  cases stay free of self-reference / read-your-own-writes / input reclocking.

## Out of Scope

- **Exactly-once delivery to external systems.** External effects are
  at-least-once with idempotency keys; no 2PC.
- **High write throughput under heavy contention.** Conflicting commits serialize
  via OCC retries; not an OLTP write path.
- **Event-time / partially-ordered time.** We assume the existing totally-ordered
  logical-time model (event-time imports are a separate effort).
- **General multi-output atomic tasks** beyond what the commit substrate gives;
  the first increment targets few outputs.
- **Replacing materialized views.** Deterministic IVM stays in MVs; this is for
  recording non-deterministic/processing-time decisions.

## Solution Proposal

### Summary

The feature is a small calculus over two kinds of collection, plus a standing
object that applies it.

- A **TVC** (time-varying collection) is a normal collection — change is implicit.
- A **dTVC** (delta-TVC) carries its change *as data*: rows with `mz_timestamp`
  and `mz_diff` columns. Its value is the integral of those rows.

Four operations move between them:

| operation | signature | surface |
|---|---|---|
| **differentiate** | TVC → dTVC | `CHANGES(x AS OF …)` (shipped, #36869) |
| **integrate** | dTVC → TVC | `INTEGRATE r AS v` |
| **record** | dTVC → durable dTVC | `RECORD r INTO delta_table` |
| **bound** | keep a dTVC finite | temporal filter, or `DELETE r FROM delta_table` |

plus **freeze**, which is not a keyword but falls out of typing (below).

A **`RECORDER`** is the standing object that runs a body on each input change and
**commits its `RECORD`/`INTEGRATE`/`DELETE` actions atomically at the commit time
`T`** (reusing the OCC timestamped-write substrate). Everything else —
stream-table join, upsert, retention, the compliance cascade, the correctness
tiers — is a *composition* of these, not a separate feature.

### The conceptual basis: TVC ↔ dTVC duality

`differentiate` turns a *mutable* TVC into an **append-only log of immutable
change-records**: each `(row, mz_timestamp, mz_diff)` is permanent and
self-describing, so it is independent of system/processing time — *when* you read
it, or how the source mutates afterward, cannot change it. `mz_timestamp` /
`mz_diff` are meaningful **only on a dTVC**; `differentiate` introduces them,
`integrate` consumes them.

In stream/table-duality terms: **`CHANGES` is `differentiate`** (`D = 1 − z⁻¹`),
**`INTEGRATE`/`RECORD` are `integrate`** (`I = D⁻¹`) made *durable and
authoritative* — the integral *is* the state, not a recomputable view — and
**`freeze` is `sample-and-hold`** (a projection: idempotent, with `D(freeze x) =
0`), the operator other engines bury inside their join. All three are faces of
the single delay operator `z⁻¹`.

`record` and `integrate` are the two durable shapes: `record` persists a dTVC
*as deltas* (a `DELTA TABLE` — a log); `integrate` persists it *as state* (a TVC
— self-correcting, MV-like).

### The determinism boundary (why composition stays sound)

Non-definiteness enters at exactly two points — **the commit picks `T`** and **a
frozen value samples a non-tracked source at `T`** — and is then **absorbed**:
the moment a result is `RECORD`ed/`INTEGRATE`d it is definite-by-persistence, so
downstream MVs read a definite collection and never see the non-determinism.
`differentiate`, `integrate`, `JOIN`, `reduce`, and temporal filters introduce
none. The single enforceable rule: **freezing (and processing-time writes) are
legal only inside a `RECORDER`'s recorded output**; a frozen lookup in a plain MV
taints it and is rejected at plan time.

### The surface: `DELTA TABLE` and `RECORDER`

A **`DELTA TABLE`** is a table typed as a dTVC: it has implicit
`mz_timestamp mz_timestamp` and `mz_diff bigint` columns, and its rows are
*deltas* (its value is their integral). It is *not* append-only — it can be
pruned/compacted — which is why "delta" (what the rows are), not "journal" (an
append-only promise we don't keep), is the right name.

```sql
CREATE DELTA TABLE enriched (a, b, c, val);   -- implicit mz_timestamp, mz_diff
```

A **`RECORDER`** binds named relations and a set of actions:

```sql
CREATE RECORDER rec1 WITH
  rel1 AS (
    SELECT e.*, d.val
    FROM CHANGES(events AS OF AT LEAST mz_now() - INTERVAL '1 hour') e
    JOIN dim d ON e.fk = d.key          -- dim is a TVC reference ⇒ frozen lookup
  ),
  rel2 AS (
    SELECT DISTINCT ON (a, b, c) *      -- first value seen per key
    FROM enriched WHERE mz_diff > 0 ORDER BY a, b, c, mz_timestamp ASC
  ),
  rel3 AS ( SELECT * FROM enriched ANTI JOIN rel2 )  -- the non-first-seen rows
AS
  RECORD    rel1 INTO enriched,         -- append frozen enriched deltas
  INTEGRATE rel2 AS  first_seen,        -- maintain a deduped TVC
  DELETE    rel3 FROM enriched;         -- prune superseded rows (bound the dTVC)
```

This records enriched events into a `DELTA TABLE`, integrates a first-seen
(deduped) TVC out of it, and prunes the table to one row per key. The actions
commit atomically at `T`.

- **`RECORD r INTO d`**: `r` must be a dTVC; its deltas are appended to `d`.
- **`INTEGRATE r AS v`**: `r` must be a dTVC; `v` is a maintained TVC reclocked
  back onto the **input's timeline (domain A)** — see "Time domains" below. Each
  delta is placed by its `mz_timestamp` (domain A), clamped to
  `max(mz_timestamp, upper)` for arbitrary / below-frontier values (logical
  compaction, required to hold the TVC invariant), and `v`'s frontier is driven
  into domain A by the reclock. So an event at input-time `t` appears in `v` at
  `t`. `v` is a definite function of the recorded `DELTA TABLE` + reclock;
  non-determinism lives only in the recorded *values*.
- **`DELETE r FROM d`**: removes rows from a `DELTA TABLE` (one way to `bound` it).

**Frontiers: data vs. progress.** The `mz_timestamp` is *data* and does not by
itself set the output's frontier. `INTEGRATE`'s output `v` lives in domain A (the
input's timeline; see below), and its `upper` is driven into domain A by the
reclock — advancing as the `DELTA TABLE`'s domain-B write frontier advances,
gated so it never passes an input-time whose data is not yet recorded. An
idle-but-live input still advances its domain-A frontier, so `v` stays readable.
Whether time-based aging and `mz_now()` in a body resolve in **domain A**
(event-age — "keep the last 30 days of *events*"; stalls if the input idles) or
**domain B** (wall-clock age — always advances) is a real semantic choice the
design must settle (see Open Questions).

**Time domains and reclocking.** A recorder moves data between two time domains
and reclocks back — this is what preserves consistency, since *an event at time
`t` must happen at `t` throughout the system unless we deliberately re-timestamp
it*.

- **Domain A** — the input's timeline; the `mz_timestamp` values live here.
- **Domain B** — the recorder's processing/system time; the `DELTA TABLE`'s
  physical write frontier advances here. (That B is "system time" is incidental —
  it is just a different domain, a durable recording layer.)

- **`RECORD`** takes a query in domain A and writes it as data into the `DELTA
  TABLE`, which physically advances in domain B; the `mz_timestamp` column carries
  the domain-A times, and `RECORD` also **notes the mapping A → B** (the reclock:
  "input time `t` was recorded at system time `t'`").
- **`INTEGRATE`** reads the `DELTA TABLE` (domain B), **places its output by the
  `mz_timestamp` (domain A)** — clamped to `max(mz_timestamp, upper)` for
  arbitrary / below-frontier values (logical compaction) — and **uses the reclock
  to drive its output frontier back into domain A**.

So the integrated output `v` lives in **domain A**: an event at input-time `t`
appears in `v` at `t`, not at the recording time `t'`. That is what lets `v`
compose consistently with the input and sibling collections, and the reason the
recording round-trips A → B → A rather than leaving the result at its recording
time. (A frozen value is computed in domain B but placed at its domain-A time `t`
— temporally consistent, though not the domain-A historical truth at `t`; see the
freeze caveat.)

**Reclocking happens at the write boundary**, and `mz_now()` is contextual — the
body runs in its *inputs'* domain. `RECORD`'s query is in A and reclocks A→B as
it writes the `DELTA TABLE` (so `mz_now()` there is A); `INTEGRATE`'s query reads
the `DELTA TABLE` in B and reclocks B→A as it writes `v` (placing by
`mz_timestamp`, driving the frontier via the reclock; `mz_now()` there is B).

**Representing the reclock (a choice that does not change the model).** The A→B
mapping can be a **separate collection**, or live **in-band** as progress markers
in the `DELTA TABLE` (`SUBSCRIBE`'s `mz_progressed` rows — frontier advances with
no data), making the `DELTA TABLE` a persisted `SUBSCRIBE` stream. Either way the
semantics are identical; only the representation differs:

- *In-band* gives a single-shard, single-CAS `RECORD` (data and reclock cannot
  diverge), but burdens every consumer of the `DELTA TABLE` with progress-marker
  noise — the same awkwardness as consuming `SUBSCRIBE` directly.
- *Separate collection* keeps the `DELTA TABLE` clean to query, at the cost of
  committing the reclock atomically with the data — but that is a **combined
  CAS** the design already needs for the multi-output bundle
  (`RECORD`/`INTEGRATE`/`DELETE` at one `T`), so it is nearly free.

It is an open implementation choice (see Open Questions); the clean-data argument
leans toward a separate collection. (The conceptual symmetry is worth noting
regardless: `CHANGES` turns *changes* into data via `mz_diff`, the frontier can be
turned into data via `mz_progressed`, and `INTEGRATE` reads them back.)

Replica races are an *exactly-once* concern — a delta must not be recorded twice
— not a correctness one (the data and frontiers each function sees are
deterministic); whichever representation, the guard is the CAS on the recording
commit. Non-determinism is confined to the recorded **values** (frozen at
processing time), so `v` is a definite function of the recorded data. The
`mz_timestamp` remains queryable as data for "as of input-time" questions, within
`RETAIN HISTORY`.

The write verbs differ by *shape*: **`RECORD` → `DELTA TABLE`** keeps the per-row
change log in domain B (with `mz_timestamp` as data); **`INTEGRATE` → TVC**
maintains integrated state reclocked onto domain A.

### Freeze is typing, not a keyword

A `RECORDER` body re-evaluates only on **driver** deltas (its dTVC inputs:
`CHANGES(...)` or `DELETE TABLE`s). A **TVC reference** joined in (`JOIN dim d`)
is therefore looked up **once per driver-delta and recorded** — a later change to
`dim` produces no driver-delta, so the recorded row never moves. **Recording a
join against a bare TVC reference *is* the freeze**; no marker is needed.

The rule: **`CHANGES(x)` / `DELTA TABLE` = tracked (deltas flow); bare TVC =
frozen reference (sampled at lookup).** Tracking is the opt-in; freeze is the
default. (A per-value `FROZEN(expr)` survives only as a rare fine-grained tool —
freeze *some* columns of a tracked relation — and is likely droppable for v1.)

A frozen value is sampled at **processing time `t'`** but tagged with the fact's
**`mz_timestamp` `t`**: it is a *processing-time fact recorded against `t`*,
**not** the historical truth of the reference at `t`. So filtering the recorded
data by `mz_timestamp <= t` returns the value as the recorder saw it when it
processed the fact, not what `dim` held at `t`. This is the only option for non-deterministic /
external values (there is no historical truth at `t` for a UDF), but it means a
frozen column must not be conflated with the *definite as-of-event-time temporal
join* (which returns `dim`'s version covering `t`); mixing both in one row yields
columns aligned to different times.

This makes the **compliance cascade compose** rather than needing a bespoke
`ON DELETE CASCADE`: make the dimension a *driver* via a second action.

```sql
  DELETE (SELECT * FROM enriched e WHERE e.user_id IN
            (SELECT user_id FROM CHANGES(users) WHERE mz_diff < 0))
    FROM enriched
```

Frozen value + live existence = two driver-actions in one recorder.

### The evaluation rule (the key semantic)

The example reads and writes `enriched` in the same recorder (`rel2`/`rel3` read
it; `RECORD`/`DELETE` write it). Left unspecified, that is the
Continual-Tasks Decision-Log problem (reads@`T-1` vs `T`, "controlled
iteration"). We avoid it with one rule:

> **All reads in a recorder observe the pre-commit snapshot; all writes apply
> atomically at `T`; there is no intra-commit fixpoint.**

Consequences: a row recorded at `T` is visible to `rel2`/`rel3` only at `T+1`, so
pruning lags recording by one tick — **eventual convergence**, which is exactly
the relaxed bar these use cases accept. There is no read-your-own-writes *within
a commit*, hence none of the intra-commit *fixpoint* that CTs never finished —
though reading a recorder's own output across commits still needs the CT
`step_forward` / read-hold machinery (see Implementation feasibility). (In the
example the
`DELETE` is fixpoint-stable anyway — deleting non-first-seen rows cannot change
which row is first-seen — but the rule is what guarantees that in general.)

### Stream-table join: there are two freezes

For `events` (→ `CHANGES`, carrying `mz_timestamp = t_e`) joined to a dimension:

1. **As of the event's logical time `t_e` — DEFINITE.** Because `t_e` is data,
   "the dim value at `t_e`" is an ordinary relational join against the
   dimension's *changelog* (the version whose validity interval contains `t_e`).
   It does **not** backfill (a later dim change adds a new version; it doesn't
   change which version covered `t_e`), is fully recomputable, and needs **no
   freeze** — the inequality on the timestamp columns *is* the freeze. Cost: the
   dimension must retain history back to the oldest live event. This is nearly
   expressible today with `CHANGES` + `RETAIN HISTORY` + SQL.
2. **As of processing time — NON-DEFINITE.** Attach whatever the dimension was
   when the event was processed, recorded once. Used when you do not want to keep
   dimension history, or the value is not in Materialize (external UDF, `now()`).
   This is the case that needs `record` (and the type-based freeze above).

> **Recording the frozen value is the dual of retaining the input's history.**
> Either keep the inputs' history and recompute the as-of join (definite), or
> record the output and forget the inputs (non-definite). `record`/freeze is the
> second — it buys "forget the upstream state" (finalization) at the price of
> "not recomputable," and it is the only way to attach a value not derivable from
> retained Materialize data.

### Bounding growth = bounding a dTVC

A dTVC is append-only by nature, so the central growth problem is exactly
**keeping a dTVC finite**. `record`-don't-recompute wants to keep data; bounded
resources wants to forget it; they reconcile only if forgetting is **explicit,
part of the contract, and (being non-deterministic) irreversible** — i.e.
finalization. The ways to bound, composed from the operations:

- **Age** — a temporal filter (`mz_now() < mz_timestamp + W`) on the dTVC;
  clock-driven, self-finalizing (insert@`T`/retract@`T+W` consolidate as `since`
  advances), no index. Reuses the `CHANGES` maintained-MV machinery.
- **Referential / ownership** (compliance) — a `DELETE` action driven by
  `CHANGES(dimension) WHERE mz_diff < 0`; a **physical** retraction, not a
  read-time filter; costs an index on the dTVC's liveness key. Note the tension
  with stable history: a retraction placed at the deletion's *event time* leaves
  the rows visible to `AS OF` reads *before* it (history stays stable, but the
  data is not erased from the past — insufficient for GDPR). **True erasure**
  requires advancing `since` to physically drop the rows, which forfeits `AS OF`
  / replay in the erased range. The two are mutually exclusive; "compliance
  erasure" means the latter, scoped: definiteness holds only forward of the
  advanced `since`.
- **Supersession** — a `top-k`/`reduce` body; the reduce emits exact
  retract-old/insert-new diffs (O(live keys)).
- **Arbitrary** — an explicit `DELETE` action (read-then-write; documented cost).

Two physical invariants the engine must hold for space to be reclaimed:

1. **Retractions must consolidate with their inserts** — a `-1` cancels a `+1`
   only if it is the *exact same row*. `reduce` and the cascade emit exact
   retractions; hand-written `DELETE`s may not (the "careful re-integration"
   hazard).
2. **Bounded growth ⇔ bounded `since` lag ⇔ bounded retention** — space is
   reclaimed only once compaction advances `since` past a retraction, so
   `RETAIN HISTORY` trades off directly against bounded growth. The engine owns
   the read policy / compaction on its outputs.

Note that the input window (`CHANGES(events AS OF AT LEAST mz_now() - '1 hour')`)
and the output `DELTA TABLE` are **two independent dTVCs with two independent
bounds**: the window bounds the `differentiate` state; the `DELETE`/dedup bounds
the recorded output.

### Surfaces / altitudes

Same operations, two altitudes; neither adds capability.

1. **`RECORDER`** (declarative, durable object) — the safe default shown above.
   No self-reference beyond the pre-commit-read rule; structurally free of the
   Decision-Log questions.
2. **`BEGIN CONTINUAL TRANSACTION … COMMIT EVERY`** (imperative) — Aljoscha's
   prototype: arbitrary bundled read-then-write statements over the same engine,
   fed from `CHANGES`. The power-user surface; self-referential bodies allowed
   with documented footguns. ("Continual transaction" is reserved for this
   imperative form; the durable object is a `RECORDER`.)

### The correctness ladder is a classification of compositions

The tiers are not primitives; they fall out of which operations a body uses:

| Tier | Composition | Semantics |
|---|---|---|
| **1. Recorded append** | `RECORD` over `CHANGES` (frozen refs) | **exactly-once into persist** |
| **2. General read-then-write** | imperative bundle, self-referential body | exactly-once per commit |
| **3. Eventual / stateful** | `RECORD`/`INTEGRATE` + `reduce`/`top-k` | **at-least-once / eventual**; exact once caught up |
| **4. External effects** | recorded "delivered-through-`T`" frontier | **at-least-once + idempotency key** |

### Restart behavior

- **`RECORD INTO delta_table`**: resume appending from the table's `upper`; no
  snapshot, no recompute. The "consumed-through-`T`" frontier *is* the output's
  `upper`. Definite-by-persistence; the impl-drift risk is acceptable because the
  rows are frozen.
- **`INTEGRATE`**: an MV over the durable `DELTA TABLE`, so it recomputes and
  self-corrects — modulo the history caveat above (current contents definite).

### Dependencies / things that may change

- Builds on `CHANGES` (#36869) and the OCC timestamped-write substrate; needs the
  "commit a bundle of actions atomically at `T`" capability the prototype flagged
  as missing.
- Needs `DELTA TABLE` as a typed object (implicit `mz_timestamp`/`mz_diff`), and
  recorder-owned outputs with an engine-driven read policy / compaction.
- Time-based bounding reuses the `CHANGES` maintained-MV temporal-filter /
  lagged-read-hold mechanism.
- Multi-replica race-to-commit (compare-and-append, losers discard).

## Minimal Viable Prototype

Two prototypes already de-risk this: `CHANGES` (#36869, the `differentiate` side,
including restart-exact reproduction) and `BEGIN CONTINUAL TRANSACTION` (Feb
2026, the imperative surface + the data-plane/control-plane commit split). The
MVP is to connect them as a `RECORDER`: a `RECORD` into a `DELTA TABLE` plus an
`INTEGRATE`, over `CHANGES(input)`, demonstrated on (a) the stream-table join /
UDF enrichment with type-based freeze (Tier 1) and (b) the `top-1` eventual
upsert with input forgetting (Tier 3). The currency-conversion-with-compliance
case (frozen value + a `DELETE`-driven cascade) is the stretch goal that
validates the freeze typing and the lint rule.

## Implementation feasibility

The companion implementation design doc (`20260604_recorders_implementation.md`)
covers this in depth — architecture, the per-crate change map, the ranked risk
register, what to salvage from the removed CT code, and a phased plan. The gating
findings from a codebase pass (against the recovered Continual Tasks (CT)
implementation, PR #35967):

- **Gating dependency.** The atomic "commit a bundle of actions at exactly `T`,
  retry on conflict" the design relies on is the OCC timestamped-write substrate
  (`20260210_incremental_occ_read_then_write.md`), which is **unbuilt** — none of
  its symbols exist in the tree. The *storage* primitive does exist: group commit
  already writes multiple persist shards atomically at a supplied `T` via txn-wal
  (`commit_at(write_ts)` fails on `UpperMismatch`), so multi-action atomicity is
  reachable **if every output is a txns-registered shard**. The net-new work is
  the adapter-level target-`T`/retry loop and the **data-plane → control-plane
  hand-off**. CTs used a bespoke compute sink that wrote a single shard and
  bypassed txn-wal — which is exactly why they could not commit multiple outputs
  atomically, confirming this control-plane path is mandatory, not optional.
- **Known sharp edges, all hit by CTs.** (a) *Self-reference reclocking* — the
  "no fixpoint / `T+1` visibility" rule avoids the fixpoint CTs never finished,
  but the lagged self-read still needs the CT `step_forward` / since-held-below-
  output-upper machinery (reusable, hardest-won). (b) *Multi-output* — CTs were
  structurally single-output (a one-sink-per-dataflow assertion); recorders most
  likely model each `DELTA TABLE`/`INTEGRATE` view as an independent catalog item
  with the `RECORDER` as orchestrator (the tables-from-sources pattern), which is
  new catalog + controller machinery. (c) *Freeze-by-typing* — CTs did it as a
  persist-source renderer trick (inputs fed as insert-then-retract diffs) with a
  known gap; first-class HIR/MIR support is new optimizer work. (d) *`DELTA
  TABLE`* is a new collection kind (rows embedding `mz_timestamp`/`mz_diff` while
  written at system `T`).
- **Reusable from CT history:** body rendering, restart-cheap rehydration
  (resume from output `upper`, snapshot-exclude), and self-reference bootstrap.
  **Not reusable:** the commit path and the multi-output model.

## Alternatives

- **Bespoke features per use case** (separate stream-join, upsert, retention
  features). Rejected: this is what bloated and stalled CTs. The calculus makes
  the surface small and the features fall out as compositions.
- **Revive Continual Tasks as-is.** Rejected: removed for lack of consensus and
  incompleteness; its core mechanism (input reclocking to `T-1`) is the source of
  the input/reference inconsistency and the UPSERT impossibility — which the
  pre-commit-read rule and the `differentiate` framing avoid.
- **Imperative-only / declarative-only.** Rejected as *sole* surfaces: imperative
  alone is session-scoped and exposes self-reference to everyone; declarative
  alone cannot express general bundled read-then-write. Kept as two altitudes.
- **`journal table` instead of `delta table`.** Rejected: "journal" promises
  append-only immutability we do not keep (the recorder prunes/compacts it);
  "delta" names what the rows are (`mz_diff`) and is consistent with dTVC /
  `differentiate`.
- **A `FROZEN()` keyword as the primary freeze.** Rejected as primary: freeze
  falls out of TVC-vs-dTVC typing, so a keyword is redundant for the common case
  (kept only as an optional fine-grained tool).
- **Iceberg source + sink for finalization.** Complementary (archival/offload),
  not a substitute for in-Materialize recorded transforms / non-det enrichment.
- **External Postgres / Kafka loopback (status quo).** Rejected: expensive and
  operationally heavy; the point is to stay inside Materialize.
- **Distributed locking instead of OCC commit.** Rejected (latency, brittleness,
  scalability), per the OCC read-then-write design.
- **Reclock representation: in-band progress markers vs. a separate collection.**
  A representation choice, not a model change (see Solution). In-band
  (`mz_progressed`, a persisted `SUBSCRIBE`) gives a single-shard CAS but makes
  the `DELTA TABLE` awkward to consume; a separate collection keeps the data clean
  at the cost of a combined CAS the multi-output bundle already needs. Leaning
  toward a separate collection for data cleanliness; left open.

## Open questions

- **`RECORDER` object model & syntax.** Confirm `CREATE RECORDER WITH <rels> AS
  <actions>`. How is the trigger cadence expressed (vs `COMMIT EVERY`)? Are the
  bundled actions always one atomic commit at `T`?
- **Reclock representation.** In-band progress markers (`mz_progressed`, a
  persisted `SUBSCRIBE`) vs. a separate reclock collection — a representation
  choice that does not change the model; trades single-shard CAS against
  data-consumption cleanliness (the body leans toward a separate collection).
- **Which domain does `mz_now()` / aging resolve in?** With `INTEGRATE`'s output
  reclocked onto the input timeline (domain A), time-based aging and `mz_now()`
  in a body could mean **domain A** (event-age — "last 30 days of *events*";
  stalls when the input idles) or **domain B** (wall-clock — always advances).
  These are different semantics; the design must pick (perhaps per use:
  event-age vs. wall-clock retention), and the input `AS OF AT LEAST mz_now()`
  window has the same A/B question.
- **Commit-timestamp / frontier-advance policy.** "Every timestamp" vs
  "timestamps where a driver is non-empty" (the Decision Log's question) — the
  `INSERT … VALUES` footgun, exposing millisecond granularity, whether
  time-driven bodies are allowed; the cadence/granularity of frontier ticks and
  the data-write trigger.
- **Freeze typing & per-value `FROZEN`.** Confirm bare-TVC-reference = frozen,
  `CHANGES`/`DELTA TABLE` = tracked. Is a per-value `FROZEN(expr)` needed at all,
  or only the typing? Can one dimension supply a frozen value *and* anchor
  lifetime (via a separate `CHANGES(dim)` driver-action)?
- **Cascade atomicity & cost.** A dimension delete and the resulting recorded-row
  retractions committed at one `T`; how is the liveness-key index costed and made
  explicit?
- **`INTEGRATE` past-dated deltas.** Confirm `max(mz_timestamp, mz_now())` +
  consolidation; the history caveat under `RETAIN HISTORY`; behavior of
  retractions for already-clamped rows.
- **`DELETE` from a `DELTA TABLE` semantics.** Physical removal vs appending a
  `-1`; how reclamation is guaranteed and surfaced when compaction lags.
- **Stream-table join: which freeze is the default?** As-of-event-time (definite,
  needs dim history) vs as-of-processing-time (the `record` route). Do we offer
  the definite temporal join as first-class sugar, given it is nearly expressible
  today?
- **Output ownership & multi-output.** Recorder-owned vs user-writable `DELTA
  TABLE`s; multi-output recorders (webhook demux); `RETAIN HISTORY` interaction.
- **Read-your-own-writes.** Is the pre-commit-read rule sufficient for all
  intended bodies, or do some need controlled iteration (imperative surface)?
- **Naming.** `RECORDER` / `DELTA TABLE` / `RECORD`-`INTEGRATE`-`DELETE`; is
  "recorder" the right object noun, or should it follow the artifact (e.g.
  `JOURNAL`)? Is the `STREAM JOIN` keyword worth adding as sugar for the
  as-of-event-time join?
- **Non-deterministic function story.** Explicit `VOLATILE` marker + memoization,
  or is "recorded once" sufficient?
- **Relationship to standing queries (PR #35347)** — overlap or composition?
