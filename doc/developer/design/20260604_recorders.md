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
cases remain, and two new primitives are now in reach that did not exist when CTs
were designed: the `CHANGES` table function (#36869, open PR) and the OCC
timestamped-write substrate (`20260210_incremental_occ_read_then_write.md`,
designed but unbuilt). This document revives the capability by **distilling it to
a small calculus** (`differentiate` / `integrate` / `record` / `freeze`) realized
over **`DELTA TABLE`s**: one new standing-write object (the `RECORD` writer),
`INTEGRATE` as a read operator usable in ordinary materialized views, and ordinary
DML for bounding.

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
  stays tied to a referenced entity** — deleting a user erases their recorded
  rows, without the values recomputing. *True (GDPR) erasure is a consolidating
  `DELETE` **plus advancing `since`** to physically drop the history (forfeiting
  `AS OF`/replay in the erased range); a cascade `DELETE` alone leaves rows visible
  to earlier `AS OF` reads. Definiteness holds forward of the advanced `since`.*
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
- **Atomic multi-output transactions.** Outputs are independent objects (a delta
  table fed by a `RECORD` writer, MVs over `INTEGRATE`, bounding DML); cross-output
  consistency comes from reading at a common logical time, not from co-committing —
  there is no atomic multi-action bundle.
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
| **differentiate** | TVC → dTVC | `CHANGES(x AS OF …)` operator (open PR #36869) |
| **integrate** | dTVC → TVC | `INTEGRATE(r)` operator — dual of `CHANGES`, usable in plain MVs |
| **record** | dTVC → durable dTVC | `RECORD (r) INTO d` — the one new standing-write object |
| **bound** | keep a dTVC finite | temporal filter, or `DELETE`/`UPDATE` DML on the mutable `d` |

plus **freeze**, which is not a keyword but falls out of typing (below).

The surface is deliberately small: only the **`RECORD` writer is a new standing
object**. `INTEGRATE` is a *read operator* (the dual of `CHANGES`) usable inside
ordinary materialized views — it carries no non-determinism, so it needs no new
object kind. `DELTA TABLE`s are **mutable**, so bounding is ordinary `DELETE` /
`UPDATE` DML, not a standing object. These pieces are **not** bundled into one
atomic transaction: cross-object consistency comes from **reading at a common
logical time** (as it already does everywhere in Materialize); the `RECORD`
writer (and any body that reads what it writes) commits via a **frontier-gated
(OCC) write** (compute over data through frontier `X`, commit at `X+1`).
Everything else — stream-table join, upsert, retention, the compliance cascade,
the correctness tiers — is a *composition*, not a separate feature. (Exact
keywords/object-kind ergonomics are deferred; this doc fixes the model.)

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

**Invariant (optimizer barrier): a `DELTA TABLE` is authoritative, never
recomputable from the `RECORD` body's original inputs.** The optimizer must treat
a delta table as an opaque source — it *may* recompute `INTEGRATE` and everything
downstream *over the delta table + its reclock* (those are definite), but it must
**never** see through `RECORD` to re-derive the recorded rows from the body's
inputs. If it did, a recomputation would re-sample the frozen / non-deterministic
values and this whole boundary argument collapses. This is a load-bearing
soundness rule, not an optimization heuristic.

**Freeze must be diagnosable, never silent (decision).** Because the *same*
`JOIN dim` is a maintained join in a plain MV but a frozen sample inside a `RECORD`
body — and freeze is the default — a `RECORD` author could freeze a reference they
meant to track and get no error. Freeze stays the default (tracking a UDF /
external / `now()` reference is often impossible or unbounded, so *tracking* is the
deliberate opt-in), but the engine must make freeze **loud**: `EXPLAIN` and a
plan-time **`NOTICE` naming every frozen reference** in a body, with an optional
explicit `FROZEN`/`TRACKED` marker for readability (see Open Questions).

### The surface: `DELTA TABLE` and the operations

A **`DELTA TABLE`** is a table typed as a dTVC: it has implicit
`mz_timestamp mz_timestamp` and `mz_diff bigint` columns, and its rows are
*deltas* (its value is their integral). It is *not* append-only — it can be
pruned/compacted, and supports ordinary `DELETE`/`UPDATE` DML — which is why
"delta" (what the rows are), not "journal" (an append-only promise we don't keep),
is the right name. A `DELTA TABLE` **names the domain** its `mz_timestamp`s belong
to (the input's timeline, domain A) and **owns the reclock** for that domain (the
A↔B mapping; see "Time domains"). That makes it self-describing — data (physical,
domain B) + a named domain + the reclock — so any reader knows how to reclock it,
and there is no separate question of "what does `INTEGRATE` reclock by" (it is the
delta table's reclock).

The domain is **inherited by default**: there is one global logical timeline
(domain A) in Materialize, so a delta table left unbound at creation has its
domain **bound by its first `RECORD` writer** (from that writer's differentiate
input). The binding is then **immutable and table-owned** — it survives dropping
the writer, and any later writer must conform to it. An explicit `IN DOMAIN
<name>` is the escape hatch for a hand-written delta table (no input to inherit
from) or to bind several writers to a shared named domain (see "Time domains").

```sql
CREATE DELTA TABLE enriched (a, b, c, val);   -- implicit mz_timestamp, mz_diff
```

The dedup pipeline below uses all three surfaces — the `RECORD` writer object,
`INTEGRATE` inside a plain MV, and bounding DML (syntax illustrative, TBD):

```sql
-- 1) record enriched events (frozen dim lookup) into the delta table.
--    The RECORD writer is the one new standing object; it picks T, re-evaluates
--    on driver deltas, and commits frontier-gated. Cadence is implicit.
CREATE RECORDER enrich INTO enriched AS
  RECORD (
    SELECT e.*, d.val
    FROM CHANGES(events AS OF AT LEAST mz_now() - INTERVAL '1 hour') e
    JOIN dim d ON e.fk = d.key          -- bare TVC reference ⇒ frozen lookup
  );

-- 2) maintain a first-seen (deduped) TVC with INTEGRATE inside a plain MV.
--    All mz_timestamp-aware logic lives INSIDE INTEGRATE's argument: it is the
--    typing boundary. The result is a TVC with no mz_timestamp/mz_diff columns.
CREATE MATERIALIZED VIEW first_seen AS
  SELECT * FROM INTEGRATE(
    SELECT DISTINCT ON (a, b, c) *      -- first +1 per key, by event time
    FROM enriched WHERE mz_diff > 0 ORDER BY a, b, c, mz_timestamp ASC
  );

-- 3) bound the delta table with ordinary (periodic) DML: a consolidating DELETE
--    that retracts superseded rows at their original mz_timestamp.
DELETE FROM enriched e
  WHERE NOT EXISTS (SELECT 1 FROM first_seen f
                    WHERE (f.a, f.b, f.c) = (e.a, e.b, e.c));
```

The `RECORD` writer is the only standing object here; `first_seen` is a normal MV,
and the bounding step is plain DML run when desired. They coordinate through
`enriched`'s logical time, not a shared transaction (see "The evaluation rule").

- **`CREATE RECORDER … INTO d AS RECORD (r)`** — the one new standing-write
  object. `r` must be a dTVC; its deltas are appended to delta table `d`, which
  also advances `d`'s reclock for its named domain. It re-evaluates on its
  **driver deltas** and commits **frontier-gated** (compute through `X`, commit at
  `X+1`); cadence is implicit — there is no `COMMIT EVERY` (an anti-pattern;
  frontier advancement drives commits). (Terminology: the object kind is
  `RECORDER`; "the `RECORD` writer" names its role.)
- **`INTEGRATE(r)`** — a **read operator**, the dual of `CHANGES`; *not* an object
  kind. `r` is a dTVC expression (often a reduce over a delta table). `INTEGRATE`
  is the **typing boundary**: inside its argument `mz_timestamp`/`mz_diff` are
  data; its result is a TVC where they are *gone* — turned into the row's time.
  Each delta is placed by its `mz_timestamp`, clamped to `max(mz_timestamp, upper)`
  for arbitrary / below-frontier values (logical compaction), and the result's
  frontier is driven onto `d`'s named domain (domain A) by `d`'s reclock — so an
  event at input-time `t` appears at `t`. Used in a plain MV (as above) it
  maintains a definite TVC; non-determinism lives only in the recorded *values*.
  *To keep a timestamp as queryable data past the boundary, copy it into an
  ordinary column* (`mz_timestamp AS first_seen_at`) **inside** the argument — but
  **do not treat that column as an immutable fact**: it is subject to the same
  clamp and can **advance as `since` ticks forward**, so a "first seen at" value is
  stable only within `RETAIN HISTORY`. The engine should surface this loudly (a
  warning/lint), since users will expect such a column not to move; see the pitfall
  under "Bounding growth".
- **`DELETE` / `UPDATE` on `d`** — ordinary DML against the mutable delta table,
  one way to `bound` it. A `DELETE` is **consolidating**: it retracts at the
  targeted rows' *original* `mz_timestamp` so the `-1` cancels the `+1` and
  compaction reclaims the space — distinct from age-out (a temporal filter that
  retracts *forward*; see "Bounding growth"). It reads its inputs at a logical
  time and commits as a normal table write; run periodically, it converges
  eventually. (A *standing*, frontier-gated pruner that automates this is a later
  power-user convenience, deferred — keeping v1 bounding off the OCC critical
  path.)

**Frontiers: data vs. progress.** The `mz_timestamp` is *data* and does not by
itself set the output's frontier. `INTEGRATE`'s output `v` lives in domain A (the
input's timeline; see below), and its `upper` is driven into domain A by the
reclock — advancing as the `DELTA TABLE`'s domain-B write frontier advances,
gated so it never passes an input-time whose data is not yet recorded. An
idle-but-live input still advances its domain-A frontier, so `v` stays readable.
Time-based aging and `mz_now()` in a body **default to domain B (wall-clock)** —
retention advances even if the input idles, so a stalled input cannot make
retention unbounded — with **domain A (event-age — "keep the last 30 days of
*events*") as an explicit opt-in**. The input `AS OF AT LEAST mz_now() − W` window
inherits the same default. (Decision; pulled forward to Phase 1, since it gates the
bounding design and the user mental model — see Open Questions.)

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

**The reclock is engine-owned metadata, owned by the `DELTA TABLE` (decision).**
The A→B mapping is a durable, monotone mapping between the two domains — exactly
the **source-remap pattern** (`20210714_reclocking.md`) — **owned by the delta
table** (whose named domain it reclocks), *not* encoded as data in the table. Two
grounds:

- **Clean reasoning.** The reclock is a separable object `R`, so `v` is provably
  `INTEGRATE(DELTA TABLE)` with its frontier driven by `R` — a function we can
  state and verify in isolation, rather than one entangled with parsing frontier
  information back out of the data stream.
- **No tampering, no validation.** It is **engine-owned metadata, not user
  data**, so its invariants (monotone, well-formed) are *maintained* by the
  engine and *assumed* by every consumer — there is nothing for a user to corrupt
  (the frontier is not "just data" in a writable table) and nothing to
  defensively validate on read. It can also be retained independently of the
  data, long enough to interpret history.

This respects a boundary worth stating plainly: **the `DELTA TABLE` is
user-queryable data; the reclock is control-plane bookkeeping** — and tying it to
the delta table (rather than to a recorder, or a free-floating collection)
unifies ownership: the reclock is a property of the *table's accumulated writes*,
and every `INTEGRATE` reader uses it. Folding the reclock into the table as data
is the root of the tampering risk and consumption noise of the in-band
alternative (see Alternatives).

**This is exactly what lets several `RECORD` writers feed one delta table.** Each
writer commits "whenever," interleaved in domain B; the table-owned reclock
recovers the precise A→B mapping regardless of who wrote what when, so the
integral is well-defined over the merged log — a strong motivation for owning the
reclock at the table, not the writer. The constraints that keep it sound: a new
writer starts at (≥) the table's current `upper` (it extends the frontier, never
backfilling below `since`, which is finalized); the domain is **bound once, on the
table, and is immutable** — it survives dropping the writer that bound it, and any
later writer must conform. **`INTEGRATE`'s domain-A `upper` is then the meet
(minimum) over the active writers** of each writer's reclocked committed-through
A-time — the standard multi-input frontier rule: an idle-but-live writer advances
its own A-frontier (idle frontier advance) so it does not stall the meet; a
**dropped** writer leaves the meet, which may then jump forward to the
next-slowest; and a genuinely **stalled** (live but stuck) writer holds the meet
back and surfaces as lag, like any stuck input. Because there is one global domain
A, inherited-domain writers trivially agree (webhook demux = several independent
`RECORD`s into one or several delta tables). Per-writer replica races are an *exactly-once* concern — a
given writer's delta must not be recorded twice — not a correctness one (the data
and frontiers each function sees are deterministic); the guard is the CAS on that
`RECORD` commit. Non-determinism is confined to the recorded **values** (frozen at
processing time), so `v` is a definite function of the `DELTA TABLE` + its reclock.
The `mz_timestamp` remains queryable as data for "as of input-time" questions,
within `RETAIN HISTORY`.

The write verbs differ by *shape*: **`RECORD` → `DELTA TABLE`** keeps the per-row
change log in domain B (with `mz_timestamp` as data); **`INTEGRATE` → TVC**
maintains integrated state reclocked onto domain A.

### Freeze is typing, not a keyword

A `RECORD` writer's body re-evaluates only on **driver** deltas (its dTVC inputs:
`CHANGES(...)` or `DELTA TABLE`s). A **TVC reference** joined in (`JOIN dim d`)
is therefore looked up **once per driver-delta and recorded** — a later change to
`dim` produces no driver-delta, so the recorded row never moves. **Recording a
join against a bare TVC reference *is* the freeze**; no marker is needed. (Freeze
lives only in the `RECORD` body — the non-deterministic boundary; a bare TVC
reference inside a plain MV or an `INTEGRATE` argument is an ordinary maintained
join, not a freeze.)

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
  -- illustrative syntax (TBD); a second driver-action over CHANGES(users)
  DELETE (SELECT * FROM enriched e WHERE e.user_id IN
            (SELECT user_id FROM CHANGES(users) WHERE mz_diff < 0))
    FROM enriched
```

Frozen value + live existence = two independent driver-actions (the `RECORD` and
the cascade `DELETE`), each frontier-gated.

### The evaluation rule (the key semantic)

The `RECORD` writer must commit at a definite time and may read what it writes, so
its commit rule is **frontier-gating**, the same idea as the OCC commit:

> **A writer computes over its inputs through a frontier `X` and commits at
> `X+1`.** Committing at `X+1` means it has observed *every* event `≤ X` — no late
> arrival can still change the result — so the computation is final through `X`.

This is what keeps the split surfaces safe without an atomic bundle. Bounding is
ordinary DML and its semantics are deliberately **relaxed**: a periodic
consolidating `DELETE` of `enriched ANTI JOIN first_seen` reads at some logical
time and may race a concurrent `RECORD`; if it does, it simply runs again later.
Because it only retracts rows that are not first-seen *as observed at its read
time*, the worst case is that it lags — pruning trails recording, **eventual
convergence**, the relaxed bar these use cases accept. The cross-object
consistency a *reader* sees comes from reading all the objects at one logical
time, exactly as in Materialize today. There is no atomic multi-action commit and
no intra-commit fixpoint; the only residual machinery is the read-hold /
`step_forward` the `RECORD` writer needs when it reads a collection it also writes
(see Implementation feasibility). The optional standing pruner (deferred) would
reuse the writer's frontier-gating rule; the v1 manual `DELETE` does not need it.

### Stream-table join: there are two freezes

For `events` (→ `CHANGES`, carrying `mz_timestamp = t_e`) joined to a dimension:

1. **As of the event's logical time `t_e` — DEFINITE.** Because `t_e` is data,
   "the dim value at `t_e`" is an ordinary relational join against the
   dimension's *changelog* (the version whose validity interval contains `t_e`).
   It does **not** backfill (a later dim change adds a new version; it doesn't
   change which version covered `t_e`), is fully recomputable, and needs **no
   freeze** — the inequality on the timestamp columns *is* the freeze. Cost: the
   dimension must retain history back to the oldest live event. This is nearly
   expressible with `CHANGES` (#36869) + `RETAIN HISTORY` + SQL, once `CHANGES`
   lands.
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
  clock-driven, self-finalizing (insert@`T`/retract-forward@`T+W` consolidate as
  `since` advances), no index. Reuses the `CHANGES` maintained-MV machinery. This
  retracts *forward* (the row was true, then ages out); it is not the consolidating
  erasure below.
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
- **Arbitrary** — an explicit consolidating `DELETE` (read-then-write; retracts at
  the rows' original `mz_timestamp` so the `-1` cancels the `+1`; documented cost).

Two physical invariants the engine must hold for space to be reclaimed:

1. **Retractions must consolidate with their inserts** — a `-1` cancels a `+1`
   only if it is the *exact same row*. `reduce` and the cascade emit exact
   retractions; hand-written `DELETE`s may not (the "careful re-integration"
   hazard).
2. **Bounded growth ⇔ bounded `since` lag ⇔ bounded retention** — space is
   reclaimed only once compaction advances `since` past a retraction, so
   `RETAIN HISTORY` trades off directly against bounded growth. The engine owns
   the read policy / compaction on its outputs.

**Two distinct deletes.** Bounding must be precise about *where* a retraction
lands, because only one kind reclaims space:

- **Age-out / retract-forward** — append a `-1` at the *current* time. The row was
  true and becomes false now; integrated, it reads "present until now, then gone."
  This is the temporal filter; it expresses retention, but the log only *grows*.
- **Erasure / consolidating** — retract at the row's *original* `mz_timestamp` so
  the `-1` cancels the `+1` and compaction physically reclaims it (the row "never
  was"). This is what bounds growth, and what `DELETE` against a delta table means.

Ordinary table DML naturally does the former; a delta-table `DELETE` is defined to
do the latter.

**Pitfall — `mz_timestamp` surfaced as data is not stable.** Once `INTEGRATE`
copies `mz_timestamp` into an ordinary column (`… AS first_seen_at`), that value
is subject to the `max(mz_timestamp, upper)` clamp, so it can **advance as `since`
ticks forward** — `first_seen_at` may read *later* after compaction than it did
originally. It is stable only within `RETAIN HISTORY`. The change-records in the
log are immutable, but a timestamp *projected as data through `INTEGRATE`* inherits
the collection's compaction frontier. A powerful tool, but document it.

**Deferred — data-domain compaction.** The dual of logical/physical compaction,
applied in the *data* domain: "advance all `mz_timestamp`s ≤ `t` to `t` and
consolidate," shrinking the log without advancing `since`. Out of scope for v1 (it
sharpens the pitfall above), recorded here as a future capability.

Note that the input window (`CHANGES(events AS OF AT LEAST mz_now() - '1 hour')`)
and the output `DELTA TABLE` are **two independent dTVCs with two independent
bounds**: the window bounds the `differentiate` state; the `DELETE`/dedup bounds
the recorded output.

### Surfaces / altitudes

Same operations, two altitudes; neither adds capability.

1. **Declarative** — a `DELTA TABLE`, a `RECORD` writer feeding it, `INTEGRATE`
   inside ordinary MVs, and `DELETE`/`UPDATE` DML for bounding (shown above); the
   safe default. The pieces are created and dropped independently and coordinate
   via logical-time reads, not a shared transaction.
2. **`BEGIN CONTINUAL TRANSACTION`** (imperative) — Aljoscha's prototype:
   arbitrary bundled read-then-write statements, fed from `CHANGES`, where a single
   `COMMIT` is one atomic write at `T`. The power-user surface; self-referential
   bodies allowed with documented footguns. ("Continual transaction" is reserved
   for this imperative form. Commit cadence is still frontier-driven — a
   declarative `COMMIT EVERY` knob is rejected as an anti-pattern.)

### The correctness ladder is a classification of compositions

The tiers are not primitives; they fall out of which operations a body uses:

| Tier | Composition | Semantics |
|---|---|---|
| **1. Recorded append** | `RECORD` over `CHANGES` (frozen refs) | **exactly-once into persist** (via the per-commit CAS) |
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

- Builds on `CHANGES` (#36869, open PR) and the OCC timestamped-write substrate;
  the **`RECORD` writer** needs the frontier-gated timestamped write (compute
  through `X`, commit at `X+1`). `INTEGRATE` is a read operator over a definite
  collection (no new commit path), and v1 bounding is ordinary DML — both **off
  the OCC critical path**. Cross-object consistency is via logical-time reads, so
  **no cross-object atomic bundle is required**.
- Needs `DELTA TABLE` as a typed, **mutable** object that **names its domain and
  owns its reclock** (implicit `mz_timestamp`/`mz_diff`; `DELETE`/`UPDATE` defined
  as consolidating); engine-driven read policy / compaction on outputs.
- Time-based bounding reuses the `CHANGES` maintained-MV temporal-filter /
  lagged-read-hold mechanism.
- Multi-replica race-to-commit (compare-and-append, losers discard).

## Minimal Viable Prototype

Two in-flight prototypes de-risk this: the `CHANGES` PR (#36869, the
`differentiate` side, including restart-exact reproduction) and `BEGIN CONTINUAL
TRANSACTION` (Feb 2026, the imperative surface + the data-plane/control-plane
commit split). The MVP connects them: a `RECORD` writer into a `DELTA TABLE` fed
by `CHANGES(input)`, plus an `INTEGRATE`-in-a-plain-MV reading it back,
demonstrated on (a) the stream-table join / UDF enrichment with type-based freeze
(Tier 1) and (b) the `top-1` eventual upsert with input forgetting (Tier 3),
bounded by a periodic consolidating `DELETE`. The
currency-conversion-with-compliance case (frozen value + a `DELETE`-driven
cascade) is the stretch goal that validates the freeze typing and the lint rule.

## Implementation feasibility

The companion implementation design doc (`20260604_recorders_implementation.md`)
covers this in depth — architecture, the per-crate change map, the ranked risk
register, what to salvage from the removed CT code, and a phased plan. The gating
findings from a codebase pass (against the recovered Continual Tasks (CT)
implementation, PR #35967):

- **Gating dependency.** The **`RECORD` writer's** frontier-gated commit ("compute
  through `X`, commit at `X+1`, retry on conflict") is the OCC timestamped-write
  substrate (`20260210_incremental_occ_read_then_write.md`), which is **unbuilt** —
  none of its symbols exist in the tree. The *storage* primitive does exist: a
  timestamped write at a supplied `T` via txn-wal (`commit_at(write_ts)` fails on
  `UpperMismatch`). The net-new work is the adapter-level target-`T`/retry loop and
  the **data-plane → control-plane hand-off** (CTs used a bespoke compute sink that
  bypassed txn-wal — the reason a control-plane commit path is mandatory).
  `INTEGRATE` (a read operator over a definite collection) and v1 bounding
  (ordinary DML) are **off this critical path**, and there is **no cross-object
  atomic bundle**, removing the multi-shard-atomicity burden a single RECORDER
  would have imposed.
- **Known sharp edges, all hit by CTs.** (a) *Self-reference reclocking* — the
  frontier-gating rule (compute through `X`, commit at `X+1`) avoids the fixpoint
  CTs never finished, but the lagged self-read still needs the CT `step_forward` /
  since-held-below-output-upper machinery (reusable, hardest-won). (b) *Object
  model* — the only new standing object is the `RECORD` writer; the `DELTA TABLE`
  (+ its reclock) is a storage object, `INTEGRATE` lives inside ordinary MVs, and
  bounding is DML — each its own dataflow, fitting Materialize's
  one-object-one-collection model and avoiding CTs' single-sink-per-dataflow wall;
  no atomic multi-output orchestration is needed. (c) *Freeze-by-typing* — CTs did
  it as a persist-source renderer trick (inputs fed as insert-then-retract diffs)
  with a known gap; first-class HIR/MIR support is new optimizer work. (d) *`DELTA
  TABLE`* is a new, mutable collection kind (rows embedding `mz_timestamp`/`mz_diff`
  while written at system `T`).
- **Reusable from CT history:** body rendering, restart-cheap rehydration
  (resume from output `upper`, snapshot-exclude), and self-reference bootstrap.
  **Not reusable:** the commit path and the multi-output model.

## Alternatives

- **Bespoke features per use case** (separate stream-join, upsert, retention
  features). Rejected: this is what bloated and stalled CTs. The calculus makes
  the surface small and the features fall out as compositions.
- **Revive Continual Tasks as-is.** Rejected: removed for lack of consensus and
  incompleteness; its core mechanism (input reclocking to `T-1`) is the source of
  the input/reference inconsistency and the UPSERT impossibility — which
  frontier-gating and the `differentiate` framing avoid.
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
- **In-band progress markers instead of a separate reclock.** Considered and
  rejected. Encoding the A→B mapping as `mz_progressed` rows in the `DELTA TABLE`
  (a persisted `SUBSCRIBE`) gives a single-shard CAS, but (a) makes the frontier
  *user data* — inviting tampering and forcing defensive validation on every read
  — and (b) burdens every `DELTA TABLE` consumer with progress-marker noise. Its
  single-CAS upside is moot: the one `RECORD` writer already commits the delta
  table and its reclock together. (Conceptually neat — `mz_diff` is changes-as-data,
  `mz_progressed` is frontier-as-data — but not worth storing that way.)

## Open questions

- **Object kinds & syntax.** *Decided:* the only new standing object is the
  `RECORD` writer (illustratively `CREATE RECORDER … INTO d AS RECORD (…)`);
  `INTEGRATE` is a read operator usable in ordinary MVs (not an object kind);
  bounding is `DELETE`/`UPDATE` DML on a mutable `DELTA TABLE`; a delta table's
  domain is **inherited from its first `RECORD` writer** by default, with
  `IN DOMAIN <name>` as the escape hatch; cadence is **implicit / frontier-driven**
  (`COMMIT EVERY` rejected as an anti-pattern). *Open:* the exact keywords /
  object-kind ergonomics (is `RECORDER` the right noun for the writer? how is the
  `RECORD` vs prune verb spelled?).
- **Which domain does `mz_now()` / aging resolve in?** *Decided:* **default domain
  B (wall-clock)** so retention advances even when the input idles, with **domain A
  (event-age — "last 30 days of *events*") as an explicit opt-in**; the input
  `AS OF AT LEAST mz_now()` window inherits the same default. Pulled forward to
  Phase 1 (it gates the bounding design and the user mental model). *Open:* the
  opt-in syntax, and whether any single body needs both domains at once.
- **Commit-timestamp / frontier-advance policy.** With cadence frontier-driven
  (no `COMMIT EVERY`), at which frontiers does a writer actually commit — "every
  timestamp" vs "timestamps where a driver is non-empty" (the Decision Log's
  question)? A time-driven body (referencing `mz_now()` with no data driver) is
  driven by the clock frontier; confirm that subsumes the time-driven case. Plus
  the `INSERT … VALUES` footgun and millisecond-granularity exposure.
- **Freeze typing, diagnostics & per-value markers.** *Decided:* freeze stays the
  **default** in a `RECORD` body (bare-TVC-reference = frozen; `CHANGES`/`DELTA
  TABLE` = tracked), but must be **diagnosable, never silent** — `EXPLAIN` + a
  plan-time `NOTICE` naming every frozen reference. (Flipping to tracked-default
  was considered and not taken: tracking a UDF / external / `now()` reference is
  often impossible or unbounded.) *Open:* whether an explicit `FROZEN`/`TRACKED`
  marker is offered for readability (redundant for the type checker); whether one
  dimension can supply a frozen value *and* anchor lifetime (via a separate
  `CHANGES(dim)` driver-action).
- **Cascade cost.** The compliance cascade is a frontier-gated `DELETE` driven by
  `CHANGES(dim)`; how is the liveness-key index (to find a deleted entity's rows)
  costed and made explicit?
- **`INTEGRATE` past-dated deltas & timestamp-as-data stability.** Confirm
  `max(mz_timestamp, upper)` + consolidation; the history caveat under
  `RETAIN HISTORY`; behavior of retractions for already-clamped rows. Note the
  pitfall: a timestamp surfaced as a data column (`… AS first_seen_at`) inherits
  this clamp and can advance as `since` ticks forward. *Decided:* surface it loudly
  (a warning/lint), since users will expect such a column to be immutable; exact
  mechanism (warning vs hard error vs `EXPLAIN`-only) open.
- **`DELETE` from a `DELTA TABLE` semantics.** *Decided:* a delta-table `DELETE` is
  **consolidating** — it retracts at the rows' original `mz_timestamp` so the `-1`
  cancels the `+1` (distinct from age-out, which retracts forward via a temporal
  filter). *Open:* how physical reclamation is guaranteed and surfaced when
  compaction lags; the unenforceable exact-row-retraction invariant for
  hand-written `DELETE`s; and **data-domain compaction** ("advance all
  `mz_timestamp`s ≤ `t` to `t`, consolidate") as a deferred future capability.
- **Stream-table join: which freeze is the default?** As-of-event-time (definite,
  needs dim history) vs as-of-processing-time (the `record` route). Do we offer
  the definite temporal join as first-class sugar, given it is nearly expressible
  today?
- **Output ownership.** *Decided:* **multiple `RECORD` writers per delta table are
  allowed** — the table-owned reclock recovers the A→B mapping over the interleaved
  log; new writers extend the frontier (no backfill below `since`) and conform to
  the bound-once, immutable domain. `INTEGRATE`'s domain-A `upper` is the meet over
  active writers' reclocks (idle-but-live advances; a dropped writer leaves the
  meet; a stalled one holds it back — see "Time domains"). *Open:* may users *also*
  hand-write a delta table directly (mixed `RECORD` + DML provenance)? `RETAIN
  HISTORY` interaction.
- **Read-your-own-writes / frontier-gating.** Is "compute through `X`, commit at
  `X+1`" sufficient for all intended bodies, or do some need controlled iteration
  (imperative surface)?
- **Naming.** *Decided:* keep `CHANGES` (open PR, not renamed to `DIFFERENTIATE`)
  and `INTEGRATE` — dual in concept, not lexically symmetric. *Open:* the object
  kind name for the `RECORD` writer (`RECORDER`?) and whether `STREAM JOIN` is
  worth adding as sugar for the as-of-event-time join.
- **Non-deterministic function story.** Explicit `VOLATILE` marker + memoization,
  or is "recorded once" sufficient?
- **Relationship to standing queries (PR #35347)** — overlap or composition?
