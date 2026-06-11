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
produce a *different* answer, so it must be avoided. The guarantee is **at-least-once
application** of the (possibly non-deterministic) function and **at-most-once
persistence** of its result (a given result is committed once, via the per-commit
CAS) — i.e. exactly-once into persist where the application succeeds. And because we give up
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
a small calculus** (`differentiate` / `integrate` / `record` / `freeze`) over
**regular tables + an explicit progress collection**: one new standing-write object (the
`RECORD` writer), `INTEGRATE` as a SQL combinator usable in ordinary materialized
views, and ordinary DML for bounding — no new collection kind.

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
  rows, without the values recomputing. *True (GDPR) erasure is a `DELETE` of the
  rows **plus advancing `since`** to physically drop the history (forfeiting
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
- A **dTVC** (delta-TVC) carries its change *as data*: rows with a **time** and a
  **diff** column (ordinary, user-named — `change_ts` / `change_diff` here, not
  reserved `mz_` names). Its value is the integral of those rows.

Four operations move between them:

| operation | signature | surface |
|---|---|---|
| **differentiate** | TVC → dTVC | `CHANGES(rel [AS OF …] USING TIME …, DIFF …)` operator (draft PR #36869) |
| **integrate** | dTVC → TVC | `INTEGRATE(table)` combinator — carriers + progress collection declared on the table |
| **record** | dTVC → durable dTVC | `CREATE RECORDER … INTO <table> AS <changelog query>` — the one new standing-write object (writes table + a progress lane) |
| **bound** | keep a dTVC finite | temporal filter, or ordinary `DELETE` of changelog rows (data-domain compaction = `clamp + GROUP BY/SUM`) |

plus **freeze**, which is not a keyword but falls out of typing (below).

The surface is deliberately small, and **needs no new collection kind**. The store
is a **regular table** that declares its change carriers — a **time** and a
**diff** column — and an associated progress collection in one clause:
`CREATE TABLE … WITH (PROGRESS, TIMESTAMP = …, DIFF = …)`. The carriers are ordinary
data columns (no reserved `mz_` names); `CHANGES` *produces* them (it names its
output time/diff so a recorder can write them into the table) and the table *holds*
them, so `INTEGRATE(table)` needs no carrier arguments at all. Only three things
are new:

- the **`RECORD` writer** — the one new standing object; it picks the commit time
  `T`, commits **frontier-gated**, and writes the table **and a lane of the table's
  progress collection** together;
- the **progress collection** — a named, relation-valued, engine-written /
  user-read-only mapping (the source-progress-subsource precedent) **owned by the
  *table*** (created by `WITH (PROGRESS, …)`, initialized to `(0, 0)`); it is what
  `INTEGRATE` reclocks through. *("Reclock"/"reclocking" name the operation of
  mapping through it; "progress collection" is the object.)*
- **`INTEGRATE(table)`** — a *combinator* (the dual of `CHANGES`) usable inside
  ordinary materialized views, taking just a **recorded table** (carriers and
  progress come from the table). It carries no non-determinism, so it needs no new
  object kind. (Application shaping — dedup, upsert, top-k — lives in the `RECORD`
  body, not in `INTEGRATE`.)

Bounding is ordinary `DELETE` of changelog rows. These pieces are **not** bundled
into one atomic transaction: cross-object consistency comes from **reading at a
common logical time** (as it already does everywhere in Materialize); the `RECORD`
writer (and any body that reads what it writes) commits via a **frontier-gated
(OCC) write** (compute over data through frontier `X`, commit at `X+1`).
Everything else — stream-table join, upsert, retention, the compliance cascade,
the correctness tiers — is a *composition*, not a separate feature. (Exact
keywords/object-kind ergonomics are deferred; this doc fixes the model.)

### The conceptual basis: TVC ↔ dTVC duality

`differentiate` turns a *mutable* TVC into an **append-only log of immutable
change-records**: each change becomes a row carrying a **time** and a **diff**
column *as ordinary data* (`change_ts` / `change_diff` here — user-named, not
reserved). Each `(row, change_ts, change_diff)` is permanent and self-describing,
so it is independent of system/processing time — *when* you read it, or how the
source mutates afterward, cannot change it.

**`change_diff` is data, not the persist multiplicity (fork resolved).** `CHANGES`
emits every change as a distinct row at persist multiplicity `+1`, with the signed
count living in the `change_diff` *column* (per #36869:
`(pack(row, time, diff), max(time, as_of), +1)`). The earlier draft used the diff
both ways — as data (`WHERE change_diff > 0`, per-event dedup, `CHANGES` parity)
**and** as the persist multiplicity ("rows are deltas whose value is their
integral", "consolidating `DELETE` cancels the `+1`"). Those are incompatible; we
keep it as **data**. `INTEGRATE` interprets it by accumulation (below), and
bounding is an ordinary `DELETE` of changelog rows.

**The duality, stated precisely.** `differentiate` and `integrate` are a
**carrier-preserving inverse pair**, both in the input's own timeline (domain A):
`differentiate` (`CHANGES`, `D = 1 − z⁻¹`) is `TVC(A) → changelog(A)`; `integrate`
(`INTEGRATE`, `I = D⁻¹`) is `changelog(A) → TVC(A)`, summing the `change_diff`s back
up. Neither crosses time domains and neither is non-deterministic. **The `A → B`
move lives only in `RECORD`** — the durable write at processing time (domain B),
which also notes the progress collection; *this* is where the commit time `T` is picked and
frozen values are sampled, i.e. the only non-deterministic leg. So the earlier
"`INTEGRATE`/`RECORD` are integrate" was imprecise: `INTEGRATE` is the pure,
definite inverse of `CHANGES`; `RECORD` is a separate durable-write-plus-reclock
leg that, being non-deterministic, *cannot* live inside the definite pair. `freeze`
is `sample-and-hold` (a projection with `D(freeze x) = 0`), used inside the
`RECORD` body.

### The determinism boundary (why composition stays sound)

Non-definiteness enters at exactly two points — **the commit picks `T`** and **a
frozen value samples a non-tracked source at `T`** — and is then **absorbed**:
the moment a result is `RECORD`ed/`INTEGRATE`d it is definite-by-persistence, so
downstream MVs read a definite collection and never see the non-determinism.
`differentiate`, `integrate`, `JOIN`, `reduce`, and temporal filters introduce
none. The single enforceable rule: **freezing (and processing-time writes) are
legal only inside a `RECORDER`'s recorded output**; a frozen lookup in a plain MV
taints it and is rejected at plan time.

**Invariant (optimizer barrier): a recorded table is authoritative, never
recomputable from the `RECORD` body's original inputs.** The optimizer must treat
the recorded table as an opaque source — it *may* recompute `INTEGRATE` and
everything downstream *over the table + its progress collection* (those are definite), but it must
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

### The surface: a regular table, an explicit progress collection, and `INTEGRATE`

The **store is a regular table** that happens to carry a time column and a diff
column (`change_ts` / `change_diff` — ordinary data, user-named). There is **no
`DELTA TABLE` collection kind**: the rows are change-records, the integral is
reconstructed on read by `INTEGRATE`, and the table is mutable like any other (you
`DELETE` from it to bound it). A stored, never-bounded audit log is just "a regular
table you only `INSERT` into" — still no special kind.

The **progress collection** is an explicit object — a named, relation-valued,
**engine-written / user-read-only** mapping between the input timeline (domain A)
and the table's processing/write timeline (domain B), following the source-remap /
progress-subsource precedent (`20210714_reclocking.md`, where remap was always a
collection); `INTEGRATE` *reclocks* through it. It is created with the table by
`WITH (PROGRESS, …)` and is *referenceable* but not user-writable — so there is
nothing to tamper with and nothing to defensively validate on read (preserving the
rejection of in-band `mz_progressed` markers). `RECORD` writes the data table **and**
a lane of its progress collection together, as a two-shard `group_commit`.

The domain a progress collection describes is **inherited by default** (there is one global
logical timeline, domain A, so it is bound from the first `RECORD` writer's
differentiate input and is then immutable — it survives dropping the writer); an
explicit binding is the escape hatch for a hand-written table or several writers
sharing a domain (see "Time domains").

```sql
CREATE TABLE enriched (
  a BIGINT, b BIGINT, c BIGINT, val TEXT,
  change_ts mz_timestamp NOT NULL,        -- the carrier columns, declared once
  change_diff bigint NOT NULL
) WITH (PROGRESS, TIMESTAMP = change_ts, DIFF = change_diff);
-- WITH (PROGRESS, …) makes this a recorded table: it names the carrier columns and
-- creates an associated, engine-written progress collection, initialized to (0, 0)
-- ("no data yet, more may come" — an empty progress collection means an empty,
-- i.e. sealed, frontier, so it must be initialized). INTEGRATE then needs no
-- carrier arguments; it reads them, and the progress, from the table.
```

The dedup pipeline below uses all three pieces — the `RECORD` writer, the
`INTEGRATE` combinator inside a plain MV, and bounding DML (syntax illustrative,
TBD):

```sql
-- 1) record enriched events (frozen dim lookup) into the table.
--    The RECORD writer is the one new standing object; it picks T, re-evaluates
--    on driver deltas, and commits frontier-gated. Cadence is implicit. CHANGES
--    names its carriers with USING TIME …, DIFF …; the AS OF binds to CHANGES
--    (the SUBSCRIBE pattern), not the inner query.
CREATE RECORDER enrich INTO enriched AS
  SELECT e.*, d.val
  FROM CHANGES(events AS OF AT LEAST mz_now() - INTERVAL '1 hour'
               USING TIME change_ts, DIFF change_diff) e
  JOIN dim d ON e.fk = d.key;           -- bare TVC reference ⇒ frozen lookup
-- the recorder writes a lane into `enriched`'s progress collection in the same commit.

-- 2) reconstruct the recorded collection with INTEGRATE over the *table*. It does
--    the accumulate-and-threshold (GROUP BY the non-carrier columns, SUM(change_diff)
--    keeping > 0) internally, and reads the carriers + progress from `enriched` — so
--    it takes the table directly, with no carrier and no progress arguments.
CREATE MATERIALIZED VIEW enriched_now AS
  SELECT * FROM INTEGRATE(enriched);

-- 3) bound the table with ordinary (periodic) DML — a real persist retraction.
--    Integral-preserving bounding is data-domain compaction (see "Bounding growth").
DELETE FROM enriched WHERE change_ts < mz_now() - INTERVAL '30 days';
```

The `RECORD` writer is the only standing object here; `enriched_now` is a normal MV;
bounding is plain DML. **Shaping** — dedup, first-seen, upsert (retract-old /
insert-new), top-k — lives in the **`RECORD` body** (it decides *which* deltas to
record), not in `INTEGRATE`. First-seen and upsert bodies read what they have already
recorded, so they are **self-referential** (Tier 3; see "The evaluation rule"); the
clean cases that record raw (like `enrich` above) are not. They coordinate through
`enriched`'s logical time, not a shared transaction.

- **`CREATE RECORDER … INTO <table> AS <changelog query>`** — the one new
  standing-write object. The body is a bare query (no `RECORD(...)` wrapper — it
  binds like `CREATE MATERIALIZED VIEW … AS <query>`, and `INTO <table>` names the
  destination like `CREATE SINK … INTO`); it must type as a changelog (dTVC). Its
  rows are appended to `<table>`, and the writer advances a **lane of `<table>`'s
  progress collection** (it belongs to the *table*, not the recorder — see "The
  progress collection") in the same commit. It re-evaluates on its **driver deltas**
  and commits **frontier-gated** (compute through `X`, commit at `X+1`); cadence is
  implicit — no `COMMIT EVERY` (an anti-pattern; frontier advancement drives
  commits). (Object kind: `RECORDER`.)
- **`INTEGRATE(table)`** — a **combinator**, the dual of `CHANGES`; *not* an object
  kind. It takes a **recorded table** and reconstructs its TVC by **accumulating
  `DIFF` per row and thresholding at zero** — internally a `GROUP BY <non-carrier
  columns, TIME>` summing `DIFF` and keeping `> 0` (multiplicity `max(0, Σ)`,
  dropping net-non-positive). Building the threshold in makes it **safe by
  construction**: a negative accumulation can never leak past the boundary into a
  relation that must have non-negative multiplicity. (This is why `repeat_row` is the
  wrong primitive to expose — `RepeatRow` emits negatives, `RepeatRowNonNegative`
  errors; the threshold lives in the reduce, so only a non-negative count is ever
  materialized.) The cost is a **stateful reduce**: memory ∝ live output. **No
  arguments beyond the table**: the carrier columns and the progress collection are
  declared on the table (`WITH (PROGRESS, TIMESTAMP = …, DIFF = …)`), so `INTEGRATE`
  reads both from it — there is nothing to mismatch. (This is the reason the argument
  is a table and not arbitrary SQL: an arbitrary relation has no single,
  well-defined progress collection; to combine sources, record them into one table
  via multi-writer rather than union-at-integrate.) It drives the output frontier
  into domain A. `INTEGRATE` is the **typing boundary**: `TIME`/`DIFF` are
  data in the table and *gone* (turned into time / multiplicity) in the result. It is
  definite given `(table, progress collection)`; non-determinism lives only in the recorded
  *values*. To keep an event-time as a stable queryable value, **record it as an
  ordinary value column** — not the `TIME` carrier, which is consumed and clamps (see
  "Bounding growth").
- **`DELETE` / `UPDATE` on the table** — ordinary DML; bounding is just deleting
  changelog rows, which is a **real persist retraction** that reclaims once `since`
  advances (no special "consolidating delete" semantics needed). Because deleting
  rows changes what `INTEGRATE` reconstructs, safe bounding is **integral-preserving**:
  rewrite each key's deltas `≤ t` as a single summed delta at `t` — a **reduce**
  (`clamp + GROUP BY/SUM`), *not* a bare in-place clamp (**data-domain compaction**;
  see "Bounding growth"). Run periodically, it converges eventually. (A *standing*,
  frontier-gated pruner that automates it is a later power-user convenience,
  deferred — keeping v1 bounding off the OCC critical path.)

**Frontiers: data vs. progress.** The `change_ts` column is *data* and does not by
itself set the output's frontier. `INTEGRATE`'s output `v` lives in domain A (the
input's timeline; see below), and its `upper` is driven into domain A by the
reclock — advancing as the table's domain-B write frontier advances, gated so it
never passes an input-time whose data is not yet recorded. An idle-but-live input
still advances its domain-A frontier, so `v` stays readable.
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

- **Domain A** — the input's timeline; the `change_ts` values live here.
- **Domain B** — the recorder's processing/system time; the table's physical write
  frontier advances here. (That B is "system time" is incidental — it is just a
  different domain, a durable recording layer.)

- **`RECORD`** takes a query in domain A and writes it as data into the table,
  which physically advances in domain B; the `change_ts` column carries the
  domain-A times, and `RECORD` also **notes the mapping A → B** (the progress collection:
  "input time `t` was recorded at system time `t'`").
- **`INTEGRATE`** reads the table (domain B), **places each delta's contribution by
  `change_ts` (domain A)** — folding below-frontier times into the current `upper`,
  then accumulating `change_diff` per row and thresholding — and **uses the progress collection
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
it writes the table (so `mz_now()` there is A); `INTEGRATE` reads the table in B
and reclocks B→A as it produces `v` (placing by `change_ts`, driving the frontier
via the progress collection; `mz_now()` there is B). Consequence worth stating: to stamp
**system / wall-clock time** onto a recorded row (e.g. an `ingested_at` for
staleness), use **`now()`** — a frozen processing-time sample — *not* `mz_now()`,
which in a `RECORD` body is the domain-A *event* time. (`now()` is non-deterministic
and so legal only here, recorded once; record it as a **value column**, where it is
stable, not as the `change_ts` carrier.)

**The progress collection (decision).** The A→B mapping is a durable, monotone
mapping between the two domains — exactly the **source-remap pattern**
(`20210714_reclocking.md`, where remap was always a collection); we call the object
the **progress collection** (matching a source's), and "reclock"/"reclocking" the
operation of mapping through it. It is a **named, relation-valued object**, created
with the table by `WITH (PROGRESS, …)` and initialized to `(0, 0)` (an empty
progress collection is an empty — i.e. sealed — frontier, so it must be
initialized), written by `RECORD` alongside the data table and **read-only to
users**, *not* encoded as data in the table. Three grounds:

- **Clean reasoning.** The progress collection is a separable object `R`, so `v` is provably
  `INTEGRATE(table)` with its frontier driven by `R` — a function we can state and
  verify in isolation, rather than one entangled with parsing frontier information
  back out of the data stream.
- **No tampering, no validation.** Engine-written and user-read-only, its
  invariants (monotone, well-formed) are *maintained* by the engine and *assumed*
  by every consumer — nothing for a user to corrupt, nothing to defensively
  validate on read. It can be retained independently of the data, long enough to
  interpret history.
- **An associated subobject of the *table*, not the recorder.** The progress
  collection is intrinsic to the table — you cannot interpret/integrate the table
  without it — so it is owned by the table and **created with it** by `WITH
  (PROGRESS, …)`, surfaced as an associated relation (the `CREATE SOURCE … EXPOSE
  PROGRESS AS <name>` precedent; engine-written, user-read-only). A `RECORDER` only
  *writes a lane* into it; it does not own it (and could not — N recorders feed one
  table). `INTEGRATE(table)` therefore looks the progress collection up **from the
  table**: there is **no per-operator argument**, and (the reason `INTEGRATE` takes a
  table, not a relation) no ambiguity about *which* progress collection — a mismatch
  is unrepresentable.

This respects a boundary worth stating plainly: **the recorded table is
user-queryable data; the progress collection is control-plane bookkeeping that users may read
but not write.** Folding the mapping into the table as data is the root of the
tampering risk and consumption noise of the in-band alternative (see Alternatives).

**One progress collection per table — structurally, and necessarily.** `B` is the table's
single persist-shard write frontier (one shard, one clock), and A-completeness is a
*joint* property of all the table's writers. So **all `RECORD`s into a table and
all `INTEGRATE`s of it share one progress collection — and must, for consistency.** Multiple
`INTEGRATE`s (say a top-1 upsert and a sum) then agree on "what is final" by
construction (same progress collection → same completeness frontier) and compose at a common
logical time. The table is therefore *the unit of shared completeness*: wanting two
readers to integrate over *different* completeness is a statement that they should
not share a table — use separate tables. (A `WITH (TIMELINE = …)` option sets the
units/timeline for a hand-built table; it is not a shared remap. We avoid an `IN
DOMAIN` clause — it would overload the busy `IN` keyword and foreclose a future
standard `CREATE DOMAIN`.)

**Several `RECORD` writers can feed one table — via per-writer progress lanes, with
zero mutual coordination.** A single *merged* frontier can't work: a writer only
knows its own committed-through `X_i`, and a fast writer must not advance
A-completeness past a slow one. So each writer records its **own lane** `R_i: B →
X_i`, and the table's A-completeness `= meet_i R_i[B_upper]`, **computed at read
time**. Each `RECORD` commit atomically writes *(its deltas + its own lane)* — a
per-writer two-shard `group_commit`, independent of every other writer; consistency
is reconstructed by the meet, so multi-writer is cheap.

- **Add a writer:** it registers a lane starting at `≥` the current `upper`
  (extends; no backfill below `since`).
- **Drop a writer:** its lane leaves the meet, which may jump forward — a **one-way
  door** (you cannot un-declare a time complete); its already-committed deltas stay.
- **Drop *all* writers:** two reasonable terminal states — **freeze** (leave the
  progress where it is; the table stays readable up to its last completeness but
  never advances) or **seal** (advance to the *top* / empty frontier — complete to
  the end of time, a static, fully-readable collection, **finalization for free**).
  Which is the default is open (see Open Questions).

Per-writer replica races stay an *exactly-once* (CAS) concern — a given writer's
delta must not be recorded twice — not a correctness one. Non-determinism is
confined to the recorded **values** (frozen at processing time), so `v` is a
definite function of the table + its progress collection. The `change_ts` remains queryable as
data for "as of input-time" questions, within `RETAIN HISTORY`. (Webhook demux =
several independent `RECORD`s, one lane each.)

The two write shapes differ: **`RECORD` → recorded table** keeps the per-event
change log in domain B (with `change_ts`/`change_diff` as data); **`INTEGRATE`**
reconstructs integrated state reclocked onto domain A (and persists *as state* only
when wrapped in an MV).

### Freeze is typing, not a keyword

A `RECORD` writer's body re-evaluates only on **driver** deltas (its dTVC inputs:
`CHANGES(...)` or a recorded changelog). A **TVC reference** joined in (`JOIN dim d`)
is therefore looked up **once per driver-delta and recorded** — a later change to
`dim` produces no driver-delta, so the recorded row never moves. **Recording a
join against a bare TVC reference *is* the freeze**; no marker is needed. (Freeze
lives only in the `RECORD` body — the non-deterministic boundary; a bare TVC
reference inside a plain MV or an `INTEGRATE` argument is an ordinary maintained
join, not a freeze.)

The rule: **`CHANGES(x)` / a recorded changelog = tracked (deltas flow); bare TVC =
frozen reference (sampled at lookup).** Tracking is the opt-in; freeze is the
default. (A per-value `FROZEN(expr)` survives only as a rare fine-grained tool —
freeze *some* columns of a tracked relation — and is likely droppable for v1.)

A frozen value is sampled at **processing time `t'`** but tagged with the fact's
**`change_ts` `t`**: it is a *processing-time fact recorded against `t`*,
**not** the historical truth of the reference at `t`. So filtering the recorded
data by `change_ts <= t` returns the value as the recorder saw it when it
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
            (SELECT user_id FROM CHANGES(users) WHERE change_diff < 0))
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
integral-preserving compaction of `enriched` (data-domain compaction over
`change_ts ≤ t`) reads at some logical time and may race a concurrent `RECORD`; if
it does, it simply runs again later (it only touches `change_ts ≤ t`, well below the
live frontier; OCC retries any overlap). The worst case is that it lags — bounding
trails recording, **eventual convergence**, the relaxed bar these use cases accept.
The cross-object
consistency a *reader* sees comes from reading all the objects at one logical
time, exactly as in Materialize today. There is no atomic multi-action commit and
no intra-commit fixpoint; the only residual machinery is the read-hold /
`step_forward` the `RECORD` writer needs when it reads a collection it also writes
(see Implementation feasibility). The optional standing pruner (deferred) would
reuse the writer's frontier-gating rule; the v1 manual `DELETE` does not need it.

### Stream-table join: there are two freezes

For `events` (→ `CHANGES`, carrying `change_ts = t_e`) joined to a dimension:

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

### Bounding growth = bounding a changelog

A changelog grows without bound by nature, so the central problem is **keeping it
finite**. `record`-don't-recompute wants to keep data; bounded resources want to
forget it; they reconcile only if forgetting is **explicit, part of the contract,
and (being non-deterministic) irreversible** — i.e. finalization. Because
`change_diff` is *data* (not the persist multiplicity), nothing auto-consolidates:
every delta is a distinct persist `+1` row, and **bounding is an ordinary `DELETE`
of changelog rows** — a real persist retraction that reclaims once compaction
advances `since` past it.

The catch is that `INTEGRATE` reconstructs its result *from* the changelog, so
deleting rows arbitrarily changes that result. Safe bounding therefore deletes in
**integral-preserving** form:

- **Data-domain compaction (the primitive).** Rewrite each key's deltas with
  `change_ts ≤ t` as a single summed delta at `t`. It is a **reduce, not a bare
  in-place clamp**: clamp `change_ts := max(change_ts, t)`, then
  `GROUP BY <all data columns, clamped change_ts>` summing `change_diff` into one
  row per group (net-zero groups drop). A bare clamp is **wrong** — two rows clamped
  to the same `t` become identical `SourceData`, which persist merges into
  multiplicity 2 while the `change_diff` data column still reads `+1` (a direct
  consequence of `change_diff`-as-data; under the rejected multiplicity reading
  persist would have auto-consolidated, so you must sum the column explicitly). It is
  **integral-preserving above `t`** — clamping only lifts rows from `< t` up to `t`,
  so for every `T ≥ t` the included set, hence the integral, is unchanged; history
  below `t` is discarded (the finalization price). The principled bounding operation.
- **Age / retention.** A horizon `t = mz_now() − W`: compact or drop everything
  older. Per the aging decision this defaults to **wall-clock (domain B)** so a
  stalled input cannot make retention unbounded; event-age (domain A) is opt-in.
- **Referential / ownership (compliance).** A `DELETE` driven by
  `CHANGES(dim) WHERE change_diff < 0` physically deletes a removed entity's
  changelog rows; costs an index on the liveness key. Note the tension with stable
  history: a deletion at the entity's *event time* still leaves rows visible to
  `AS OF` reads *before* it. **True (GDPR) erasure** additionally requires advancing
  `since` to physically drop the history, forfeiting `AS OF`/replay in that range —
  mutually exclusive with stable history; definiteness holds forward of the advanced
  `since`.
- **Supersession.** A `top-k`/`reduce` body records only the live net per key, so
  the changelog stays bounded by construction (O(live keys)).

Two realities the engine must hold:

1. **`DELETE` is a genuine retraction; bounding must preserve the live integral.**
   Deleting a changelog row removes its persist `+1`; data-domain compaction is the
   form that does this *without* changing `INTEGRATE`'s result. A hand-written
   `DELETE` that is not integral-preserving silently changes the reconstructed TVC —
   the documented hazard (and `INTEGRATE`'s `max(0, Σ)` threshold is the safety net
   that at least keeps multiplicities non-negative).
2. **Bounded growth ⇔ bounded `since` lag ⇔ bounded retention** — space is reclaimed
   only once compaction advances `since` past a deletion, so `RETAIN HISTORY` trades
   off directly against bounded growth. The engine owns the read policy / compaction
   on its outputs (and on `INTEGRATE`'s arrangement).

**Data-domain compaction is reclock-free and recorder-free.** It lives entirely in
the A/data domain — no progress entry, no recorder involvement. The horizon `t` it
compacts to is an **A-domain retention frontier** (an A-`since`, distinct from the
persist B-`since`), bounded only by downstream `INTEGRATE`/MV read-holds (the
engine-owned read policy). Reclamation is the usual two layers: the reduce commits
as ordinary DML, and persist reclaims when the B-`since` passes it. It **races
`RECORD` harmlessly** (it touches only `change_ts ≤ t`, well below the live
frontier; OCC retries any overlap), is **idempotent** for a fixed `t` and
**monotone** as `t` advances — so a standing pruner can simply re-run it. Recorded
*value* columns survive (they are part of the group key); only the placement
`change_ts` clamps — the same phenomenon as the pitfall below, and the reason the
idiom holds: **for a stable event-time, record it as a value column, not the
projected coordinate.**

**Pitfall — the `change_ts` carrier is not a stable event-time.** The `change_ts`
carrier clamps under compaction (below-`since` times fold into `upper`), so reading
it back can **advance as `since` ticks forward** — it is stable only within `RETAIN
HISTORY`. If you need an immutable event-time, **record it as an ordinary value
column** (part of the row identity, so data-domain compaction's group-by preserves
it) rather than relying on the `change_ts` carrier. The change-records are immutable,
but the carrier coordinate is not. The engine should surface this loudly
(warning/lint), since users expect such a column not to move.

Note that the input window (`CHANGES(events AS OF AT LEAST mz_now() - '1 hour')`)
and the recorded table are **two independent dTVCs with two independent bounds**:
the window bounds the `differentiate` state; the `DELETE`/dedup bounds the recorded
output.

**Rule: a `CHANGES` over a long-lived input must be bounded**, or its
`differentiate` state grows without limit. Bound it with `AS OF AT LEAST mz_now() -
W` (a sliding lower bound, so history older than `W` falls out of scope) *and* a
temporal filter on its time carrier (`mz_now() < change_ts + W`, so the
change-records retract out of the relation). Neither alone suffices — the first
bounds what the operator retains, the second bounds what downstream sees. (`CHANGES`
over a *short-lived* input — itself already temporally filtered, like a windowed
view — still needs the `AS OF AT LEAST`.)

### Surfaces / altitudes

Same operations, two altitudes; neither adds capability.

1. **Declarative** — a regular recorded table + its progress collection, a `RECORD` writer
   feeding it, `INTEGRATE` inside ordinary MVs, and `DELETE`/`UPDATE` DML for
   bounding (shown above); the safe default. The pieces are created and dropped
   independently and coordinate via logical-time reads, not a shared transaction.
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

- **`RECORD INTO <table>`**: resume appending from the table's `upper`; no
  snapshot, no recompute. The "consumed-through-`T`" frontier *is* the output's
  `upper`. Definite-by-persistence; the impl-drift risk is acceptable because the
  rows are frozen.
- **`INTEGRATE`**: a combinator over the durable recorded table (+ reclock), so an
  MV using it recomputes and self-corrects — modulo the history caveat above
  (current contents definite).

### Dependencies / things that may change

- Builds on `CHANGES` (#36869, open PR) and the OCC timestamped-write substrate;
  the **`RECORD` writer** needs the frontier-gated timestamped write (compute
  through `X`, commit at `X+1`). `INTEGRATE` is a combinator over a definite
  collection (no new commit path), and v1 bounding is ordinary DML — both **off
  the OCC critical path**. Cross-object consistency is via logical-time reads, so
  **no cross-object atomic bundle is required**.
- Needs **a regular table** carrying the time/diff columns (no new collection kind)
  plus an **explicit, engine-written / user-read-only progress collection** (source-remap
  precedent); engine-driven read policy / compaction on outputs and on `INTEGRATE`'s
  arrangement.
- Time-based bounding reuses the `CHANGES` maintained-MV temporal-filter /
  lagged-read-hold mechanism.
- Multi-replica race-to-commit (compare-and-append, losers discard).

## Minimal Viable Prototype

Two in-flight prototypes de-risk this: the `CHANGES` PR (#36869, the
`differentiate` side, including restart-exact reproduction) and `BEGIN CONTINUAL
TRANSACTION` (Feb 2026, the imperative surface + the data-plane/control-plane
commit split). The MVP connects them: a `RECORD` writer into a regular table
(+ reclock) fed by `CHANGES(input)`, plus an `INTEGRATE`-in-a-plain-MV reading it
back, demonstrated on (a) the stream-table join / UDF enrichment with type-based
freeze (Tier 1) and (b) the `top-1` eventual upsert with input forgetting (Tier 3),
bounded by a periodic integral-preserving `DELETE`. The
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
  `INTEGRATE` (a combinator over a definite collection) and v1 bounding
  (ordinary DML) are **off this critical path**, and there is **no cross-object
  atomic bundle**, removing the multi-shard-atomicity burden a single RECORDER
  would have imposed.
- **Known sharp edges, all hit by CTs.** (a) *Self-reference reclocking* — the
  frontier-gating rule (compute through `X`, commit at `X+1`) avoids the fixpoint
  CTs never finished, but the lagged self-read still needs the CT `step_forward` /
  since-held-below-output-upper machinery (reusable, hardest-won). (b) *Object
  model* — the only new standing object is the `RECORD` writer; the recorded
  **table is a regular table** (no new collection kind), `INTEGRATE` lives inside
  ordinary MVs, and bounding is DML — each its own dataflow, fitting Materialize's
  one-object-one-collection model and avoiding CTs' single-sink-per-dataflow wall;
  no atomic multi-output orchestration is needed. (c) *Freeze-by-typing* — CTs did
  it as a persist-source renderer trick (inputs fed as insert-then-retract diffs)
  with a known gap; first-class HIR/MIR support is new optimizer work. (d) *The
  net-new storage piece* is the **explicit progress collection** (source-remap
  precedent), not a new table kind; and `INTEGRATE` is a **stateful reduce** in
  compute (accumulate + threshold), memory ∝ live output.
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
- **A dedicated `DELTA TABLE` collection kind.** Rejected in favor of a **regular
  table + an explicit progress collection + the `INTEGRATE` combinator**. Once
  `change_diff` is *data* (not the persist multiplicity), a special kind buys
  nothing: the store is an ordinary table, bounding is an ordinary `DELETE`, and the
  integral is reconstructed on read. This also dissolves the earlier
  "journal vs. delta" naming debate and removes a speculative collection kind.
- **A `FROZEN()` keyword as the primary freeze.** Rejected as primary: freeze
  falls out of TVC-vs-dTVC typing, so a keyword is redundant for the common case
  (kept only as an optional fine-grained tool).
- **Iceberg source + sink for finalization.** Complementary (archival/offload),
  not a substitute for in-Materialize recorded transforms / non-det enrichment.
- **External Postgres / Kafka loopback (status quo).** Rejected: expensive and
  operationally heavy; the point is to stay inside Materialize.
- **Distributed locking instead of OCC commit.** Rejected (latency, brittleness,
  scalability), per the OCC read-then-write design.
- **In-band progress markers instead of a separate progress collection.** Considered and
  rejected. Encoding the A→B mapping as `mz_progressed` rows in the recorded table
  (a persisted `SUBSCRIBE`) gives a single-shard CAS, but (a) makes the frontier
  *user data* — inviting tampering and forcing defensive validation on every read
  — and (b) burdens every consumer with progress-marker noise. Its single-CAS
  upside is moot: each `RECORD` write already commits the table and its progress collection
  together. (Conceptually neat — `change_diff` is changes-as-data, `mz_progressed`
  is frontier-as-data — but not worth storing that way; the explicit progress collection
  keeps the mapping referenceable without making it user-writable.)
- **A per-operator `RECLOCK` argument** (`INTEGRATE(…, RECLOCK => r)`). Rejected:
  two `INTEGRATE`s of one table could then name *different* reclocks → silent
  cross-reader inconsistency, with no legitimate use. Binding the progress collection 1:1 to the
  table makes a mismatch unrepresentable; operators reference the table and the
  reclock comes along.
- **A single merged progress collection for multi-writer tables.** Rejected: a writer knows only
  its own committed-through `X_i`, so a merged frontier would let a fast writer
  advance A-completeness past a slow one. Per-writer lanes + a read-time meet are
  correct and need no inter-writer coordination (see "multi-writer").
- **`INTEGRATE` over an arbitrary relation.** Rejected for v1 in favor of
  **`INTEGRATE(table …)`** (a bare recorded table): an arbitrary relation has no
  single, well-defined progress collection — which of its recorded inputs? a join has several —
  so its completeness would be ambiguous, exactly the failure the table-owned progress collection
  avoids. Taking the table directly keeps the progress collection lookup trivial and pushes
  application shaping (dedup/first-seen/upsert/top-k) into the `RECORD` body, where
  the non-determinism already lives. (A relaxation to a *single-recorded-table*
  relation — inline reduces whose lineage is exactly one table, reclock = that
  table's — is a possible future extension; combining sources is otherwise handled
  by recording into one table via multi-writer.)
- **`=>` named arguments for the carriers** (`CHANGES(rel, TIME => …)`). Rejected for
  now in favor of a `USING` clause: Materialize has **no named-function-argument
  support today**, and `=>` already lexes as the *map-entry* token (`MAP[k => v]`),
  so `=>` would be net-new grammar with a conflicting connotation; `USING` reuses an
  already-reserved keyword. (`=>` *is* the cross-system named-arg convention —
  Postgres/Snowflake/BigQuery — so this could be revisited if general named-arg
  support is added; co-design with #36869.)

**Prior-art / consistency notes (deliberate divergences):**

- **`DIFF` is a signed multiplicity, not an op-enum.** Comparable systems expose
  the change *kind* categorically — Snowflake Streams `METADATA$ACTION`/`ISUPDATE`,
  RisingWave `changelog_op` (1–4), Flink row-kinds `+I/-U/+U/-D`, Debezium `c/u/d`.
  Our `DIFF` is the differential-dataflow **multiplicity** (it composes, sums, and
  consolidates; an op-code cannot express multiplicity > 1), which is the right
  carrier for the `differentiate`/`integrate` calculus — but it is a deliberate,
  documented divergence from what Snowflake/Flink/RisingWave users expect.
- **`CHANGES` collides with Snowflake's `CHANGES` clause** (`… CHANGES(INFORMATION
  => …) AT(…)`), which carries time-travel + append-only-mode semantics ours do not.
  Kept (evocative, and #36869 already chose it), flagged to set expectations.

## Open questions

- **Object kinds & syntax.** *Decided* (an SQL-consistency pass grounded these
  against Materialize's grammar; see the prior-art notes in Alternatives): the store
  is a **regular table** (no
  new collection kind); the only new standing object is the `RECORD` writer,
  `CREATE RECORDER … INTO <table> AS <changelog query>` (bare query, no
  `RECORD(...)` wrapper — binds like `CREATE MATERIALIZED VIEW … AS`; `INTO` matches
  `CREATE SINK … INTO`); the table **declares its carriers and an associated progress collection** in one
  clause, `CREATE TABLE … WITH (PROGRESS, TIMESTAMP = …, DIFF = …)` (initialized to
  `(0, 0)`); so **`INTEGRATE(table)` is argument-free** — it reads the carriers and
  the progress collection from the table (nothing to mismatch), does the
  accumulate-and-threshold internally, and application shaping lives in the `RECORD`
  body. `CHANGES(rel [AS OF …] USING TIME …, DIFF …)` *names its output* carriers so
  a recorder can write them into the table's columns, with `AS OF` binding to
  `CHANGES` like `SUBSCRIBE (query) AS OF …` (not the inner query); the
  carrier-naming on `CHANGES` uses a **`USING` clause** (`=>` named args are not
  supported in MZ today and `=>` already means map-entry — `USING` reuses a reserved
  keyword) — though the exact `CHANGES` spelling is to co-design with #36869. The
  **progress collection** is engine-written / user-read-only and **owned by the
  table**; a hand-built table's timeline is a `WITH (TIMELINE = …)` option (not `IN
  DOMAIN`, which would overload `IN` / foreclose standard `CREATE DOMAIN`); bounding is `DELETE`/`UPDATE` DML; cadence is **implicit /
  frontier-driven** (`COMMIT EVERY` rejected). Carriers are **ordinary user-named
  columns** (no reserved `mz_` names). *Open:* keyword ergonomics — `RECORDER`/
  `INTEGRATE` are collision-free, but `RECORD` as a verb overlaps the composite-type
  "record" (a reason the `RECORD(...)` wrapper was dropped); should this be
  co-designed with the
  `CHANGES` PR (#36869), since that fixes the producer side and `CHANGES` is not yet
  in the tree?
- **Which domain does `mz_now()` / aging resolve in?** *Decided:* **default domain
  B (wall-clock)** so retention advances even when the input idles, with **domain A
  (event-age — "last 30 days of *events*") as an explicit opt-in**; the input
  `AS OF AT LEAST mz_now()` window inherits the same default. Pulled forward to
  Phase 1 (it gates the bounding design and the user mental model). *Open:* the
  opt-in syntax, and whether any single body needs both domains at once.
- **Where does the `RETAIN HISTORY` / "see back in time" modifier sit?** On the
  *table* it would read as system time (wrong); the better homes are the **`INTEGRATE`
  consumer** or the **recorder**. Leaning `INTEGRATE` — it is where the user wants to
  see back in time, and figuring out how much progress-collection / reclock history
  the controller must retain to satisfy all consumers is then the controller's job
  (it tracks each `INTEGRATE`'s read frontier and GCs progress entries no longer
  needed; the read policy sets the lag).
- **Commit-timestamp / frontier-advance policy.** With cadence frontier-driven
  (no `COMMIT EVERY`), at which frontiers does a writer actually commit — "every
  timestamp" vs "timestamps where a driver is non-empty" (the Decision Log's
  question)? A time-driven body (referencing `mz_now()` with no data driver) is
  driven by the clock frontier; confirm that subsumes the time-driven case. Plus
  the `INSERT … VALUES` footgun and millisecond-granularity exposure.
- **Freeze typing, diagnostics & per-value markers.** *Decided:* freeze stays the
  **default** in a `RECORD` body (bare-TVC-reference = frozen; `CHANGES` / a
  recorded changelog = tracked), but must be **diagnosable, never silent** —
  `EXPLAIN` + a plan-time `NOTICE` naming every frozen reference. (Flipping to tracked-default
  was considered and not taken: tracking a UDF / external / `now()` reference is
  often impossible or unbounded.) *Open:* whether an explicit `FROZEN`/`TRACKED`
  marker is offered for readability (redundant for the type checker); whether one
  dimension can supply a frozen value *and* anchor lifetime (via a separate
  `CHANGES(dim)` driver-action).
- **Cascade cost.** The compliance cascade is a frontier-gated `DELETE` driven by
  `CHANGES(dim)`; how is the liveness-key index (to find a deleted entity's rows)
  costed and made explicit?
- **`INTEGRATE` accumulation & timestamp-as-data stability.** *Decided:*
  `INTEGRATE` takes a **bare recorded table** and accumulates `change_diff` per row,
  emitting multiplicity `max(0, Σ)` (the threshold is the safety net; below-frontier
  `change_ts` folds into `upper`), as a stateful reduce; the progress collection is the table's.
  *Open:* confirm faithful multiset semantics (vs. existence/0-1); the `RETAIN
  HISTORY` history caveat; and the pitfall that the `change_ts` *carrier* clamps and
  can advance as `since` ticks forward (record a value column for a stable
  event-time) — *decided* to surface loudly (warning/lint), mechanism open. Also
  *open* (deferred): relaxing `INTEGRATE` to a single-recorded-table relation (inline
  reduces) instead of a bare table.
- **`DELETE` / bounding semantics.** *Decided:* because `change_diff` is data, a
  `DELETE` of changelog rows is an **ordinary persist retraction** (no "consolidate
  a `-1` against a `+1`"); safe bounding is **integral-preserving** — collapse a
  key's deltas `≤ t` into one summarizing delta (**data-domain compaction**) and
  delete the rest. *Open:* how reclamation is guaranteed/surfaced when compaction
  lags, and a guardrail for hand-written `DELETE`s that are *not*
  integral-preserving (they silently change `INTEGRATE`'s result).
- **Stream-table join: which freeze is the default?** As-of-event-time (definite,
  needs dim history) vs as-of-processing-time (the `record` route). Do we offer
  the definite temporal join as first-class sugar, given it is nearly expressible
  today?
- **Output ownership & multi-writer.** *Decided:* **multiple `RECORD` writers per
  table are allowed** via **per-writer progress lanes** `R_i: B → X_i` (no merged
  frontier, no inter-writer coordination); table A-completeness `= meet_i R_i[B_upper]`
  computed at read time. Adding a writer registers a lane `≥ upper`; dropping one
  removes its lane from the meet (**one-way door**). Multiple `INTEGRATE`s share the
  one progress collection, so they are consistent by construction. *Open:* on
  dropping the **last** writer, **freeze** the progress (stay at the last
  completeness) or **seal** (advance to the top/empty frontier — finalization for
  free)? Whether sealing is reversible; whether users may *also* hand-write such a
  table directly (mixed `RECORD` + DML provenance); `RETAIN HISTORY` interaction.
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
