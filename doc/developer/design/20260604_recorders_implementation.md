# Recorders: Implementation Design

- Associated:
  - Design: `20260604_recorders.md` (the conceptual design this implements)
  - Dependency design: `20260210_incremental_occ_read_then_write.md` (the commit substrate — **unbuilt**)
  - `CHANGES` table function (#36869) — the `differentiate` primitive (assumed working)
  - Removed Continual Tasks (CT): PR #35967 (removal); implementation recoverable from its base commit `add050bf8`
  - txn-wal multi-shard atomic writes: `src/txn-wal/`
  - Multi-output catalog precedent: `20240625_source_versioning__table_from_sources.md`
  - Reclocking framework: `20210714_reclocking.md` (converting between time gauges, incl. out of the system timeline — the A↔B reclock)

## Context

This is the implementation companion to `20260604_recorders.md`. That doc
distils the feature to a calculus (`differentiate` / `integrate` / `record` /
`bound`, with `freeze`) realized over **regular tables + an explicit reclock
object** as **one new standing-write object** (the `RECORD` writer), `INTEGRATE`
as a **SQL combinator** (accumulate-and-threshold) usable in ordinary materialized
views, and ordinary **DML** for bounding — **no new collection kind**, no atomic
multi-output bundle; consistency via logical-time reads. This doc assesses **how
to build it**: the architecture, the gating dependency, the per-crate change map,
the risks, and a phased plan.

`CHANGES` (#36869) is assumed working. The single most important input to this
doc is the **removed Continual Tasks implementation**, which actually shipped a
self-referential standing dataflow writing to persist; it is the best evidence of
what is feasible, reusable, and hard. Findings below are grounded in a codebase
pass against the current tree and the recovered CT code.

## Architecture: the central fork

A recorder is a *standing dataflow* (the body) whose results must be *committed
by the control plane* at `T`. There are two ways to wire that, and the codebase
makes the choice clear.

The only new standing object is the `RECORD` writer (its own dataflow + a
frontier-gated commit). `INTEGRATE` is a combinator (a stateful reduce) inside
ordinary MVs (no new commit path) and bounding is ordinary DML; there is **no atomic multi-output
bundle** (cross-object consistency is via logical-time reads). The question is how
the `RECORD` writer's body commits.

- **Option A — compute sink (what CTs did).** The body is a compute dataflow
  whose sink writes persist directly. CTs reused the MV optimizer but swapped
  `PersistSinkConnection` for `ContinualTaskConnection` and rendered a bespoke
  async sink (`continual_task_sink`) running its own compare-and-append loop
  (`truncating_compare_and_append`) — **not** the shared `render_sink`
  (`src/compute/src/render/sinks.rs:166`). It bypasses txn-wal (writing a
  txns-registered shard with a raw handle is UB, `src/txn-wal/src/lib.rs`), so it
  cannot even commit the data table together with its reclock shard, and gives no
  control-plane "commit at exactly `T`, retry on conflict" needed for OCC
  frontier-gating.

- **Option B — control-plane timestamped commit (chosen).** The body computes
  *proposed* diffs; a control-plane loop reads them at the dataflow's output
  frontier and commits at the frontier-gated `T` via group commit / txn-wal
  (committing the object's data shard and its reclock together). This is the OCC
  timestamped write, per object — not a cross-object bundle.

**Decision: Option B, for the `RECORD` writer.** The writer commits through the
table-write path (txn-wal), not a compute sink — needed even for one object (to
commit data+reclock atomically and to support frontier-gated retry). The largest
piece of net-new code is the **data-plane → control-plane hand-off**: a standing
dataflow whose output frontier and proposed diffs are drained by a coordinator
loop that commits them. CT code is reusable for the *body rendering and diff
production*; it is **not** reusable for the commit path.

## The commit substrate (gating dependency)

Each object's core semantic — *compute diffs through frontier `X`, commit at
`X+1` (its data + reclock together), fail-and-retry on conflict* — rests on the
OCC timestamped-write substrate
(`20260210_incremental_occ_read_then_write.md`). **That substrate is unbuilt**:
its symbols (`AttemptTimestampedWrite`, `CreateReadThenWriteSubscribe`,
`enable_adapter_frontend_occ_read_then_write`) have zero hits in the tree.

What already exists and can be built on:

- **Atomic multi-shard write at an externally supplied `T`.**
  `Coordinator::group_commit` (`src/adapter/src/coord/appends.rs:344`) collects
  per-`GlobalId` writes and calls `append_table(write_ts, advance_to, appends)`
  (`src/storage-controller/src/lib.rs:2082`), which commits via txn-wal
  (`commit_at(write_ts)`, `src/txn-wal/src/txns.rs`). txn-wal **is** "atomic
  multi-shard persist writes," and `commit_at` already **fails on
  `UpperMismatch`** — i.e. the conditional, fail-on-conflict primitive exists at
  the storage layer.

What is missing (must be built, ideally as the OCC substrate so other features
share it):

- Group commit today **allocates** the write timestamp from the oracle
  (`get_local_write_ts`, `appends.rs:462`); there is no "commit at *this* `T` or
  fail" adapter path. `WriteLocks`/`GroupCommitWriteLocks`
  (`src/session`, used in `appends.rs`) are the in-process pessimistic locks the
  OCC doc proposes to replace.
- The **target-`T`/retry loop**: select `T` from the dataflow's output frontier,
  attempt the object's write (its data + reclock) at `T`, on `UpperMismatch`
  re-read and retry, advance the oracle past `T` on success.
- The **dataflow → coordinator subscribe** that drains proposed diffs (analogous
  to the OCC doc's `CreateReadThenWriteSubscribe`).

**Implication:** recorders cannot be prototyped end-to-end without at least a
minimal timestamped group commit. This is the critical-path dependency and
should be sequenced first (Phase 0).

## Component design / code-change map

| Crate / module | Change | Size |
|---|---|---|
| `src/adapter` — group commit, per-object sequencing, the target-`T`/retry loop, the dataflow→control-plane drain | Build the timestamped group-commit extension (target `T`, fail-on-conflict, oracle advance) and the per-object control loop that commits its data + reclock via txn-wal. Adapt the removed `sequence_create_continual_task` scaffolding. | **XL** |
| `src/compute/src/render` | Revive CT body rendering (`render/continual_task.rs`: the input/self/normal source transformers, `step_forward`, time extract/reduce). Replace the bespoke sink with *emit proposed diffs to the control plane*. **Render `INTEGRATE` as a stateful reduce** (accumulate `change_diff` per row, threshold `max(0, Σ)`, place by `change_ts`) — memory ∝ live output. Each object is its own dataflow (one primary export), so the CT one-sink-per-dataflow shape is kept, not torn out. | **L** |
| `src/sql` (parser + plan) | New DDL: `CREATE RECORDER … INTO <table> AS <query>` (bare query, no `RECORD(...)` wrapper — binds like MV's `AS <query>`; `INTO` reuses the SINK destination idiom; the store is a **regular `CREATE TABLE`**, no new table DDL); the table's reclock is auto-created and optionally named `EXPOSE RECLOCK AS <name>` (the `EXPOSE PROGRESS AS` precedent); a hand-built table's `WITH (TIMELINE = …)` (not `IN DOMAIN`). Carriers use a **`USING TIME …, DIFF …` clause** on `CHANGES`/`INTEGRATE` — **no named-arg (`=>`) parser work needed** (`USING` is already reserved); `CHANGES`'s `AS OF` parses like `SUBSCRIBE (query) AS OF …` (attaches to the operator, not an inner SELECT). `INTEGRATE(<table> USING …)` takes a **bare recorded table** (not a relation) and does the accumulate-and-threshold internally — so the reclock is looked up **from the table** (no per-operator arg, no lineage analysis). `DELETE`/`UPDATE` planning (ordinary retraction; integral-preserving data-domain compaction = `clamp + GROUP BY/SUM`). `freeze`-by-typing as a planner concept (bare TVC ref vs `CHANGES` / recorded changelog), legal only in a `RECORD` body, with the `EXPLAIN`/`NOTICE` diagnostics. Note `RECORD` as a verb overlaps the composite-type "record" — dropping the wrapper avoids it. Optimizer support for the asymmetric/frozen join if lifted above LIR. | **L** |
| `src/catalog` + `src/catalog-protos` | New item kinds: the **explicit reclock object** (engine-written / user-read-only, source-remap precedent) and the `RECORDER` writer; the recorded store is a **regular `TABLE`** (no new kind). Dependency edges; reclock domain binding; a durable-catalog migration version. No multi-output orchestration (`INTEGRATE` rides on MVs; bounding is DML). | **M** |
| `src/storage-types` + persist schema | **No new collection kind** — the recorded store is a regular table whose `change_ts`/`change_diff` are ordinary columns. Net-new: the **reclock object's** shard + its txns registration, and committing `(data, reclock)` in one group commit. (`INTEGRATE`'s accumulation is a compute reduce, not a storage concern.) | **S–M** |
| `src/compute-client` (`as_of_selection.rs`, `controller/instance.rs`) | Self-reference read-hold (since strictly below output upper). **These files already carry the write-only-collection (CT) special-casing** (`as_of_selection.rs:460`, `controller/instance.rs:1530,1776`) — reuse, do not rebuild. | **M** |
| `src/adapter/src/coord/read_policy.rs` + compaction | Engine-owned read policy / compaction advancement on recorder outputs so retractions consolidate and `since` advances; `RETAIN HISTORY` interaction. | **M** |

## Risk register (ranked)

### HIGH — H1: the `RECORD` writer's commit substrate (OCC timestamped write) is unbuilt
The `RECORD` writer's frontier-gated commit (compute through `X`, commit at `X+1`,
retry on conflict — committing its data shard and reclock together) depends on a
substrate that exists only as a design doc (see above). The storage-level
conditional write (`commit_at` → `UpperMismatch`) is there; the adapter-level
target-`T`/retry loop and the dataflow hand-off are not. **CTs did not solve
this** — they bypassed it with a bespoke sink. Mitigation: build the OCC substrate
first (Phase 0), shared with the OCC read-then-write effort. (Note: `INTEGRATE`
and v1 bounding DML are **off this critical path**, and there is **no**
cross-object atomic bundle to build — consistency is via logical-time reads.)

### HIGH — H2: self-reference reclocking — partly escaped, not fully
A recorder reads its own outputs (`rel2`/`rel3` read `enriched`; `RECORD`/
`DELETE` write it). The CT module documented this as the hardest part: contents
of an output at `T` are knowable only through `T-1`, requiring a timely feedback
loop through persist with a `step_forward` operator (present `T-1` at `T`), a
self-reference source transform, and a since held *strictly below* the output
upper (the special-casing still in `compute-client/src/as_of_selection.rs:460`
and `controller/instance.rs:1530,1776`). **The CT self-referential fixpoint was
never finished** (`TODO(ct3)` markers in `render/continual_task.rs`). The
recorder's "no intra-commit fixpoint / `T+1` visibility" rule genuinely removes
the *fixpoint*, but **not** the lagged self-read: reading own output at the
pre-commit frontier while writing at `T` still needs the `step_forward` /
read-hold machinery. Mitigation: reuse the (working) read-hold + step-forward
code; rely on the relaxed rule to avoid the (unfinished) fixpoint sub-scope.

### MED — H3: object/catalog model — independent objects (de-risked by the model)
Each piece is its own object: the **explicit reclock object** and the `RECORD`
writer are the new kinds (the store is a **regular table**); `INTEGRATE` rides on
ordinary MVs and bounding is DML.
CTs were structurally single-output
(`sequence_create_continual_task` allocated a single `(item_id, global_id)`;
`ContinualTaskCtx::new` hard-asserts one sink per dataflow and a single input) —
but with **separate objects, each its own dataflow with one primary export, that
is no longer a wall**: it matches how controllers already key dataflows by
`GlobalId`, and the tables-from-sources precedent (`20240625_…`, each output a
top-level catalog item). The earlier worry — a single multi-sink dataflow
committing N outputs atomically — **is moot**: there is no atomic multi-output
commit. What remains is ordinary new-object-kind catalog work (the reclock object,
`RECORDER`) plus per-object dataflow wiring — no multi-output orchestration.
(Demote from High to Med given the model change.)

### MED — M1: freeze-by-typing needs first-class optimizer support
The design needs `JOIN dim` (bare TVC) rendered as "looked up once per
driver-delta, not maintained, frozen," while `CHANGES` / recorded-changelog sides
flow diffs. CTs encoded this **in the renderer, not the optimizer**: a source
transformer classified each persist source as inserts-input (keep positive
diffs, retract at `T+1` via a `flat_map` emitting `[(row,ts,+d),(row,ts+1,-d)]`),
self-reference, or normal-reference; a normal differential join against a
snapshotted reference then produced output only at the input's tick. This is
reusable but (a) it lives at the LIR/persist-source layer and needed a
`NoIndexCatalog` hack because the renderer only transforms `persist_source`s, not
arrangements (`TODO(ct3)` flags a real correctness gap), and (b) it needed
`force_non_monotonic` because the insert/retract trick breaks monotonicity.
Making `freeze` a first-class HIR/MIR construct (so it survives optimization and
works over arrangements) is **net-new optimizer work** in `src/sql/src/plan` +
`src/expr` + `src/compute/src/render`.

### MED — M2: the explicit reclock object (the net-new storage piece)
There is **no new collection kind**: the recorded store is a regular table whose
`change_ts` / `change_diff` are *ordinary columns*. Persist already stores
`(SourceData, (), Timestamp, Diff)`; that the row's embedded `change_ts` (data) ≠
its physical commit ts is fine — it is just a column value, exactly as `CHANGES`
already produces (#36869). The net-new storage piece is instead the **explicit
reclock object**: an engine-written, **user-read-only**, relation-valued mapping
(the source-remap / progress-subsource precedent) **owned by the table** —
auto-created when the table first becomes a recording target, written by `RECORD`
*together with* the data in one group commit (a per-writer lane). It is associated
with the *table*, not the recorder, so `INTEGRATE(table …)` resolves it by table
identity (no lineage analysis). This is a smaller, more precedented surface than a
bespoke collection kind — the risk shifts from "a new table type with implicit
columns" to "a table-associated read-only, engine-written object kind + the
two-shard commit."

### MED — M4: output frontier advancement (reclock-driven, domain A)
The `change_ts` column is *data* and does not set an output's `upper`. `INTEGRATE`
is a stateful reduce: it **accumulates `change_diff` per row** up to each `change_ts`
(below-frontier times fold into the current `upper`) and **thresholds at zero**
(emit multiplicity `max(0, Σ)`), and **drives `v`'s frontier into domain A via the
reclock** (translating the table's domain-B write frontier into domain-A
completeness). The recorder must record the A→B mapping (`RECORD`) for the
integrator to advance the A-frontier and to recover it on restart. **Idle
liveness:** outputs must keep advancing their `upper` even with no data;
`append_table(write_ts, advance_to, appends)`
(`src/storage-controller/src/lib.rs:2082`) advances the `upper` via `advance_to`
independently of `appends`, so a frontier-only commit is a no-new-machinery
operation; wire it into the Phase-0 commit loop (easy to forget). **Multi-writer:**
each writer commits its own **reclock lane** `R_i: B → X_i` atomically with its
deltas (independent two-shard `group_commit`, no inter-writer coordination); the
table's A-`upper` is `meet_i R_i[B_upper]`, computed **at read time** by the
integrator (a merged frontier can't work — a fast writer would outrun a slow one).
Lane register on add (`≥ upper`), leave-the-meet on drop (one-way); **drop all →
empty meet = top → the table seals** (finalization). **Decided:** the aging/`mz_now()`
domain defaults to B (wall-clock), A (event-age) opt-in (conceptual doc), pulled to
Phase 1.

### MED — M3: engine-owned compaction / read policy
Since `change_diff` is *data*, there is **no `-1`-cancels-`+1` consolidation**: a
`DELETE` of changelog rows is an ordinary retraction that reclaims once `since`
advances, and `INTEGRATE`'s reduce arrangement compacts like any reduce. The engine
must own the read policy / compaction on both the table and that arrangement.
Time-based aging via a temporal filter (`mz_now() < change_ts + W`) uses the
**decided default of wall-clock (domain B)** — event-age (domain A) is opt-in (see
M4 / conceptual doc) — so it needs a system-time reference by default. Read policies
live in `src/adapter/src/coord/read_policy.rs`; today they are user/MV-driven.
Making a recorder *own* and continuously advance them is plausible but interacts
sharply with `RETAIN HISTORY` (the history/queryability caveat) and with the
**integral-preserving-`DELETE`** requirement. Data-domain compaction is a *reduce*
— `clamp change_ts := max(change_ts, t)` then `GROUP BY <data cols, clamped ts>,
SUM(change_diff)` (a bare in-place clamp is wrong: identical clamped `SourceData`
merges to persist multiplicity 2 while the data column still reads `+1`) — and it is
**reclock-free, recorder-free, idempotent for fixed `t`, monotone in `t`**, so a
standing pruner can re-run it. A hand-written `DELETE` that is *not*
integral-preserving silently changes `INTEGRATE`'s result, and that is
**unenforceable**. Feasible, with a sharp user-facing edge.

### LOW–MED — L1: planning / parser / sequencing / restart
New DDL is mechanical (CTs had `ContinualTaskStmt`, `CreateContinualTaskSugar`,
`CreateContinualTaskPlan` — recoverable as a template). Self-reference bootstrap
is solved precedent: a catalog placeholder lets the optimizer resolve the
not-yet-created self-id, then `update_create_sql` rewrites self-references
post-allocation. Restart: `RECORD` resumes from output `upper` with
snapshot-exclude (CTs' `set_initial_as_of` + "snapshot-exclude on re-render" — an
explicit perf optimization making rehydration independent of input size);
`INTEGRATE` recomputes as an MV. Most clearly feasible bucket — largely "un-revert
and adapt CT scaffolding."

## Salvage from the removed Continual Tasks implementation

**Reuse:**
- Self-reference bootstrap (catalog placeholder + `update_create_sql` rewrite).
- Input-as-diffs via insert-then-retract — this *is* the `freeze` mechanism and
  the dTVC framing; the design's "freeze is typing" is its principled form.
- Restart-cheap rehydration (snapshot-exclude, resume from output `upper`,
  `initial_as_of`).
- The write-only-collection read-hold special-casing still live in
  `compute-client` (`as_of_selection.rs:460`, `controller/instance.rs:1530,1776`).
- The sink `process()` state machine ("write only at times an input changed,
  else advance the upper") — relevant to the commit-cadence open question.

**Avoid / rebuild:**
- The bespoke compute sink (`continual_task_sink`, `truncating_compare_and_append`)
  — bypasses txn-wal; replace with the per-object control-plane timestamped commit.
- The single-output, single-input, single-sink assumptions (hard `assert_eq!`s).
- The unfinished self-referential fixpoint (`TODO(ct3)`).
- The `NoIndexCatalog` / persist-source-only freeze hack — lift `freeze` into
  HIR/MIR instead.

CTs were removed for *lack of consensus and incompleteness*, not unsoundness
(`v81_to_v82.rs`, PR #35967). The complexity that stalled them was overwhelmingly
the self-reference/reclocking and the bespoke-sink path — exactly what the
recorder design tries to sidestep (and, per H2, only partly does).

## Phased delivery plan

- **Phase 0 — commit substrate (gating).** Build the OCC timestamped group commit
  (target `T`, fail-on-conflict via `commit_at`/`UpperMismatch`, oracle advance)
  and the dataflow→coordinator drain. Shared with the OCC read-then-write effort.
- **Phase 1 — one `RECORD` object.** Revive CT body rendering + read-hold +
  restart machinery; emit proposed diffs to Phase 0's commit path instead of the
  bespoke sink. One `RECORD` into one regular table **+ its explicit reclock
  object**. Validates freeze (renderer form), self-read, restart, and the
  data+reclock two-shard commit.
- **Phase 2 — `INTEGRATE` combinator + mutable-table bounding.** `INTEGRATE(<table>
  USING TIME …, DIFF …)` over a **bare recorded table** as a **stateful reduce**
  (accumulate + threshold) inside a plain MV, reclock resolved by table identity (no
  per-operator arg, no lineage analysis); ordinary
  `DELETE`/`UPDATE` bounding (integral-preserving data-domain compaction = the
  `clamp + GROUP BY/SUM` reduce). No atomic bundle, no multi-sink dataflow;
  consistency via logical-time reads. Add the reclock object + its domain binding
  (inherited / explicit) and **multi-writer via per-writer lanes** (meet at read
  time; seal on drop-all).
- **Phase 3 — freeze as first-class.** Lift `freeze`/asymmetric join into
  HIR/MIR (remove the persist-source-only hack and `NoIndexCatalog`); the lint
  rule; the definite as-of-event-time temporal join (possibly `STREAM JOIN`).
- **Phase 4 — bounding polish.** Engine-owned read policy/compaction; retention
  syntax; `RETAIN HISTORY` interaction; compaction-lag observability; the optional
  standing frontier-gated pruner; data-domain compaction (deferred).

## Open implementation questions

1. **The hand-off mechanism.** Is it a per-recorder internal subscribe draining
   proposed diffs? At what frontier does the control loop decide `T` is ready?
2. **Build OCC first vs co-develop?** It is the critical-path dependency.
3. **Object granularity confirmed.** One new standing object (the `RECORD`
   writer), `INTEGRATE` on MVs, bounding via DML — one dataflow each, no atomic
   multi-output bundle, no multi-sink dataflow. (Remaining: keyword/object-kind
   ergonomics, deferred.)
4. **Does the relaxed rule actually let us drop machinery, or only the fixpoint
   sub-scope?** (H2 says: only the latter — the lagged self-read stays.)
5. **`DELETE` / bounding.** Decided: because `change_diff` is data, a `DELETE` of
   changelog rows is an ordinary retraction; safe bounding is integral-preserving
   (data-domain compaction: collapse a key's deltas `≤ t` into one and delete the
   rest). Open: a guardrail for hand-written `DELETE`s that are *not*
   integral-preserving (they silently change `INTEGRATE`'s result), and how
   reclamation is surfaced when compaction lags.
6. **`change_ts` row-value vs shard write-ts divergence** — the embedded time is an
   ordinary column value that need not equal the physical commit ts. This is just
   data (as `CHANGES` already emits), so it is a read-semantics note, not a new
   schema concern.
7. **Two time domains + reclock (A → B → A).** The full model is in the conceptual
   doc ("Time domains and reclocking", "determinism boundary") and is not restated
   here. Implementation-relevant consequences:
   - **Optimizer barrier (hard invariant).** The recorded table is authoritative and
     must **not** be treated as recomputable from the `RECORD` body's inputs (that
     would re-sample frozen / non-deterministic values); `INTEGRATE` and downstream
     **are** definite over the table + reclock and *may* be recomputed. The
     planner/optimizer must enforce this barrier.
   - **Reclock is an explicit, engine-written / user-read-only object owned by the
     table** (auto-created when it first becomes a recording target; no per-operator
     `RECLOCK` arg — that would let two readers disagree). `B` is the table's single
     shard frontier, so there is exactly one reclock per table; **multiple writers commit per-writer lanes** (each its own
     two-shard commit), and the table's A-`upper` is `meet_i R_i[B_upper]` at read
     time (a merged frontier can't work). Drop-all seals the table. All `INTEGRATE`s
     share the one reclock, so they are consistent by construction. Recovered on
     restart. Exactly-once is the per-commit CAS, not a determinism concern. (In-band
     `mz_progressed` markers: considered and rejected.)
   - **`mz_now()`/aging domain decided B (wall-clock) by default**, A (event-age)
     opt-in (M3/M4).
   - **Compliance erasure = `DELETE` of the rows + advancing `since`** (forfeits
     `AS OF`/replay forward of the new `since`); a cascade `DELETE` alone is not GDPR
     erasure.
   - **`change_ts` row-value vs commit-ts divergence** is just data (Q6); no special
     schema.
