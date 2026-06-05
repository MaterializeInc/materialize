# Recorders: Implementation Design

- Associated:
  - Design: `20260604_recorders.md` (the conceptual design this implements)
  - Dependency design: `20260210_incremental_occ_read_then_write.md` (the commit substrate — **unbuilt**)
  - `CHANGES` table function (#36869) — the `differentiate` primitive (assumed working)
  - Removed Continual Tasks (CT): PR #35967 (removal); implementation recoverable from its base commit `add050bf8`
  - txn-wal multi-shard atomic writes: `src/txn-wal/`
  - Multi-output catalog precedent: `20240625_source_versioning__table_from_sources.md`

## Context

This is the implementation companion to `20260604_recorders.md`. That doc
distils the feature to a calculus (`differentiate` / `integrate` / `record` /
`bound`, with `freeze`) and a surface (a `RECORDER` object writing `DELTA
TABLE`s, committing `RECORD`/`INTEGRATE`/`DELETE` actions atomically at the
commit time `T`). This doc assesses **how to build it**: the architecture, the
gating dependency, the per-crate change map, the risks, and a phased plan.

`CHANGES` (#36869) is assumed working. The single most important input to this
doc is the **removed Continual Tasks implementation**, which actually shipped a
self-referential standing dataflow writing to persist; it is the best evidence of
what is feasible, reusable, and hard. Findings below are grounded in a codebase
pass against the current tree and the recovered CT code.

## Architecture: the central fork

A recorder is a *standing dataflow* (the body) whose results must be *committed
by the control plane* at `T`. There are two ways to wire that, and the codebase
makes the choice clear.

- **Option A — compute sink (what CTs did).** The body is a compute dataflow
  whose sink writes persist directly. CTs reused the MV optimizer but swapped
  `PersistSinkConnection` for `ContinualTaskConnection` and rendered a bespoke
  async sink (`continual_task_sink`) running its own compare-and-append loop
  (`truncating_compare_and_append`) — **not** the shared `render_sink`
  (`src/compute/src/render/sinks.rs:166`). This gives standing-dataflow +
  self-reference behavior but **writes a single shard and bypasses txn-wal**
  (writing a txns-registered shard with a raw handle is UB,
  `src/txn-wal/src/lib.rs`). It therefore **cannot commit multiple outputs
  atomically** — exactly the recorder's headline requirement.

- **Option B — control-plane bundle commit (required).** The body computes
  *proposed* diffs for all actions; a control-plane loop reads them at the
  dataflow's output frontier and commits the bundle atomically at `T` via group
  commit / txn-wal. This is the design's stated intent and the only path to
  atomic multi-action commit.

**Decision: Option B.** Option A is a dead end for the multi-output feature, and
CTs prove it: their bespoke sink is precisely why they were single-output.
Recorders commit through the table-write path (txn-wal), not a compute sink.

The consequence is that the largest piece of net-new code is the **data-plane →
control-plane hand-off**: a standing dataflow whose output frontier and proposed
diffs are drained by a coordinator-side loop that commits the bundle. CT code is
reusable for the *body rendering and diff production*; it is **not** reusable for
the commit path.

## The commit substrate (gating dependency)

The recorder's core semantic — *compute diffs against the pre-commit snapshot,
then commit the whole bundle atomically at exactly `T`, fail-and-retry on
conflict* — rests on the OCC timestamped-write substrate
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
  attempt the bundle at `T`, on `UpperMismatch` re-read and retry, advance the
  oracle past `T` on success.
- The **dataflow → coordinator subscribe** that drains proposed diffs (analogous
  to the OCC doc's `CreateReadThenWriteSubscribe`).

**Implication:** recorders cannot be prototyped end-to-end without at least a
minimal timestamped group commit. This is the critical-path dependency and
should be sequenced first (Phase 0).

## Component design / code-change map

| Crate / module | Change | Size |
|---|---|---|
| `src/adapter` — group commit, `sequence_create_recorder`, the target-`T`/retry loop, the dataflow→control-plane drain | Build the timestamped group-commit extension (target `T`, fail-on-conflict, oracle advance) and the standing control loop that commits the bundle via txn-wal. Adapt the removed `sequence_create_continual_task` scaffolding. | **XL** |
| `src/compute/src/render` | Revive CT body rendering (`render/continual_task.rs`: the input/self/normal source transformers, `step_forward`, time extract/reduce). Replace the bespoke sink with *emit proposed diffs to the control plane*. Remove the one-sink-per-dataflow assumption for multi-output (or emit one dataflow per output — see Risk M2). | **L** |
| `src/sql` (parser + plan) | New DDL (`CREATE DELTA TABLE`, `CREATE RECORDER … WITH … AS …`). `freeze`-by-typing as a planner concept (bare TVC ref vs `CHANGES`/`DELTA TABLE`). The lint rule: freeze / processing-time write legal only inside a recorder. Optimizer support for the asymmetric/frozen join if lifted above LIR. | **L** |
| `src/catalog` + `src/catalog-protos` | New item kinds (`RECORDER`, `DELTA TABLE`); one item owning / orchestrating multiple output collections + dependency edges; a new durable-catalog migration version. | **M** |
| `src/storage-types` + persist schema | `DELTA TABLE` collection kind with reserved `mz_timestamp`/`mz_diff` columns; write path embedding logical ts/diff while writing at system `T`; txns registration of recorder outputs. | **M** |
| `src/compute-client` (`as_of_selection.rs`, `controller/instance.rs`) | Self-reference read-hold (since strictly below output upper). **These files already carry the write-only-collection (CT) special-casing** (`as_of_selection.rs:460`, `controller/instance.rs:1530,1776`) — reuse, do not rebuild. | **M** |
| `src/adapter/src/coord/read_policy.rs` + compaction | Engine-owned read policy / compaction advancement on recorder outputs so retractions consolidate and `since` advances; `RETAIN HISTORY` interaction. | **M** |

## Risk register (ranked)

### HIGH — H1: the commit substrate is unbuilt, and multi-action atomicity rides entirely on it
The headline feature depends on a substrate that exists only as a design doc
(see above). The storage-level conditional write (`commit_at` → `UpperMismatch`)
is there; the adapter-level target-`T`/retry loop and the dataflow hand-off are
not. **CTs did not solve this** — they bypassed it with a single-shard bespoke
sink. Mitigation: build the OCC substrate first (Phase 0), shared with the OCC
read-then-write effort.

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

### HIGH — H3: object/catalog model — one item, N outputs and dataflows
A recorder is one statement with multiple outputs (`DELTA TABLE`s, `INTEGRATE`
views) and likely multiple dataflows. Precedents conflict:
- **CTs: one item → one `GlobalId` → one output.** `sequence_create_continual_task`
  allocated a single `(item_id, global_id)`; `ContinualTaskCtx::new` hard-asserts
  one CT sink per dataflow and a single input. **This cannot represent a
  multi-output recorder.**
- **Tables-from-sources** (`20240625_…`) deliberately models **each output as its
  own top-level catalog item**, with an orchestrator referencing them. This is
  the better fit: `DELTA TABLE`s and `INTEGRATE` views as independent items, the
  `RECORDER` as an orchestrator item holding dependency edges and owning the
  dataflow(s).
Controllers key dataflows by `GlobalId` (one `DataflowDescription` → one primary
export), so multi-output is either multiple dataflows (multiple `as_of`s to keep
mutually consistent for an atomic commit) or a new multi-sink dataflow shape.
Atomic-commit-at-one-`T` argues for a **single dataflow with multiple
proposed-diff streams feeding one commit** — a new compute shape. **CTs hit this
wall (single-output).** This is new catalog + controller machinery.

### MED — M1: freeze-by-typing needs first-class optimizer support
The design needs `JOIN dim` (bare TVC) rendered as "looked up once per
driver-delta, not maintained, frozen," while `CHANGES`/`DELTA TABLE` sides flow
diffs. CTs encoded this **in the renderer, not the optimizer**: a source
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

### MED — M2: `DELTA TABLE` as a typed collection
Persist stores `(SourceData, (), Timestamp, Diff)` — ts/diff are *physical*
coordinates, not row data. A `DELTA TABLE` wants them as **logical columns**,
where the row's `mz_timestamp` value (event time) need not equal the shard
write-ts `t'` (system time) — two timestamps on one timeline. This is a
new collection kind in the catalog and `storage-types` plus planner support for
the implicit columns and a write path that materializes the embedded ts/diff
(`20250707_persist_schemas.md`). Not enormous, but genuinely new; CTs never had
it (CT outputs were plain `Table`s).

### MED — M4: output frontier advancement (data vs. progress)
Row `mz_timestamp` is *data* and does not set an output's `upper`; the `upper`
must advance with the recorder's **input progress** (the meet of input frontiers,
lagged by any `AS OF AT LEAST` window), like an MV. Two requirements: (a) advance
the `upper` past `t` only after integrating all input through `t`, so placement
at `mz_timestamp` is always `≥ upper` and **no clamp is needed** (a well-formed
input frontier means late data below the frontier cannot occur — do **not**
clamp/"logically compact," which silently splits retract/insert pairs); (b)
**idle liveness** — outputs must keep
advancing their `upper` even with no data, or downstream reads of an idle output
stall. The mechanism exists: `append_table(write_ts, advance_to, appends)`
(`src/storage-controller/src/lib.rs:2082`) advances the `upper` via `advance_to`
independently of `appends`, so a frontier-only commit is a no-new-machinery
operation. The recorder's commit loop must therefore **downgrade output `upper`s
on a clock** (frontier-only commits) decoupled from data writes — the frontier
face of the commit-cadence question. This is straightforward given `advance_to`,
but must be explicitly wired into the Phase-0 commit loop (it is easy to forget,
which is how the conceptual design originally missed it).

### MED — M3: dual frontier (event-time placement vs. system-time aging) + compaction
`INTEGRATE` places deltas at their event time and advances its `upper` with input
event-time completeness; it relies on the engine owning the read policy /
compaction so a `-1` consolidates with its `+1` and `since` advances (design
"Bounding growth" invariants). But **time-based aging cannot ride the event-time
frontier** — that frontier stalls when the input goes quiet, so a retention
`DELETE`/window would never fire once events pause. Aging needs a **separate
system/wall-clock frontier** on the output, advanced on a clock independent of
input, distinct from the event-time frontier governing placement/queries (a
dual-frontier model). `mz_now()` inside a body resolves to processing time. Read
policies live in
`src/adapter/src/coord/read_policy.rs`; today they are user/MV-driven. Making a
recorder *own* and continuously advance them is plausible but interacts sharply
with `RETAIN HISTORY` (the design's history caveat) and with the "exact-row
retraction or it never consolidates" invariant, which is **unenforceable** for
hand-written `DELETE`s. Feasible, with a sharp user-facing edge.

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
  — bypasses txn-wal, single-shard; replace with control-plane bundle commit.
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
- **Phase 1 — single-output recorder.** Revive CT body rendering + read-hold +
  restart machinery; emit proposed diffs to Phase 0's commit path instead of the
  bespoke sink. One `RECORD` into one `DELTA TABLE`. Validates freeze (renderer
  form), self-read, restart.
- **Phase 2 — multi-output + atomic bundle.** Catalog model (outputs as
  independent items, recorder as orchestrator); single dataflow with multiple
  proposed-diff streams committed atomically at one `T`; add `INTEGRATE` and
  `DELETE` actions. Tears out the single-sink assumption.
- **Phase 3 — freeze as first-class.** Lift `freeze`/asymmetric join into
  HIR/MIR (remove the persist-source-only hack and `NoIndexCatalog`); the lint
  rule; the definite as-of-event-time temporal join (possibly `STREAM JOIN`).
- **Phase 4 — bounding polish.** Engine-owned read policy/compaction; retention
  syntax; `RETAIN HISTORY` interaction; compaction-lag observability.

## Open implementation questions

1. **The hand-off mechanism.** Is it a per-recorder internal subscribe draining
   proposed diffs? At what frontier does the control loop decide `T` is ready?
2. **Build OCC first vs co-develop?** It is the critical-path dependency.
3. **Multi-output = multiple dataflows or one multi-sink dataflow?** Atomic
   commit-at-one-`T` argues for a single dataflow with multiple proposed-diff
   streams (a new compute shape) over N dataflows with N `as_of`s.
4. **Does the relaxed rule actually let us drop machinery, or only the fixpoint
   sub-scope?** (H2 says: only the latter — the lagged self-read stays.)
5. **`DELETE` consolidation** — invariant unenforceable for hand-written
   `DELETE`s; what is the guardrail when compaction lags and space is not
   reclaimed?
6. **`mz_timestamp` row-value vs shard write-ts divergence** — persist-schema and
   read semantics of a row whose embedded ts ≠ its physical commit ts.
7. **Bitemporality via a recorded reclock (supersedes "reclock to `T`").** A fact
   definite at event time `t` is recorded at system time `t' ≥ t`. Rather than
   collapse outputs to `t'` (which loses consistency and stable history), record
   a **durable reclock** `t → t'` (event-time completeness vs. system-time write
   frontier) and use it to drive the integrated collection's frontier —
   the **source reclock/remap pattern** (`src/storage*` reclock machinery) applied
   to a recorder output; reuse it. Consequences for the implementation:
   - The `RECORD` step (writing the `DELTA TABLE` + the reclock) is the **only**
     non-deterministic boundary; the `DELTA TABLE` is authoritative (the
     optimizer must **not** treat it as recomputable from the original inputs).
   - `INTEGRATE` and everything downstream **are** definite functions of the
     `DELTA TABLE` + reclock, placed at event time (no clamp; frontier = input
     completeness) — so the optimizer *may* treat them as definite/recomputable
     over the recorded data. Event time `t` and system time `t'` are two
     timestamps on the **same** timeline (`t' ≥ t`), so a recorder output is a
     lagged system-timeline collection — cross-table joins/transactions work
     (no separate timeline).
   - New component: a durable **reclock/remap collection per recorder** relating
     the `DELTA TABLE`'s system-time write frontier to its event-time
     completeness (so the integrator can advance its event-time `upper` on
     restart). With no late-data clamping this relates two *monotone* frontiers.
   - Self-referential prune (a `DELETE` reading the recorder's own output) must
     read the **system-time** (real-time) recorded state, not the lagged
     event-time frontier, or it converges only at the processing-skew rate and
     the dTVC grows during catch-up (the "one-tick" claim holds only on system
     time).
   - **Compliance erasure vs. stable history**: true GDPR erasure = advancing
     `since` to physically drop history, which forfeits `AS OF`/replay in the
     erased range. It is mutually exclusive with stable history there; scope it
     as "definiteness holds forward of the advanced `since`."
