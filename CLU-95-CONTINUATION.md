# CLU-95 — continuation notes

> Status: **investigation / understanding phase, no fix written yet.** A repro
> harness exists at `test/clu-95-repro/mzcompose.py` but is NOT yet confirmed to
> reproduce. This doc is the handoff: theory, evidence, code map, open
> questions, repro plan, and candidate fixes.

## The bug

Panic during coordinator bootstrap:

```
thread 'coordinator' panicked at src/compute-client/src/as_of_selection.rs:407:25:
failed to apply hard as-of constraint
(id=u732, bounds=[[1779500014000] .. [1779500014000]],
 constraint=Constraint { type_: Hard, bound_type: Upper,
   frontier: Antichain { elements: [1779499972674] },
   reason: "storage export u732 write frontier" })
```

`u732` is a **user materialized view**. Decoding:
- **lower bound = 1779500014000** comes from `apply_upstream_storage_constraints`
  (`src/compute-client/src/as_of_selection.rs:428`): it is the **read frontier
  (`since`) of one of u732's storage inputs**.
- **upper hard constraint = 1779499972674 = `step_back(write_frontier)`** of
  u732's own output (`apply_downstream_storage_constraints:475`). The constraint
  frontier is the *stepped-back* value, which `apply_downstream_storage_constraints`
  only uses when `collection_empty` is **false** (`:482-488`). So `u732.since <
  u732.upper` — u732 is a **real, durably-written** collection, not empty/fresh.

So the pathology is: **a storage input's read frontier (`since` = 1779500014000)
has advanced ~41s PAST the dependent MV's durable write frontier (`upper` =
1779499972675).** Lower > upper ⟹ the hard "storage export write frontier"
constraint can't apply ⟹ `soft_panic_or_log!` (soft-asserts are ON in CI/test
images, so it's a hard crash; in release it logs and limps with a best-effort
as-of, but that would leave a skipped-times gap = latent correctness issue).

### Where it was seen
- Linear CLU-95: `release-qualification` → "Long Workload Replay (100% initial
  data)", build 1248. That step is `workload-replay run benchmark
  --compare-against common-ancestor` (`ci/release-qualification/pipeline.template.yml:214`).
  The benchmark runs an old then a new mz over shared volumes/restart; the
  `Materialized` default `sanity_restart=True` does an ungraceful `kill`+`up`
  (`misc/python/materialize/mzcompose/composition.py:sanity_restart_mz`,
  ~line 1329), i.e. a restart of the **same** env with persisted state.
- Same panic family across history: database-issues #8718, #8753, #8631 (CT/MV),
  and crucially **#8836** ("0dt: bootstrapping can get confused by concurrent
  DROP") — that one was worked around by `prune_dropped_collections`
  (read-only-only) for the *dropped/empty-input* variant. CLU-95 is the
  **non-empty** variant (input compacted forward but still finite).
- #11273 / PR **#35933** fixed the *new builtin MV* variant: bootstrap now
  registers storage collections in topological layers and bumps a derived
  collection's `since` to the join of its inputs' sinces — but **only for
  `is_system()` collections** (`src/adapter/src/coord.rs:3099-3116`). u732 is a
  user MV, so it is not covered.

## Theory (root cause)

The intended invariant is `input.since <= step_back(dependent_MV.durable_upper)`.

**How it is maintained at runtime (traced, holds up):**
1. Both MV sink impls report the controller-visible write frontier from a
   persist `write_handle` watcher = the **durable** shard upper
   (`src/compute/src/sink/materialized_view_v2.rs:203-238`; v1 doc
   `materialized_view.rs:70-78`). So the controller never sees an MV upper ahead
   of durable data.
2. `Instance::maybe_update_global_write_frontier`
   (`src/compute-client/src/controller/instance.rs:1744`) downgrades a
   write-only MV's `implied_read_hold` to `step_back(durable upper)` (1779-1786);
   `apply_read_hold_change` (1796) then `try_downgrade`s the **storage-input**
   read holds and emits `AllowCompaction` (1844-1854).
3. The storage controller's persist `since` downgrade is async/rate-limited
   (`src/storage-client/src/storage_collections.rs:1265-1267`) — it can only make
   the persisted `since` *lag*, never run ahead.

⟹ In a **single read-write environment** the input's protective hold is a persist
**critical** `SinceHandle` (never expires), so the invariant holds across crashes
and restarts. **A single env should not be able to persist `input.since >
mv.upper`.** (This is the part that makes the workload-replay/sanity_restart
manifestation puzzling — see Open Questions.)

**How it breaks (0dt / read-only) — the load-bearing finding:**
- In read-only mode the storage controller opens a persist **`Leased`
  `ReadHandle`**, never a critical `SinceHandle`
  (`storage_collections.rs:575-579`, comment `2374-2377`).
- A shard's `since` is the **meet of all leased AND critical reader sinces**
  (`src/persist-client/src/internal/state.rs:1337` invariant; `update_since`
  at `2116-2133`). So a live, heartbeated leased hold DOES fence the leader.
- **The hole:** `expire_leased_reader` (`state.rs:2006-2037`) has
  `update_since()` **deliberately commented out** (database-issues#6885) with a
  comment describing this exact hazard: *"a clusterd process has a ReadHandle …
  if we crash and stay down longer than the read lease duration … an expiry …
  jumps the since forward."* On lease expiry the meet is not recomputed, but the
  **next** `compare_and_downgrade_since` by the leader (a critical reader)
  recomputes the `since` over the *remaining* readers — now missing the lapsed
  follower hold — and can **jump the input `since` forward past a dependent MV's
  upper**. Reader lease default = 15 min (`src/persist-client/src/read.rs:616`).
- Persist `since` never regresses, so once corrupted the bad state is durable and
  every subsequent bootstrap of the (read-only) env panics ⟹ "reproducible".

This matches the user's framing: *"during 0dt the new env has read-holds, but
maybe we're not installing all required read-holds in read-only mode."* Precisely:
the read-only env's holds are **leased and not durably effective** — either a
required input isn't held early/low enough, or a held input's lease lapses.

## The tension that makes a naive repro fail (READ THIS before repro work)

For a **shared** MV present in BOTH the leader and the follower, the *leader's*
own critical read hold keeps `input.since <= mv.upper - 1` at all times, and the
follower reads `mv.upper` from the **same** persist shard. as-of selection
acquires the input read hold (capturing `input.since`) at
`as_of_selection.rs:114` **before** it reads `mv.upper` at `:478`, and
`mv.upper` only advances, so `lower <= upper` holds. ⟹ A vanilla shared MV +
healthy leader should NOT panic even if the follower's lease lapses, because the
leader still pins the input.

So the panic seems to **require the input to stop being held by the leader on the
MV's behalf**, while the follower still expects to resume that MV from a now-stale
upper. Candidate ways to get there:
1. **MV exists in the follower's catalog but not the leader's** — e.g. a new
   builtin MV in the newer binary (the #11273 shape, runtime variant), or a
   replacement/migrated MV. (Hard to stage with a single binary in same-version
   0dt; builtin-MV diffs need two binaries.)
2. **Concurrent DROP/recreate** (the #8836 shape): the leader drops the MV (so it
   stops holding the input) while the follower still has it pending; the
   follower's lease then lapses and the leader compacts the input forward.
   Complicated by the fact that the follower reads the **shared** catalog, so a
   clean DROP is visible to it.
3. **A genuine read-write read-hold-gating bug** that lets `input.since` overtake
   `mv.upper` even in the leader (would also explain the single-env
   workload-replay manifestation). Not yet found — see Open Questions.

## Open questions (highest-value next steps)

1. **Which manifestation is CLU-95 really?** Confirm from the failing
   release-qualification logs whether the panicking process was in **read-only /
   0dt** mode or a **plain restart**. Grep the failing build for `read-only`,
   `deploy-generation`, `IS_LEADER`, and for `u732`'s lifecycle
   (`creating dataflow ... export_ids=[u732] ... as_of=...`,
   `removing collection state because the since advanced to []`,
   `AllowCompaction ... u732`). This decides whether to chase the 0dt-lease
   theory or hunt a read-write gating bug.
2. **Is the workload-replay manifestation actually single-env read-write?** If
   yes, the leased-expiry theory does NOT explain it (critical holds don't
   expire), and there is a second, distinct bug. Re-examine: REFRESH MVs
   (their upper jumps and lags — `materialized_view.rs:174-175` mentions
   `apply_refresh` rounding), and any path where an input read hold is dropped
   while the MV's durable upper lags.
3. **Does the follower install a leased read hold for every required (transitive)
   input, early enough?** Inputs are pinned at storage-collection registration
   time (`create_collections_for_bootstrap`, post-#35933 in topological layers),
   then again via `acquire_read_holds` at `as_of_selection.rs:114`. Verify
   coverage for MV-on-MV chains, indexes-in-between, and REFRESH MVs.
4. **#6885**: pull its history. Is re-enabling `update_since` on
   `expire_leased_reader` viable, or was there a correctness reason it stays off?
   This is the most direct lever on the leased-expiry theory.

## Repro harness (`test/clu-95-repro/mzcompose.py`)

Run on a Linux box:
```
bin/mzcompose --find clu-95-repro down -v
# 0dt soak: leader + read-only follower, short reader lease, reboot follower
# under continuous load + compaction, scan follower logs for the panic.
bin/mzcompose --find clu-95-repro run zdt-soak --iterations 40 --lease-seconds 5
# also try the lever that drops the leader's hold on a shared input:
bin/mzcompose --find clu-95-repro run zdt-soak --iterations 40 --drop-recreate
# single-env ungraceful restart soak (mirrors workload-replay sanity_restart):
bin/mzcompose --find clu-95-repro run restart-soak --iterations 60
```
It sets `persist_reader_lease_duration` short, builds a `t -> mv1 -> mv2` chain
plus a REFRESH MV, drives continuous inserts/deletes, and on every (re)boot of
the env scans logs for `failed to apply hard as-of constraint`, raising
`AssertionError` if found. `soft_assertions` is on by default in the
`Materialized` service, so the panic is a hard crash; `propagate_crashes=False`
+ `restart=on-failure` keep logs around for scanning.

**It is a repro HUNT, not a proven reproducer** (see the tension section). If it
doesn't fire, the knobs to turn, roughly in priority order:
- `--drop-recreate` (drop leader's hold on `mv1` while follower expects `mv2`).
- Vary `--lease-seconds` (1–10) and the follower downtime (`lease * 3` in code).
- Add MV-on-MV depth, more REFRESH MVs with short/odd intervals.
- Stall the follower's heartbeat *while up* (e.g. `docker pause mz_new` for >
  lease, then unpause) instead of kill — closer to the #6885 wording.
- Introduce a second binary/generation that adds a builtin or replacement MV the
  leader lacks (case 1 above) — most likely to deterministically hit it.
- Confirm the build actually has soft-assertions on (`MZ_SOFT_ASSERTIONS=1`,
  which the service sets) so the panic surfaces.

## Candidate fixes (do NOT implement before the manifestation is confirmed)

1. **Availability safety net (low risk):** generalize the read-only workaround.
   Today `prune_dropped_collections` (`as_of_selection.rs:781`, read-only only)
   prunes collections whose inputs have *empty* read frontiers. Extend to the
   **finite** case: in read-only mode, when an input's `since` has overtaken a
   dependent's `upper` (hard lower > hard upper), prune/defer instead of
   `soft_panic`. Rationale: the read-only env will re-select once frontiers are
   consistent / on promotion. Does NOT fix the underlying durability of holds.
2. **Make read-only hold coverage complete & durable (medium):** ensure the
   follower holds every (transitive) input at a `since <=` each dependent's
   needed as-of, established before as-of selection reads downstream frontiers
   and kept alive for the whole read-only window. Possibly re-enable / replace
   the disabled `update_since` on leased expiry (#6885).
3. **New/replacement/migrated MV `since` init (if case 1):** the #35933 analogue
   for non-system MVs — when introducing an MV whose output isn't backed by a
   durable observable upper, initialize its `since`/as-of consistently with its
   inputs' current sinces (only safe for genuinely fresh collections — guard on
   `since.is_none()` / `initial_as_of` / `replacement_target`).
4. **Real gating bug (if Open Question 2 says single-env):** fix wherever an
   input read hold is released while a dependent MV's durable upper still lags —
   that is the correctness-critical one.

## Key code map
- as-of selection: `src/compute-client/src/as_of_selection.rs`
  - `run` (101), upstream/lower (428), downstream/upper (475), panic site (407),
    `prune_dropped_collections` (781), `prune_sealed_persist_sinks` (767).
- bootstrap: `src/adapter/src/coord.rs`
  - `bootstrap` (2015), MV install + `allow_writes` (2290-2331),
    `drop(dataflow_read_holds)` (2386), `bootstrap_storage_collections` MV since =
    `initial_as_of` (2972-2976), topological since-bump loop, `is_system()`-only
    (3084-3139), `bootstrap_dataflow_as_ofs` (3430).
- compute read holds: `src/compute-client/src/controller/instance.rs`
  - `create_dataflow` input holds (1267-1360), `maybe_update_global_write_frontier`
    (1744), `apply_read_hold_change` (1796), `downgrade_warmup_capabilities` (2213).
- storage holds / since: `src/storage-client/src/storage_collections.rs`
  - leased-vs-critical (`open_data_handles` 564, `SinceHandleWrapper` 2374),
    storage-dep `since` hold-back (1862-1917), async since downgrade (1265).
- persist since: `src/persist-client/src/internal/state.rs`
  - `update_since` = meet of all readers (2116-2133), `expire_leased_reader` with
    disabled `update_since` / #6885 (2006-2037), `compare_and_downgrade_since`
    (1962).
- reader lease: `src/persist-client/src/read.rs` (`READER_LEASE_DURATION` 616,
  heartbeat task 677-694).

## Sink reference PRs / issues
- PR #35933 (topological bootstrap + system-MV since bump), database-issues
  #11273, #8836 (concurrent DROP / `prune_dropped_collections`), #8718, #8753,
  #8631, and **#6885** (disabled `update_since` on leased expiry).
