# CLU-95 — continuation notes

> Status: **root-cause mechanism identified from build 1248 logs; fix not written.**
> A repro harness exists at `test/clu-95-repro/mzcompose.py` but is NOT yet confirmed to reproduce.
> The 2026-06-01 update at the bottom supersedes much of the leased-expiry theory below — read it first.

## Update 2026-06-01 — confirmed manifestation: compute `remove_replica` hold-accounting bug (incidents-and-escalations#39)

Downloaded `services.log` (115 MiB) from the failing release-qualification build 1248, job "Long Workload Replay (100% initial data) 1" (`019e5287-c728-49cb-95bd-a0f62ec2992e`).
The manifestation is now unambiguous and rules out the leader/follower leased-expiry framing for this build.

### Open Question 1 answered: single-env ungraceful restart, NOT 0dt

* Only one mz service: `workload-replay-materialized-1`.
  No leader/follower.
* `deploy_generation=0` on every boot; never bumped.
  The "0dt preflight checks" line runs unconditionally — its presence is not evidence of 0dt.
* Sequence: boot 01:52:28 → ~22 min of workload-replay traffic → kill at 02:14:21 (composition `sanity_restart_mz`) → first bootstrap panic at 02:14:32 → crash loop, identical panic bounds on each retry.

### u732 identity (catalog-side)

* `central_db_prod.analytics.stg_audiences`, MV with `REFRESH = ON COMMIT`.
  ON COMMIT is the default and does **not** trigger the REFRESH-MV upper-rounding code path, so the design-doc `apply_refresh` concerns do not apply here.
* Inputs `[s472, s473, s485, s515, s527, u100, u103, u198, u228, u269]`.
  s472/s473/s485/s515/s527 are system tables (`mz_audit_events` family, `mz_materialized_views`, etc.).
* Host cluster `sigma_prod` (id `u77`), `balanced_large_14_90`, `MANAGED = true`, `REPLICATION FACTOR = 1`, `SCHEDULE = MANUAL`.
* Created at 02:08:40 with `as_of=[1779502118000]`.

### Panic numbers, decoded

* `input.since = 1779502433000` = 2026-05-23T02:13:53.000Z.
  Driven by storage inputs s527 / s472 (their read frontiers at restart time).
* `u732.upper = 1779502402492` = 2026-05-23T02:13:22.492Z.
  Last durable persist sink upper before the kill.
* Gap = ~30.5 s.
  Persist `since` is monotonic, so this state survives the restart and crashes the env on every subsequent boot.

### u77 clusterd lifecycle (the leased-expiry theory does not apply)

* `cluster-u77-replica-u77-gen-0` booted 01:52:51.
* First abnormal event on u77: 02:14:21 `WARN mz_persist_client::rpc: pubsub client error: BrokenPipe`, followed by `ctp connection failed: unexpected end of file` — i.e. environmentd dying as part of `sanity_restart_mz`.
* u77 did not crash during the 22-min workload window; it held its leased ReadHandles throughout.
  The hold-lapse / #6885 framing is **not** the mechanism for THIS build.

### Smoking gun: 02:12:03 hold-accounting bursts (load-bearing)

At 02:12:03 — ~111 s before the kill — environmentd emits a burst of **30+** WARN lines:

```
WARN mz_compute_client::controller::instance:
  dropping per-replica read hold without equivalent global read hold
  replica_id=u18 collection_id=… input_id=…
  replica_hold_since=Antichain { elements: [T_R] }
  global_hold_since=Some(Antichain { elements: [T_G] })
```

Properties of every warning in the burst:

* All on `replica_id=u18` (central_compute_prod — `balanced_xxxxxlarge_64_412`, the big shared compute cluster).
* `T_R < T_G` in every case, by 10–80 s.
* Listed inputs are `u-`-prefixed (u522, u272, u273, u336, u337, u338, u137, u162, u348, u75, u76, u235, u240, u332, u343, u737) — NOT s472/s527 directly.
* Burst is co-located with environmentd creating new dataflows on cluster u30 around the same instant.

### Code reference

`src/compute-client/src/controller/instance.rs:1157-1186`, function `Instance::remove_replica`.
The comment **is the bug**, verbatim:

> Before dropping the replica state (and the contained input read holds), log read holds that are the last line of defense against compaction of a dataflow's storage inputs.
> **If the corresponding global read hold has already been released, dropping the per-replica read hold will allow compaction, which can cause the replica to panic trying to install the dataflow.**
> This exists primarily to help diagnose incidents-and-escalations#39.

The existing WARN is a diagnostic marker placed where the actual gating bug lives.
"incidents-and-escalations#39" is where the unfixed root cause is tracked.

### Mechanism (consistent with all evidence)

1. Replica `R` has a dataflow whose per-replica hold on storage input `I` is at `T_R`.
2. Some path advances the global compute hold on `I` to `T_G > T_R` (e.g., a different dependent dataflow's hold-down advances, and the per-replica hold isn't reflected back into the global meet).
3. `R` is removed via compute `remove_replica`.
   The per-replica hold drops with it.
4. Storage controller's `compare_and_downgrade_since(I)` now meets only the remaining readers, all at `≥ T_G`.
5. Persist `since(I)` jumps forward, past where some still-existing dependent's durable upper sits.
6. Composition restart.
   Bootstrap reads `I.since` (durably at `T_G`) and dependent MV's durable upper at `≤ T_R`.
   `as_of_selection` lower > upper ⟹ hard-constraint panic.

The warnings prove (1)+(2) for replica u18 at 02:12:03.
Whether s472/s527 specifically also lost a per-replica protective hold via u18 (or another removed replica) is the next thing to confirm — a warning is only printed when `T_R < T_G`; if they were exactly equal at removal time, the same compaction-forward happens silently.

### Why u18 was removed at 02:12:03 — not yet identified

* No `ALTER CLUSTER` in services.log (`grep -c "ALTER CLUSTER" = 0`).
* Only one `dropped replica` log line in the whole file, from `quickstart` cluster teardown at startup.
* u18 compute-side replica was removed without a paired storage-side `drop_replica`, suggesting either:
  * a compute-only `remove_replica` triggered by replica reconciliation during DDL bursts,
  * a `DROP CLUSTER REPLICA central_compute_prod.r…` from the captured workload-replay statements,
  * or scheduler activity related to `SCHEDULE = MANUAL` / REFRESH MV scheduling.
* Read ~30 lines before services.log:72663 to identify the triggering action.

### Why the WARN exists — and why CLU-34 is not the fix for CLU-95

The WARN at `instance.rs:1167` was added in **PR #35937** specifically as the diagnostic marker for **incidents-and-escalations#39**.
Teskje's own theory in #39 (slow-path SELECT canceled mid-install, then controller-replica reconnect) describes the **render-time** variant of the bug: the replica panics while trying to render a dataflow whose inputs already compacted past its as-of.
For the render-time variant the proposed fix is **CLU-34**: report read-hold-failure back to controller so it ignores rendering errors for dataflows it has already dropped.

**CLU-34 does NOT fix CLU-95.**
CLU-95 is the **bootstrap-time** consequence of the same bug class.
By the time bootstrap reaches `as_of_selection`, the durable persist `since` on the input is already past `step_back(dependent.upper)`.
Data is gone, not just the hold.
Reporting an error back from the (long-gone) replica's rendering attempt cannot un-compact persist.

### Variants of the bug class to consider

All share the same root: **a per-replica read hold is the only thing pinning an input below where the global hold sits, and the per-replica hold is released without an equivalent global hold being established first.**

1. **Cancelled slow-path SELECT + controller-replica reconnect** (teskje's theory, render-time symptom).
   Global drops first (peek cancelled), connection severs (timeout), replica state recreated without the per-replica hold, inputs compact, replica panics on render.
2. **Two-replica cluster, slow replica fails to render in time.**
   Fast replica makes progress, controller drops global hold based on fast replica, slow replica's per-replica hold lapses or is never acquired, inputs compact, slow replica panics.
   Build 1248 has **no** multi-replica clusters (`grep "REPLICATION FACTOR = [0-9]+"` yields only `= 1`), so this variant is NOT what fired in 1248 — but is a valid alternate trigger of the same bug class.
3. **Long-lived MV + replica re-creation** (this is the CLU-95 / build 1248 manifestation).
   MV on replica R has per-replica hold at `step_back(MV.upper) = T_R`.
   Global hold on the input has somehow advanced to `T_G > T_R`.
   R is removed (DROP CLUSTER REPLICA, ALTER CLUSTER, or compute-controller-side reconnect drop without storage drop — note build 1248 had NO storage `dropped replica` log other than at startup).
   R's per-replica hold is released; input compacts to `T_G`; bootstrap on next env restart panics.

### Three-pronged fix framing

Reading teskje's CLU-34 comment ff86489c carefully: the value isn't "retry the dataflow."
It's the **report-don't-panic** pattern.
That pattern maps to CLU-95 at three levels, each addressing a different concern:

1. **Upstream prevention — fix the hold-accounting bug**.
   This is the only fix that prevents NEW corruption.
   Tracked under incidents-and-escalations#39.
   Concretely, in compute controller:
   * keep `global_hold(I) >= meet(per-replica_hold(I))` as an invariant at all times, or
   * on per-replica hold drop, transfer to a controller-owned critical hold before releasing (so the meet never lifts), or
   * block per-replica drop until storage acknowledges `since(I) <= replica_hold_since`.
2. **Bootstrap-time report-don't-panic — CLU-95-specific safety net**.
   Replace the `soft_panic_or_log!` in `as_of_selection::apply_constraint` with a per-collection structured error.
   Controller catches the error, marks that collection unhealthy, **skips installing its dataflow**, and finishes bootstrap.
   Subsequent queries against the broken collection return an error explaining the cause.
   Other collections work as usual; the env starts up.
   This does NOT violate Moritz's determinism argument from CLU-34: the controller never invents a new as-of for the broken MV — it just refuses to start it.
   Strictly more general than the `prune_dropped_collections` empty-input safety net.
   Independent of #1: ships orthogonally, covers envs whose persist state is already corrupted.
3. **Render-time report-don't-panic — the original CLU-34 moral successor**.
   Clusterd's `cannot serve requested as_of` becomes an `Err` back to the controller instead of a panic.
   Controller decides: force replica reconnect (for transient leases) or mark dataflow degraded.
   Determinism is preserved — the controller never bumps as-of, it only triggers reconnect-and-reconcile.
   CLU-34 itself was **canceled 2026-06-01** because the proposed "retry" mechanic was wrong-shaped; the report-don't-panic principle survives, just not its CLU-34 framing.

#4 from the previous version of this doc (#35933-style topological `since` init for user MVs) is for the new/replacement MV variant — not THIS bug.

### Open Questions — updated

1. ~~Read-only / 0dt vs single-env?~~ **Single-env restart, plain `sanity_restart_mz` kill.**
2. **Live: gating bug in compute hold accounting** (incidents-and-escalations#39).
3. What action triggered the u18 compute-side `remove_replica` at 02:12:03?
   No `ALTER CLUSTER` in log (count = 0).
   Only one `dropped replica` log line, from quickstart teardown at startup.
   Compute-only `remove_replica` was called without paired storage `drop_replica`, which points at a controller-replica reconnect path (teskje variant 1) or compute-controller-internal reconciliation, not a SQL-driven drop.
4. Did s472 / s527 ever have a per-replica hold whose `T_R < T_G` at the moment of a `remove_replica`?
   The WARN only fires when strictly less-than at removal time.
   Compaction-forward also occurs when equal at removal, and silently.
   Need to instrument or grep more carefully — search whether u77, or any other replica reading s472/s527, was removed.
5. Was there a controller-replica reconnect on u18 (or others) around 02:12:03 that we can confirm from the log (e.g., persist pubsub error, ctp connection failed, replica reconcile)?

## Reproducer redesign

The existing harness exercises kill+restart and DROP/recreate at the SQL level.
It does NOT exercise:

* Controller-replica connection severance while a dataflow is mid-install.
* Cancelled slow-path SELECT during hydration window.
* Two-replica clusters with one slow replica.
* Targeted clusterd kill (it kills the whole composition).

### Knobs to add

* **Run clusterd as a separate `Clusterd` service in mzcompose** (not as a subprocess of `Materialized`), so it can be paused / killed independently via `docker kill`, `docker pause`, or `compose.kill(service)`.
* **FAILPOINTS env var.** The `fail` crate is in the codebase (`doc/developer/failpoints.md`).
  Useful failpoints to add for this work (small code change):
  * `compute_instance::remove_replica::before_release` — pause/panic between the diagnostic WARN and the actual `drop(replica)`.
  * `compute_instance::install_dataflow::after_global_hold_drop_before_per_replica` — to materialize teskje's variant 1.
  * `compute_controller::replica_reconnect::skip_re_install_holds` — model the reconnect-state-recreation race directly.

### Proposed mzcompose workflows (add to `test/clu-95-repro/mzcompose.py`)

1. **`cancelled-peek-reconnect`** (teskje variant 1):
   single env, single-replica cluster `c` with explicit `Clusterd`.
   Create table `t` and MV `mv ON CLUSTER c` reading `t`.
   Loop: issue a heavy slow-path `SELECT` on `c`, cancel after 100 ms, immediately `docker kill -s 9 clusterd-c`, let it restart, wait `compaction_lag + 5 s`, `sanity_restart_mz`, scan bootstrap for `failed to apply hard as-of constraint`.
2. **`replica-removal-under-load`** (build 1248 variant):
   two clusters `c_writer` (RF=1) and `c_compute` (RF=1) each on their own Clusterd, both reading a shared table `t`.
   `c_writer` hosts MV with a deliberately slow upper-advance (large batches, infrequent commits).
   On `c_compute`, repeatedly install/drop dataflows reading the same `t` (or the same MV) to drive global hold forward.
   Then issue `DROP CLUSTER REPLICA c_compute.r` (or `ALTER CLUSTER c_compute SET (REPLICATION FACTOR 0)`).
   Wait, `sanity_restart_mz`, scan.
3. **`two-replica-slow`** (variant 2):
   one cluster `c` with `REPLICATION FACTOR 2`, with one replica on a CPU-throttled Clusterd (via `cpus:` in compose or `docker update --cpus`).
   Create a heavy MV on `c`.
   Drop the fast replica.
   `sanity_restart_mz`.
   Scan.
4. **`failpoint-targeted`** (if we add the failpoint above):
   single env, single-replica cluster.
   Create MV.
   Enable `FAILPOINTS=compute_instance::remove_replica::before_release=pause(5s)`.
   `DROP CLUSTER REPLICA` and during the 5 s pause, force adapter to advance global hold via fresh dataflow installs.
   Resume, `sanity_restart`, scan.

Tag each workflow with a checked log marker so we can tell whether the WARN at `instance.rs:1167` fired during the run — if it did, the bug class manifested even if the eventual bootstrap didn't panic.

### Where the harness should add assertions / tooling

* On every reboot of `materialized`, scan the new log for `failed to apply hard as-of constraint`.
  If found, raise.
* Also scan for `dropping per-replica read hold without equivalent global read hold` — if present, the hazard class fired (whether or not we got the bootstrap panic this run).
* Capture services.log + journalctl into the workdir on assertion failure.

### Repro harness results (2026-06-01)

Implemented and ran both new workflows. Results:

* `cancelled-peek-reconnect` (30 iters, mz_unsafe.mz_sleep slow query + cancel + clusterd docker-kill reconnect, REFRESH EVERY 120s MV): **0 WARN, 0 panic**.
* `replica-removal-under-load` (40 iters, churn MVs on compute cluster + DROP CLUSTER REPLICA + recreate, REFRESH EVERY 20s MV on writer cluster): **0 WARN, 0 panic**.

Conclusion: with a single dependent dataflow + a single input, the global hold and per-replica hold advance lockstep — we never produce the `replica_hold.since < global_hold.since` asymmetry the WARN requires.
Build 1248 saw the WARN despite RF=1 because it had hundreds of MVs sharing system-table inputs under sustained concurrent DDL — that volume creates the async window.

Recommendation: **do not pursue a SQL-driven deterministic repro further.**
The cheapest deterministic test is a Rust unit test against `compute-client::Instance` that drives `install_dataflow` + frontier downgrades + `remove_replica` directly to construct the asymmetry.
That same test then locks down the fix for #39.

The harness workflows are still useful as smoke tests once a fix exists, but they should not be the primary regression signal.

---

The pre-2026-06-01 notes below are kept for reference but are partly superseded:
the leader/follower leased-expiry framing in particular is NOT the mechanism for build 1248.

---

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
