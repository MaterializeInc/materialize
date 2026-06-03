# Cluster Autoscaling — Implementation Plan & Progress Tracker

Companion to the design doc: [`20260522_cluster_autoscaling.md`](./20260522_cluster_autoscaling.md).
That doc is the source of truth for **what** and **why**; this doc is the source of truth
for **how we land it** — the PR sequence, per-PR scope, gating, and live progress.

> **Status:** Not started. The design (the in-flight target lives in a durable
> `reconfiguration` record; `cluster.size` stays the realized fact) is settled; no controller
> code exists in the tree yet. Build from PR 1.

---

## Working on this plan (operating protocol for implementation sessions)

**To start a session, the user says something like:**

> "Please work on the next step of `@doc/developer/design/20260522_cluster_autoscaling_plan.md`."

**An implementation session then does, in order:**

1. **Read the design doc** [`20260522_cluster_autoscaling.md`](./20260522_cluster_autoscaling.md) in full, then this plan.
2. **Work from current code, not assumptions.** Skim the *Codebase anchors* below and
   re-verify the file:line references you'll touch before relying on them — they drift.
3. **Pick the current PR**: the first PR below whose status is not `✅ Merged`. If a PR is
   `🚧 In progress`, continue its unchecked checklist items. Do not start a later PR before
   an earlier one is merged unless the user says so (the stack is ordered by dependency).
4. **Confirm the open decisions** for that PR (each PR lists the decisions it must resolve).
   If a decision is genuinely the user's to make and isn't already settled here or in the
   design doc, ask before building. Otherwise pick the recommended option and note it.
5. **Implement**, following the working agreement below.
6. **Update this file**: tick the checklist items you finished, update the PR status line,
   and append a dated note to the Progress Log at the bottom. Keep the tracker honest —
   if something was authored-but-not-run, say so.

**Working agreement:**

- **Match the codebase.** Read the neighbouring code before writing; mirror its idioms,
  comment density, and naming. Honour root `CLAUDE.md` (workspace deps, `cargo update`
  discipline, license sync) and the `mz-*` skills.
- **Cheap checks before claiming success:** `cargo fmt` and `cargo check -p <crate>` for the
  crates you touched. The optimized build is heavy — do **not** block on a full release
  build locally; rely on `cargo check` locally and **author** the boundary tests for CI.
- **Test the boundaries, user-visible behavior first.** The boundary that matters most is what
  a user observes: assert it with sqllogictest / testdrive (`ALTER` returns, `SHOW CLUSTERS`
  shows target vs current, cut-over, timeout tombstone, burst appears/tears down, rejected
  options error) and survival across restart/upgrade with platform-checks. The other boundary
  is the `ClusterControllerCtx` interface — exercise the controller against a fake impl. Add
  unit tests for the pure reconcile kernel only for tricky, high-value cases (multiset union,
  convergence, timeout precedence) — **not** exhaustive per-function suites.
- **Keep each PR green and behaviour-preserving** — every PR (1–6) lands dark; only the final
  cleanup PR (7) changes defaults and removes code.
  Everything new lands **dark** behind the feature gate; existing tests must still pass with
  the gate at its default.
- **Pure strategies behind a clean interface.** Strategy logic is pure
  (`fn(config, status, now) -> output`, no I/O) in `mz-cluster-controller`; the controller pulls
  inputs through the `ClusterControllerCtx` trait and applies outputs through it. Spend design
  effort on that interface — it is the boundary that matters.
- **Migrations:** follow the catalog-migration recipe (see Anchors). Bump `CATALOG_VERSION`,
  snapshot the proto, add the `vN_to_vN+1.rs` module, generate encodings. One migration PR
  at a time; never regenerate `Cargo.lock` wholesale.
- **Commits/PRs:** only when the user asks. Use the project's commit conventions.
- **Changes that belong to an earlier PR.** While implementing PR N you may discover a change that
  logically belongs in an earlier PR's commit (M < N) — a durable field that should have been added
  in PR 1, a wrong conversion in PR 2's scaffolding, a missing test. **Do not fold it silently into
  the PR N commit.** Make it a **separate commit placed before** your PR N change, with a message
  that names the PR it really belongs to (e.g. `fixup! <PR M's commit subject>`, or a subject like
  `PR 1: <fix>` so `git rebase --autosquash` / a later manual squash can land it). This keeps the
  out-of-scope fix **visible and reviewable on its own** now, and lets us later squash it into the
  PR-M commit it was meant to be in from the start. The PR N commit then stays focused on PR N's
  scope. Note any such commit in the Progress Log so the eventual squash isn't forgotten.

**Status legend:** `⬜ Not started` · `🚧 In progress` · `👀 In review` · `✅ Merged`

---

## Context: starting point

- There is no `mz-cluster-controller` crate, no controller module in the adapter, and none of
  the autoscaling dyncfgs yet — we build from scratch, starting at PR 1.
- The model is **realized-fact** (per the design doc): `ALTER` writes a durable
  `reconfiguration` record (target + deadline) and returns; `cluster.size` only advances at the
  controller's cut-over. The records and their migration land in PR 1.

---

## Architecture at a glance

**Crate layout (target):**

- `src/cluster-controller/` (`mz-cluster-controller`) — **pure** framework: the strategy
  trait, the reconcile kernel (union desired contributions → diff vs actual → decisions),
  the strategy implementations, and their unit tests. Minimal deps (ids, repr, controller
  types for shapes); **no** adapter/catalog-writer deps.
- `src/adapter/src/coord/cluster_controller.rs` — the **driver/glue**: implements
  `ClusterControllerCtx` over the catalog + live controller signals (the controller pulls), and
  applies the emitted `Decision`s as catalog transactions under a compare-and-append guard.

**The reconcile tick (per the design):** two phases separated by a barrier —
(1) run every strategy's `update_state`, transact the durable writes (cut-overs, record
writes/clears) and **await** confirmation; (2) run every strategy's `desired_replicas`,
union the contributions (implicit baseline included), diff against actual replicas, emit the
creates/drops that close the gap. Commands name **explicit replicas**; re-emission is
harmless (create-existing / drop-gone are no-ops).

**The boundary — a clean, agnostic pull-interface (the most important thing to get right).**
The controller depends on one narrow trait (working name `ClusterControllerCtx`) and *pulls*
what it needs through it; the environment never guesses and pushes a snapshot. The trait is
**strategy-agnostic** — primitive signals in, primitive catalog mutations out — and carries
**no per-strategy state or vocabulary**:

- reads: a consistent durable view of cluster + replica state; "are these collections on this
  cluster hydrated on these replicas?"; collection write frontiers; a read timestamp; `now`.
- writes: "apply this catalog batch under a compare-and-append guard."

This keeps the abstraction **zero-cost / pay-for-what-you-use**: a tick fetches only the
signals it actually examines (no eager all-clusters-all-replicas snapshot), and the controller
drives what is fetched. The controller crate knows nothing about the Coordinator; the
Coordinator implements the trait. That single seam is what makes the controller testable (drive
it against a fake impl) and extractable later (swap the impl for one backed by builtin-relation
subscriptions) without touching controller code.

Keep two boundaries distinct:

- **controller ↔ environment** = the `ClusterControllerCtx` trait (above). Design and test *here*.
- **controller ↔ strategy** = the pure strategy functions `update_state` / `desired_replicas`
  over `(config, observed status, now)`. The controller assembles each strategy's inputs by
  pulling through the ctx; strategies never touch the ctx.

**Compare-and-append guard.** The Coordinator stays the sole catalog writer. Each applied batch
carries the controller's view of the durable cluster state it was derived from (config +
`reconfiguration` + `burst`); the Coordinator applies it only if that state still holds, so a
user `ALTER` that lands mid-tick rejects the stale batch and the controller recomputes next tick.

**Gating model (how we land 7 PRs without changing prod behaviour until ready):**

- `enable_cluster_controller` (master gate, default **false**). When false the controller
  task is inert and **all legacy paths run** (3-stage graceful machine + `cluster_scheduling.rs`).
  When true the controller owns the managed-cluster replica set and legacy paths are bypassed.
- Because the controller — when on — must own **baseline + graceful + on-refresh** coherently
  (two writers of the replica set is not allowed), strategies land **dark** across PRs 2–6 (the
  gate stays false in prod; tests force it true to exercise each strategy in isolation). When the
  gate is on, the legacy entry points (graceful stage machine, `cluster_scheduling.rs` tick)
  no-op, so flipping it is a clean switch.
- **Turning it on is operational, not a code change.** Once the whole feature (incl. burst) is in
  `main` dark, we enable the gates at runtime via LaunchDarkly, roll out, and **bake in prod**.
  The legacy paths stay in the binary so rollback is a flag flip. Only **after** a successful bake
  does a final code PR (PR 7) flip the dyncfg defaults to true and delete the legacy paths (and
  only then is it safe to drop `pending`). We never enable *and* remove-the-fallback in one step.
- This is intentional: we trade "incremental prod enablement per strategy" (two coexisting
  replica-set writers) for "incremental, independently-reviewable PRs behind one gate, a runtime
  flip we can roll back, and a cleanup that lands only once prod is happy."

**Dyncfgs (final set; add as the relevant PR lands):**

| Key | Type | Default | Introduced | Purpose |
|---|---|---|---|---|
| `enable_cluster_controller` | bool | false | PR 2 | Master gate: controller owns the replica set; legacy paths bypassed. |
| `cluster_controller_tick_interval` | Duration | (≈ today's scheduling interval) | PR 2 | Reconcile cadence (replaces `cluster_check_scheduling_policies_interval`). |
| `enable_background_alter_cluster` | bool | false | PR 3 | When true `ALTER` returns immediately; when false the session wait-shim blocks (foreground UX) over the same durable mechanism. |
| `default_cluster_reconfiguration_timeout` | Duration | (system default) | PR 3 | Deadline written when an `ALTER` omits `WITH (WAIT ...)`. |
| `enable_auto_scaling_strategy` | bool | false | PR 6 | Gates `AUTO SCALING STRATEGY` SQL acceptance. |
| `enable_hydration_burst` | bool | true (at GA) | PR 6 | Break-glass: set false to disable the burst strategy env-wide without touching graceful/on-refresh. |

Defaults stay as above (the off ones off) in code until **PR 7**; enabling in prod is a runtime
LaunchDarkly flip, not a default change.

---

## Cross-cutting decisions to confirm

These shape multiple PRs. A recommendation is given for each; confirm with the user before
building where the choice is genuinely theirs.

1. **The `ClusterControllerCtx` interface + execution placement.** The controller pulls signals
   through the agnostic trait (see *The boundary* above) and never receives a pushed snapshot.
   *Open within that:* where the trait impl runs. The signals live on coordinator-owned
   compute/storage controllers reachable only from its loop, so the impl either (a) runs the
   controller as a **separate task** whose ctx calls marshal to the Coordinator over a channel
   (use **batch** pull methods to bound round-trips), or (b) drives the controller from a tick
   **on the coordinator loop** with the ctx as direct calls (truly zero-cost, but occupies the
   loop for the tick). The trait makes this reversible. *Recommend* (a) to honour the design's
   separate-task intent; pick in PR 2. Either way, controller code is written only against the trait.
2. **Compare-and-append mechanism.** *Recommend:* the apply path re-reads each target cluster's
   durable config + records and rejects the batch if it differs from the controller's carried
   view (`expected`), then the controller recomputes next tick. Could be a new `Op` precondition
   or a check in `apply_cluster_decisions`. Resolve in PR 2.
3. **Where the new records live.** *Recommend:* add `auto_scaling_strategy`, `reconfiguration`,
   `burst` to durable `ClusterVariantManaged` (managed-only; cut-over already rewrites this struct
   via `Op::UpdateClusterConfig`). Resolve in PR 1.
4. **`ReconfigurationState.target` shape.** *Recommend:* a struct carrying `size`,
   `replication_factor`, `availability_zones`, `logging` (the full config-shape the cluster is
   moving to, so a combined size+rf+AZ change is one record). Resolve in PR 1.
5. **Burst-replica identity — not needed.** A burst replica is just a replica; the union/diff
   reconciler handles it without any tag or label. A replica survives iff *some* strategy desires
   a slot of its shape this tick, and the controller drops to the desired count by shape, picking
   any excess replica — it never needs to know "which one was the burst replica." Abandoned
   re-target replicas are dropped only when no strategy desires their shape (so if burst still
   wants that shape, the replica is simply reused — fine). Audit attribution ("which strategies
   desired this create") is computed per-tick from the current desired set, not stored. So **no
   durable marker** is needed; this drops out of PR 1 and PR 6.
6. **Naming.** Design Open Question 7. *Recommend:* "cluster controller" / `mz-cluster-controller`
   (matches the design's lean). Alternatives: `ClusterReconciler`, `ClusterScheduler`.
7. **Timing of the non-inert changes** (`pending: bool` removal; durable `replication_factor`→0
   for scheduled clusters). Neither can ride PR 1: removing `pending` breaks the legacy stage
   machine, and normalizing rf while the legacy scheduler still toggles it would just be undone.
   *Recommend:* PR 1 holds only **additive, inert** durable changes; the controller normalizes
   scheduled-cluster rf at runtime when enabled (PR 5 — self-healing, no migration); and the
   `pending` drop rides the **final cleanup PR (7)**, after legacy removal.

---

## Codebase anchors

Verified against the tree on 2026-06-01 unless marked *(approx)*. **Re-verify line numbers
before relying** — they drift.

**Durable catalog state & migrations**
- Durable cluster config: `src/catalog/src/durable/objects.rs` — `ClusterConfig` (335), `ClusterVariant` (341), `ClusterVariantManaged` (347–354).
- Durable replica location: same file — `ReplicaLocation::Managed { size, availability_zone: Option<String>, internal, billed_as, pending }` (470–483). `availability_zone` is the single user-pin; the **provisioned AZ list** must be added here.
- In-memory mirrors: `src/catalog/src/memory/objects.rs` — `ClusterConfig` (≈3253), `ClusterVariantManaged` (≈3286). *(approx)*
- Proto: `src/catalog-protos/src/objects.rs` — `ManagedCluster`, `ManagedLocation`, `ClusterSchedule` *(approx)*; `.proto` sources under `src/catalog-protos/protos/`.
- Version: `src/catalog-protos/src/lib.rs` — `CATALOG_VERSION = 85`, `MIN_CATALOG_VERSION = 74`. **Next migration is v85→v86.**
- Migration framework: `src/catalog/src/durable/upgrade.rs` (recipe in the module header, `run_upgrade` dispatch, `MigrationAction`); per-version modules `src/catalog/src/durable/upgrade/vN_to_vN+1.rs` (latest `v84_to_v85.rs`); encodings under `.../upgrade/snapshots/`. Generate with `cargo test -p mz-catalog --lib durable::upgrade::tests::generate_missing_encodings -- --ignored`.

**Graceful reconfiguration (legacy, to be bypassed then removed)**
- `src/adapter/src/coord/sequencer/inner/cluster.rs`: `PENDING_REPLICA_SUFFIX` (50); 3-stage machine — Stage 1 create (≈148+, pending replicas at ≈1129), Stage 2 `check_if_pending_replicas_hydrated_stage` (≈488), Stage 3 `finalize_alter_cluster_stage` (≈362, strip suffix at 441); `AlterClusterWhilePendingReplicas` raised at ≈1063.
- `src/adapter/src/coord.rs`: `enum ClusterStage` (818) with `WaitForHydrated`/`Finalize` structs (≈825–846); `ConnMeta.pending_cluster_alters` (≈1226). *(approx)*
- Plan/AST: `AlterClusterPlan`, `AlterClusterPlanStrategy` (None/For/UntilReady), `OnTimeoutAction` in `src/sql/src/plan.rs` *(approx)*; cluster options AST `ClusterOptionName` in `src/sql-parser/src/ast/defs/statement.rs` *(approx)*; planning in `src/sql/src/plan/statement/ddl.rs` (`plan_create_cluster*`, `plan_alter_cluster`) *(approx)*; gate `ENABLE_ZERO_DOWNTIME_CLUSTER_RECONFIGURATION`.

**Scheduling (legacy, to be ported then removed)**
- `src/adapter/src/coord/cluster_scheduling.rs`: `check_scheduling_policies` / `check_refresh_policy`, `SchedulingDecision::Refresh(RefreshDecision)`, `handle_scheduling_decisions` (rf toggled 0↔1; swallows `AlterClusterWhilePendingReplicas`). *(approx)*
- `src/adapter/src/coord.rs`: `Message::CheckSchedulingPolicies` (414), `Message::SchedulingDecisions` (420), `check_cluster_scheduling_policies_interval` (1966; tick 3683; init 4734).

**Hydration / frontier signals (controller inputs)**
- Compute: `ComputeController::collections_hydrated_for_replicas(&self, instance_id, replicas, exclude) -> oneshot::Receiver<bool>` — `src/compute-client/src/controller.rs` (≈421). Takes `&self`.
- Storage: `StorageController::collections_hydrated_on_replicas(&self, replicas, cluster_id, exclude) -> Result<bool, _>` — `src/storage-client/src/controller.rs` (≈334). Takes `&self`.
- Frontiers: `collection_frontiers(&self, ...)` on both controllers. The legacy wait stage calls these at `cluster.rs` ≈548–558.
- Controllers are owned by `Coordinator.controller: mz_controller::Controller` (`src/controller/src/lib.rs` ≈148). Reachable from the coordinator loop only — so the `ClusterControllerCtx` impl backs the hydration/frontier reads from there (decision 1).

**Coordinator wiring & dyncfgs**
- Main loop `serve` + biased `select!`: `src/adapter/src/coord.rs` (≈3483+); `internal_cmd_tx: mpsc::UnboundedSender<Message>` (≈1848). Background-task pattern: `mz_ore::task::spawn`, clone `internal_cmd_tx`, send a `Message` (see storage-usage collection in `message_handler.rs`). Oneshot request/response: `Command { tx: oneshot::Sender<_> }` in `src/adapter/src/command.rs`.
- Catalog ops: `Op` enum + `transact(...)` in `src/adapter/src/catalog/transact.rs` (`Op::UpdateClusterConfig` ≈220; `ReplicaCreateDropReason` ≈311; `into_audit_log` ≈322).
- Dyncfg: define in `src/adapter-types/src/dyncfgs.rs` (`Config::new(name, default, desc)`), register in `all_dyncfgs()`, read via `KEY.get(self.catalog().system_config().dyncfgs())`.

**Builtins / observability / audit**
- Builtin defs: `src/catalog/src/builtin/mz_catalog.rs` (`MZ_CLUSTERS` ≈1804, `MZ_CLUSTER_REPLICAS` ≈1976) and `src/catalog/src/builtin/mz_internal.rs` (`MZ_SHOW_CLUSTERS` ≈4806 — this is what `SHOW CLUSTERS` selects from). `SHOW CLUSTERS` SQL template: `src/sql/src/plan/statement/show.rs` `show_clusters` (≈755). *(approx)*
- Builtin tables are catalog-populated (rows written where catalog state is reflected to builtin tables); builtin views are pure SQL. New builtins need no migration; **changing** an existing builtin needs a step in `src/adapter/src/catalog/open/builtin_schema_migration.rs`. Builtin dependency ordering in `BUILTINS_STATIC` is enforced by a test.
- Audit: `src/audit-log/src/lib.rs` — `VersionedEvent`, `EventDetails`, `CreateClusterReplicaV4` (≈559), `DropClusterReplicaV3` (≈460), `SchedulingDecisionsWithReasonsV2` (≈626). New reason variants are added to `CreateOrDropClusterReplicaReasonV1` + `ReplicaCreateDropReason`.

---

## PR sequence overview

| PR | Title | Maps to user's bucket | Gate state | Catalog migration |
|---|---|---|---|---|
| **1** | Durable state model + migration (additive, inert) | durable state | n/a (dark by being unused) | v85→v86 (additive) |
| **2** | Controller scaffolding + baseline strategy + coordinator wiring + CaA | controller scaffolding | adds `enable_cluster_controller` (off) | none |
| **3** | Graceful reconfiguration strategy + `ALTER` reshape + wait-shim + hydration input | move graceful over | dark behind master gate | none |
| **4** | Observability: introspection view + `SHOW CLUSTERS` + reconfig audit | (supports graceful) | dark (no records until enabled) | none |
| **5** | `ON REFRESH` scheduling as a strategy | move scheduling over | dark behind master gate | none |
| **6** | Hydration burst: `AUTO SCALING STRATEGY` SQL + burst strategy + break-glass (last **feature** PR) | add burst (last) | adds `enable_auto_scaling_strategy` / `enable_hydration_burst` (off) | none |
| — | **Operational rollout** (not a PR): enable the gates at runtime via LaunchDarkly; bake in prod | turn it on | runtime flip — legacy stays as fallback | none |
| **7** | Flip dyncfg defaults to true + remove legacy paths (only after prod bake) | the cleanup | defaults → on | drop `pending` |

Splitting/merging notes: PR 3 is the largest and may split into **3a** (strategy + hydration
input + record handling, controller side) and **3b** (`ALTER` SQL reshape + wait-shim + audit
reason). PR 4 may merge into PR 3 if you want graceful + its observability in one review. PR 6
may split into **6a** (SQL surface + validations) and **6b** (burst strategy + observability).

---

## PR 1 — Durable state model + migration

**Status:** 👀 In review

**Goal.** Add every *additive, behaviourally-inert* durable field the controller will need,
with one catalog migration (v85→v86) defaulting them for existing clusters. No reads of the
new fields yet → no behaviour change.

**Scope (in).**
- New durable + in-memory + proto types and their conversions:
  - `AutoScalingStrategy { on_hydration: Option<OnHydration { hydration_size: String, linger_duration: Option<Duration> }> }` — extensible (a struct/list so future strategies slot in). User-configured *policy*.
  - `ReconfigurationState { target: ReconfigurationTarget, deadline: mz_repr::Timestamp, on_timeout: OnTimeoutAction }`, where `ReconfigurationTarget { size, replication_factor, availability_zones, logging }` (decision 4) and `on_timeout` is the `COMMIT`/`ROLLBACK` action applied if the deadline passes un-hydrated (default `ROLLBACK`). *(The `on_timeout` field was added after PR 1's initial commit, in the PR 3 design refinement; it lands as a `fixup!` against PR 1's catalog commit — v86 is unshipped, so the proto is revised in place — per the fixup-commit convention.)*
  - `BurstState { burst_size: String, linger_duration: Duration, steady_hydrated_at: Option<mz_repr::Timestamp> }`.
  - Add the three as `Option<...>` fields on `ClusterVariantManaged` (decision 3), `None` by default.
- **Provisioned AZ list on managed replicas:** collapse durable `ReplicaLocation::Managed`'s single `availability_zone` user-pin into an `availability_zones: Vec<String>` recording the zones the replica was provisioned under — a managed cluster's `AVAILABILITY ZONES` pool, or an unmanaged replica's pin as a zero-/one-element list. Backfill existing managed replicas from their cluster's current `availability_zones` and unmanaged-cluster replicas from their pin. (Needed so the controller can tell realized- from target-shape replicas by config shape, including AZ divergence.) The in-memory `ManagedReplicaAvailabilityZones` enum is kept here and removed in a follow-up commit — it can only become a bare `Vec` once this durable field stores the list unconditionally.
- Migration `v85_to_v86.rs`: snapshot proto, bump `CATALOG_VERSION`, default new fields to `None`/empty, backfill the AZ list, generate encodings. Thread the new fields through all `From`/`RustType` conversions and any exhaustive matches.

**Scope (out).** No reads of the new fields. **Not** here: `replication_factor` normalization
and `pending` removal (decision 7 → PR 7). No SQL surface for `AUTO SCALING STRATEGY` (PR 6).
No builtin/`mz_clusters` exposure (PR 4).

**Verification.** `cargo fmt`; `cargo check -p mz-catalog -p mz-catalog-protos -p mz-adapter`;
catalog upgrade unit tests + `generate_missing_encodings`; confirm the round-trip
(`from_proto(to_proto(x)) == x`) holds for migrated rows.

**Acceptance criteria.** Builds; all existing tests pass; a catalog opened at v85 upgrades to
v86 with new fields defaulted and managed replicas' AZ lists backfilled; no behaviour change.

**Checklist.**
- [x] Decide records placement (3 — on durable `ClusterVariantManaged`), target shape (4 — `ReconfigurationTarget { size, replication_factor, availability_zones, logging }`).
- [x] Add types (durable, memory, proto) + conversions for `AutoScalingStrategy`/`OnHydration`, `ReconfigurationState`/`ReconfigurationTarget`, `BurstState`.
- [x] Collapse durable `ReplicaLocation::Managed` `availability_zone` → `availability_zones: Vec<String>`; update all construction/conversion sites (the controller→durable `From` maps both `ManagedReplicaAvailabilityZones` variants to the list; concretize re-derives the managed pool from the cluster and reads the list as the pin for unmanaged clusters).
- [x] Write `v85_to_v86.rs`; bump `CATALOG_VERSION` to 86; snapshot proto (`objects_v86.rs` + hashes); backfill AZ list; generate encodings (`objects_v86.txt`).
- [x] `cargo fmt` + `cargo check -p mz-catalog -p mz-catalog-protos -p mz-adapter` clean; catalog-protos snapshot tests + catalog durable/upgrade tests pass (incl. four new migration tests and `proptest_state_update_kind_roundtrip`).
- [x] Update this tracker (status + Progress Log).

---

## PR 2 — Controller scaffolding + baseline strategy + coordinator wiring

**Status:** 👀 In review

**Goal.** Stand up the controller end-to-end with **only the implicit baseline strategy**, so
the loop runs but is a no-op for steady-state clusters. Establish the task boundary, the
input/decision types, the compare-and-append apply path, and the master gate — all dark.

**Scope (in).**
- New crate `mz-cluster-controller` (`src/cluster-controller/`): add to workspace, `deny.toml`/`about.toml` if new licenses.
  - **`ClusterControllerCtx` trait** — the agnostic pull/apply interface (reads: durable cluster+replica view, hydration of collections on replicas, frontiers, read-ts, `now`; write: apply a CaA-guarded batch). This is the PR's centerpiece — design it carefully. Baseline needs only the durable view, so the hydration/frontier methods exist but go unused until PR 3.
  - Pure strategy trait: `update_state(config, status, now) -> Vec<StateUpdate>` and `desired_replicas(config, status, now) -> Vec<DesiredReplica>`.
  - Reconcile kernel: pull current state via the ctx → run strategies → union desired contributions (baseline included) → match-by-shape against actual → emit `Decision::{CreateReplica { name, config, reasons }, DropReplica { id, reasons }, UpdateClusterState { expected, writes }}` → apply via the ctx. Attribution (which strategies desired a create) is computed here, per tick.
  - **Implicit baseline strategy:** desires `replication_factor` replicas at `cluster.size` with the cluster's shape. Baseline-only ⇒ desired == realized ⇒ no decisions in steady state.
- Adapter driver `src/adapter/src/coord/cluster_controller.rs`:
  - Implement `ClusterControllerCtx` on the Coordinator side; drive the controller per decision 1 (separate task with batched marshaling, or on-loop tick) at `cluster_controller_tick_interval`.
  - The ctx apply method transacts `update_state` writes (await), then create/drop, under the **compare-and-append guard** (decision 2) — reject + recompute next tick on mismatch.
  - Master gate `enable_cluster_controller` (off): when off, the controller is inert; legacy paths untouched.

**Scope (out).** No hydration input yet (baseline doesn't need it; PR 3). No graceful/on-refresh/burst
strategies. No legacy removal. No SQL changes.

**Verification.** `cargo fmt`; `cargo check -p mz-cluster-controller -p mz-adapter -p mz-adapter-types`.
Boundary tests: drive the controller against a **fake `ClusterControllerCtx`** to assert the
reconcile loop is a no-op for a steady cluster and that a CaA conflict is rejected and recovered.
User-visible: an integration test that force-enables the gate and asserts a steady managed cluster
sees **zero** controller-initiated replica changes. A couple of kernel unit tests for the multiset
union/diff only.

**Acceptance criteria.** With the gate off, behaviour is identical to today. With the gate forced
on in a test, the controller reconciles a steady managed cluster to a no-op and a CaA conflict is
rejected and recovered on the next tick.

**Checklist.**
- [x] Resolve decisions 1, 2, 6. (1 — separate task; the `CoordCtx` marshals each batched pull/apply to the coordinator over `internal_cmd_tx` + oneshot. 2 — each `UpdateClusterState` carries an `ExpectedClusterState`; the apply path re-reads each cluster and rejects the whole batch on mismatch. 6 — `mz-cluster-controller` / "cluster controller".)
- [x] Create `mz-cluster-controller` crate (workspace + default-members; no new third-party deps so `deny.toml`/`about.toml` unchanged); the `ClusterControllerCtx` trait, pure `Strategy` trait, `Decision`, reconcile kernel (multiset union = max-per-shape, match-by-shape diff, attribution).
- [x] Implement baseline strategy (`replication_factor` replicas at the realized shape).
- [x] Adapter driver `coord/cluster_controller.rs`: implement `ClusterControllerCtx` on the Coordinator (`observe_cluster_state` read; `apply_cluster_decisions` builds `Op::UpdateClusterConfig` + `Op::CreateClusterReplica` + `Op::DropObjects`); drive the controller as a separate task; CaA-guarded apply.
- [x] Add `enable_cluster_controller` (off) + `cluster_controller_tick_interval` dyncfgs (registered in `all_dyncfgs`).
- [x] Boundary tests against a fake ctx (steady no-op, under/over-provision, wrong-shape, union max-not-sum, distinct-shape attribution, CaA-reject + recover) + gate-on slt no-op test (`test/sqllogictest/cluster_controller.slt`, authored for CI).
- [x] `cargo fmt`/`check`/`clippy` clean on `mz-cluster-controller`/`mz-adapter`/`mz-adapter-types`; controller unit tests pass; update tracker.

---

## PR 3 — Graceful reconfiguration strategy + `ALTER` reshape + wait-shim

**Status:** 👀 In review — implementation + review landed (commits `8022d9a`, `d7d0254`); the
2026-06-02 design refinement (durable, controller-honored `ON TIMEOUT`, default `ROLLBACK`) is now
implemented (the three reopened checklist items below are done).

**Goal.** Move graceful (zero-downtime) reconfiguration into the controller as a strategy,
driven by the durable `reconfiguration` record, with hydration-aware cut-over and timeout —
all dark behind the master gate; legacy 3-stage machine still runs when the gate is off.

**Scope (in).**
- **Implement the hydration/frontier ctx methods** (first hydration-dependent strategy): back `ClusterControllerCtx`'s hydration query with `collections_hydrated_for_replicas` / `collections_hydrated_on_replicas` (and frontiers as needed), pulled on demand — the controller asks only about the replicas a tick examines. This is the seam stubbed in PR 2.
- `GracefulReconfigurationStrategy` (pure):
  - Engaged when `reconfiguration` record is present.
  - `desired_replicas`: `target.replication_factor` replicas at `target` shape (size + logging + AZ list).
  - `update_state`: target replicas present **and** hydrated → cut over (`cluster.size := target.{size,...}`, clear record); success precedence holds even past the deadline. Otherwise once `now > deadline` and not yet hydrated, apply the record's `on_timeout`: **`ROLLBACK`** (default) → drop the whole target set, **retain record as tombstone**, stop contributing (parked); **`COMMIT`** → advance `cluster.size` to the target and clear the record anyway. Emit an audit event recording the action.
  - Diff on size + logging + **AZ list** (now diffable thanks to PR 1) + count.
- **`ALTER CLUSTER` reshape (gated):** when the master gate is on and the change is a config-shape change (size/logging/AZ), write the `reconfiguration` record (target + deadline + on_timeout) in one txn and return; leave realized config. Once a record is present, further `ALTER`s **fold into it** (overwrite target + deadline + on_timeout). Non-shape changes (rf-only, workload_class) with no record in flight update realized config directly (as today). Deadline + on-timeout come from the **existing** `WITH (WAIT ...)` surface (kept verbatim — no new token): `WAIT UNTIL READY (TIMEOUT '<dur>', ON TIMEOUT COMMIT|ROLLBACK)` → `deadline = txn_ts + timeout`, `on_timeout` as given; `WAIT FOR '<dur>'` ≡ `UNTIL READY (TIMEOUT '<dur>', ON TIMEOUT COMMIT)`; omitting `WAIT` → `default_cluster_reconfiguration_timeout` + default `ROLLBACK`. **Flip the planner's implicit `ON TIMEOUT` default `COMMIT`→`ROLLBACK` globally** (`OnTimeoutAction::default()`), which also changes the legacy foreground default — the safe, no-surprise-downtime default. **Keep the legacy 3-stage path for gate-off** (don't delete yet — PR 7).
- **Wait-shim:** `enable_background_alter_cluster` off (or foreground UX during rollout) → the session polls the durable `reconfiguration` record (cleared = done; `now > deadline` = timed out) up to its own wait. On → `ALTER` returns immediately. (Polls the record directly so this PR doesn't depend on PR 4's view. Session disconnect no longer aborts the reconfiguration.)
- **Audit:** `ReplicaCreateDropReason::GracefulReconfiguration` → new `CreateOrDropClusterReplicaReasonV1::Reconfiguration`, carried on the controller's create/drop events.

**Scope (out).** No on-refresh/burst. No legacy removal. Rich observability (`SHOW CLUSTERS`,
introspection view) is PR 4.

**Verification.** User-visible first (sqllogictest/testdrive, gate forced on): background `ALTER`
returns immediately / foreground shim waits; replicas transition and cut-over advances `cluster.size`;
`SHOW`/introspection reflect target then realized; timeout leaves a tombstoned record and the
realized set. Platform-check: a mid-flight reconfiguration survives an `environmentd` restart.
Targeted kernel unit tests only for the tricky cases (timeout-vs-hydrated precedence,
refold/ALTER-back, AZ-only vs size+AZ).

**Acceptance criteria.** With the gate off, legacy graceful reconfiguration is unchanged. With the
gate forced on, a background `ALTER` returns immediately, the controller converges/cuts over,
timeouts park as tombstones, and the flow survives restart.

**Checklist.**
- [x] Implement the hydration method of `ClusterControllerCtx` (pulled on demand). Reshaped the speculative PR-2 stub `collections_hydrated_on_replicas(cluster, replicas, collections) -> bool` into `hydrated_replicas(cluster, replicas) -> BTreeSet<ReplicaId>` (per-replica "all current collections hydrated"), backed by `collections_hydrated_for_replicas`/`collections_hydrated_on_replicas` with an empty exclude set. (Frontier/read-ts reads still land with their first consumer, PR 5.)
- [x] Implement `GracefulReconfigurationStrategy` (pure); targeted kernel tests for the tricky cases (timeout-vs-hydrated precedence, partial-hydration, AZ-only shape change, full overlap→cut-over, ALTER-back no-churn).
- [x] Reshape `ALTER CLUSTER` (gated): write/fold `reconfiguration` record; keep legacy path for gate-off; timeout/deadline resolution + dyncfg default.
- [x] Wait-shim polling the record; `enable_background_alter_cluster` dyncfg.
- [x] Audit reason variant (`Reconfiguration`) on create (graceful-desired); old-set drops at cut-over are `Manual` (none-desired), with richer drop-lifecycle attribution deferred to PR 4.
- [x] Integration test authored (`cluster_controller.slt`, gate + background forced on); platform-check **deferred** (see Progress Log).
- [x] `cargo fmt`/`check`/`clippy` clean; update tracker.

Reopened by the 2026-06-02 design refinement (the durable `on_timeout` knob):
- [x] Add `on_timeout: OnTimeoutAction` to durable `ReconfigurationState` — landed as a `fixup!` against PR 1's catalog commit (v86 unshipped → proto revised in place: `ReconfigurationState` field + mirrored `OnTimeoutAction` proto enum + `RustType` conversion; durable/memory mirrors threaded; `OnTimeoutAction` gained serde/ordering derives, shared by both layers like `ClusterSchedule`; v86 encoding snapshot regenerated). Per the fixup-commit convention.
- [x] Honor `on_timeout` in `GracefulReconfigurationStrategy::update_state` (`ROLLBACK` = drop target set + tombstone; `COMMIT` = cut over un-hydrated and clear); kernel tests for `COMMIT`-on-timeout vs `ROLLBACK`-on-timeout (and an overlap-before-deadline parity case). Audit: the action is recorded through the existing reason-carrying create/drop + cut-over ops; the dedicated deadline-carrying *timeout-fired* lifecycle event stays with PR 4's reconfiguration audit lifecycle (see Deferred).
- [x] Flipped planner `OnTimeoutAction::default()` `COMMIT`→`ROLLBACK` (global; also changes the legacy foreground default, which reads the same `default()`); threaded `on_timeout` from the `WITH (WAIT ...)` plan into the written record (a fold overwrites it with the latest `ALTER`'s); `WAIT FOR` desugars to `ON TIMEOUT COMMIT`; no `WAIT` → default `ROLLBACK`. Extended `cluster_controller.slt` (gate + background on) to assert the omitted-`ON TIMEOUT` default and both explicit actions drive a record under the gate (authored for CI).

**Deferred (with reasons).**
- **No new `WITH (TIMEOUT = ...)` token (settled — *not* deferred).** Earlier the implementation deferred a new top-level token to PR 6; per the 2026-06-02 design refinement we keep the existing `WITH (WAIT ...)` surface **permanently** as the only spelling — it already expresses both the deadline (`TIMEOUT`) and the on-timeout action (`ON TIMEOUT`), so a second token would only fragment it. The timeout value already flows from the `WAIT` plumbing; no parser work remains. See the design doc's *Per-`ALTER` timeout* section.
- **Restart-survival platform-check.** The controller is dark by default; a platform-check would have to `ALTER SYSTEM SET enable_cluster_controller = true` env-wide, which would change behavior for **every other check** in the shared suite (the controller would own all replica sets) while the feature is dark. Survival across restart is guaranteed by construction (the `reconfiguration` record is durable catalog state) and is exercised by the controller tests + the slt; a dedicated restart test belongs with the rollout enablement, not the dark suite.
- **Dedicated *timeout-fired* audit event (with the active deadline).** The on-timeout action is already attributable through the existing reason-carrying ops (a `COMMIT` cut-over via `Op::UpdateClusterConfig`; a `ROLLBACK` drop via `Op::DropObjects`, none-desired). A distinct cluster-level *timeout-fired* event recording the action and the active deadline is part of PR 4's reconfiguration audit *lifecycle* (started / finalized / cancelled / timeout-fired), which owns the audit-event surface; adding it here would duplicate that work and split the event family across two PRs.
- **Deterministic ROLLBACK-vs-COMMIT-at-timeout slt.** A timeout-park requires a target that does **not** hydrate before its deadline, but an empty slt cluster hydrates promptly (success precedence then cuts over regardless of the action). The action's behavior *at* a timeout is asserted deterministically in the controller kernel tests (`graceful_commit_on_timeout_cuts_over_unhydrated`, `graceful_timeout_parks_and_drops_target`), where hydration does not race the deadline; the slt asserts only that the three spellings (omitted/`ROLLBACK`/`COMMIT`, plus `WAIT FOR`) are accepted under the gate and drive a record.

> **Superseded:** the implementation's "`ON TIMEOUT COMMIT` collapsed into the tombstone-revert model"
> deferral no longer holds — `on_timeout` is now an honored durable knob (the three reopened
> checklist items above), with the default flipped to `ROLLBACK`. `COMMIT` is no longer silently
> treated as `ROLLBACK`.

---

## PR 4 — Observability: introspection view + `SHOW CLUSTERS` + audit

**Status:** 👀 In review — implementation + review follow-up landed on `cluster-autoscaling`.
Introspection table + `SHOW CLUSTERS` extension + reconfiguration audit lifecycle, all dark
(records stay `None` until the controller is enabled). The review follow-up adds the `cancelled`
lifecycle transition (ALTER-back to the realized shape), corrects the `finalized`-vs-`timed-out`
classification (a `ROLLBACK` record only ever clears on a hydrated success, so it is never
mislabeled `timed-out`; the residual `COMMIT`-late-success imprecision is documented), fixes the
`belongs_to_cluster` ontology cardinality to one-to-one, regenerates the unshipped-`v86` proto
snapshot so the new audit variant round-trips, and updates the `canary-clusters.td` `SHOW CLUSTERS`
row for the new columns. The `ROLLBACK`-timeout audit event remains deferred with a reason (see
Deferred); the other lifecycle transitions are covered. A later follow-up (2026-06-03, same
branch) reworked the introspection relation from the imperatively-packed `BuiltinTable` into a
`BuiltinMaterializedView` over `mz_internal.mz_catalog_raw` (plus a `mz_catalog_server` index),
following the move off imperative builtin-table packing for catalog-derived relations — schema,
ontology, and dark behavior unchanged. See the Progress Log.

**Goal.** Make in-flight reconfigurations observable in SQL, so background `ALTER` (PR 3) is
usable and validatable.

**Scope (in).**
- Surface the durable `reconfiguration` record to SQL: catalog-populate a new builtin **table**
  (working name `mz_internal.mz_cluster_reconfigurations`) with realized vs. target shape,
  replication factor, in-flight flag, active deadline, and (placeholder columns for) burst — fed
  from the new durable fields where `mz_clusters` rows are built. Add a user-facing **view** if a
  friendlier shape is wanted.
- Extend `MZ_SHOW_CLUSTERS` view SQL to join the new relation so `SHOW CLUSTERS` answers
  "what did I ask for / what's there now / in progress, done, or timed out" — including the
  current (realized) size and the target size when a reconfiguration is in flight.
- Round out reconfiguration audit lifecycle (started / replica created / replica dropped /
  finalized / cancelled / timeout-fired) using the reason-carrying events; record the active
  deadline on start/timeout events.

**Scope (out).** Burst columns are placeholders here; populated in PR 6.

**Verification.** Testdrive: query the view and `SHOW CLUSTERS` before/during/after a forced-on
reconfiguration and at timeout; assert audit-log rows. Builtin-migration test green (new builtins
need no migration; ordering test passes).

**Acceptance criteria.** During a (forced-on) background reconfiguration, the view and
`SHOW CLUSTERS` show current vs. target and an in-flight/timed-out indicator; audit log captures
the lifecycle.

**Checklist.**
- [x] Add the introspection relation; derive it from the durable records. (`mz_internal.mz_cluster_reconfigurations`: realized vs. target size/rf/AZ list, in-flight flag, active deadline, placeholder `burst_size`. New builtin → no migration. No separate user view added — the relation is the introspection surface and `SHOW CLUSTERS` is the friendly shape. **Reworked 2026-06-03** from an imperatively-packed `BuiltinTable` into a `BuiltinMaterializedView` over `mz_internal.mz_catalog_raw` (with a companion `mz_catalog_server` index), matching the convention of moving catalog-derived relations off imperative builtin-table packing onto views over the raw catalog — schema/ontology unchanged, dark behavior preserved. See the Progress Log.)
- [x] Extend `MZ_SHOW_CLUSTERS` SQL; keep `show.rs` template pointing at the view. (LEFT JOINs the new table to add `current_size`, `target_size`, `reconfiguration_in_flight`; `show.rs` projects the new columns. The view stays **non-temporal** — the indexed `mz_show_clusters` cannot use `mz_now()`, so the timed-out-vs-in-progress split is read from `mz_cluster_reconfigurations.reconfiguration_deadline`, not computed in the view.)
- [x] Reconfiguration audit lifecycle events (with deadline on start/timeout/cancel). (New `EventDetails::AlterClusterReconfigurationV1` with a `started`/`finalized`/`timed-out`/`cancelled` transition + target size/rf + optional deadline; proto variant added in place to the unshipped v86 (`objects.rs` + `objects_v86.rs`, hashes regenerated, `v86` snapshot regenerated) with `RustType` conversions. Emitted from the `Op::UpdateClusterConfig` durable transition, classified purely from the before/after `reconfiguration` record + the realized shape + the write timestamp vs. the deadline — covers record-write/re-target (`started`, with deadline), re-target *back* to the realized shape (`cancelled`, with deadline), success cut-over (`finalized`), and `COMMIT`-on-timeout cut-over (`timed-out`, with deadline). A `ROLLBACK` record only ever clears on a hydrated success (rollback never clears on a timeout), so it is classified `finalized` regardless of the deadline; only a `COMMIT` clear past the deadline is `timed-out`. The deadline is a heuristic under `COMMIT`: a hydrated-late `COMMIT` success is indistinguishable from a timeout-committed un-hydrated target at the agnostic apply site and surfaces as `timed-out` (documented on the event variants and asserted in the unit test). Replica create/drop attribution (`Reconfiguration`) already lands from PR 3. The `ROLLBACK`-timeout event is deferred, see below.)
- [x] Testdrive + audit assertions. (Authored for CI: `cluster_controller.slt` gains an observability section asserting the introspection table + `SHOW CLUSTERS` shape on a steady and a reconfigured cluster, the audit lifecycle rows for a success (`started`+deadline, `finalized`) and for an ALTER-back cancel (`started`, `cancelled`, `finalized`); `show_clusters.slt` asserts the new gate-off columns are benign; `canary-clusters.td` updated for the new `SHOW CLUSTERS` columns. A `classify_reconfiguration_transition` unit test in `mz-adapter` covers started/finalized/timed-out/cancelled/re-target/no-op deterministically, including the `ROLLBACK`-late-success and the documented `COMMIT`-late-success imprecision.)
- [x] `cargo fmt`/`check` clean; update tracker.

**Deferred (with reasons).**
- **`ROLLBACK`-timeout audit event.** The lifecycle events are emitted by classifying the durable
  `reconfiguration` before/after at the single `Op::UpdateClusterConfig` write site. A `ROLLBACK`
  timeout performs **no config write** — the strategy's `update_state` returns
  `StateWrite::default()`, the record is retained as a tombstone unchanged, and only the in-flight
  target replicas are dropped (none-desired). With no config transition to hang the event off, a
  dedicated `timed-out (rollback)` event would need either a fire-once signal threaded through the
  controller `StateWrite` seam (widening the agnostic interface with audit-only vocabulary) or a
  durable tombstone-stamp on the record (a PR-1 schema change). Both are larger than PR 4's
  observability scope. The rollback is already observable: the tombstoned record (past deadline,
  `reconfiguration_in_flight = true`) surfaces in `mz_cluster_reconfigurations` and `SHOW CLUSTERS`,
  and the target-set drops carry the none-desired reason. Lands with the burst lifecycle (PR 6) or
  a focused follow-up once the seam shape for it is settled.
- **Live `mz_now()`-based timed-out indicator in `SHOW CLUSTERS`.** The `mz_show_clusters` view is
  indexed in `mz_catalog_server`, and an index cannot be built over a relation that uses `mz_now()`,
  so the view exposes only a boolean `reconfiguration_in_flight`. The deadline needed to split
  in-progress from timed-out lives on `mz_cluster_reconfigurations.reconfiguration_deadline`; a user
  compares it to `mz_now()` there. Surfacing a derived status in `SHOW CLUSTERS` would require
  dropping the index or a non-indexed companion view; not pursued in this PR.
- **Autogenerated catalog-doc / index-accounting snapshots.** The slt snapshots that enumerate every
  builtin and the catalog-server index dependency columns (`autogenerated/mz_internal.slt`,
  `mz_catalog_server_index_accounting.slt`, `information_schema_tables.slt`, `oid.slt`, `catalog.td`)
  were hand-updated to include the new table/columns and the bumped table count, matching the
  generators' format; they are authoritative only after the live-server regen steps in CI confirm
  byte-equality. Not runnable locally without a full environmentd.

---

## PR 5 — `ON REFRESH` scheduling as a strategy

**Status:** ⬜ Not started

**Goal.** Port the existing `ON REFRESH` policy into the controller as `OnRefreshStrategy`, so
the framework owns all three existing behaviours — still dark; legacy `cluster_scheduling.rs`
runs when the gate is off.

**Scope (in).**
- `OnRefreshStrategy` (pure): contributes one replica at `cluster.size` while the cluster is
  inside a refresh window, nothing otherwise — from REFRESH MV write frontiers + the configured
  hydration-time estimate + current read timestamp. Does not apply to MANUAL clusters. Pull the
  read-ts / frontiers via `ClusterControllerCtx` on demand (the same signals `check_refresh_policy`
  reads today), not a pushed snapshot.
- Model: a scheduled cluster's `replication_factor` is the controller's domain; the baseline
  desires nothing and `OnRefreshStrategy` is the sole contributor. So enabling-by-flag is safe
  regardless of the stored rf: the strategy's `update_state` **normalizes scheduled-cluster rf to
  0 at runtime** (self-healing — no migration, and it makes `mz_clusters.replication_factor` read 0).
- Keep legacy `cluster_scheduling.rs` for gate-off (removed in PR 7).

**Scope (out).** No legacy removal (PR 7). No rf migration — rf is normalized by the controller at runtime.

**Verification.** User-visible first (gate forced on): a scheduled cluster turns on inside its
refresh window and off outside, matching today's behaviour. A few kernel unit tests for the
window decision (in/out-of-window; frontier vs. estimate) where they're cheap.

**Acceptance criteria.** With the gate off, scheduling is unchanged. With the gate forced on, an
`ON REFRESH` cluster's replica lifecycle matches today's behaviour, driven by the strategy.

**Checklist.**
- [ ] Implement `OnRefreshStrategy` (pure); targeted parity unit tests for the window decision.
- [ ] Normalize scheduled-cluster `replication_factor` to 0 via `update_state` (so flag-enablement needs no migration).
- [ ] Back the read-ts / frontier ctx methods used by the refresh strategy.
- [ ] Integration test (gate forced on) for window on/off.
- [ ] `cargo fmt`/`check` clean; update tracker.

---

## PR 6 — Hydration burst (last feature PR; lands dark)

**Status:** ⬜ Not started

**Goal.** Add the new `ON HYDRATION` burst capability: the SQL surface, the strategy, the
break-glass flag, validations, and burst observability/audit. This completes the feature; like
PRs 2–5 it lands **behind gates, defaults off** — no production behaviour change. After it merges,
the whole feature is in `main` and ready to be turned on at runtime (see *Operational rollout*).

**Scope (in).**
- **SQL surface:** `AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = '...', LINGER DURATION = '...'))`
  at `CREATE CLUSTER` and `ALTER CLUSTER ... SET (...)`. AST (`ClusterOptionName::AutoScalingStrategy`
  + nested option parsing), parser, planner → the `auto_scaling_strategy` durable field (PR 1).
  Whole block set/replaced atomically; omitted/empty disables. `SHOW CREATE CLUSTER` renders it.
  Gated by `enable_auto_scaling_strategy`.
- **Validations (parse/plan):** reject `HYDRATION SIZE == cluster SIZE`; reject
  `AUTO SCALING STRATEGY` combined with `SCHEDULE = ('on-refresh', ...)`.
- `HydrationBurstStrategy` (pure): when `on_hydration` set, cluster on (rf > 0), and no
  realized-config replica has all current objects hydrated → write `burst` record (size, linger).
  Desires one extra replica at `HYDRATION SIZE` while the record is present. `update_state`:
  record the timestamp when a steady replica first hydrates; once `now > that + linger` clear the
  `burst` record (teardown). Re-arm (reset the timestamp) if steady replicas go un-hydrated again.
  No TTL on the burst replica (accepted: permanent oversize if steady can never hydrate). Burst is
  **not** suppressed during a reconfiguration (coexists). The burst replica is an ordinary replica —
  the union/diff reconciler creates and tears it down by shape+count; no tag or special identity
  (decision 5).
- **Break-glass** `enable_hydration_burst` (default true at GA; false disables the strategy
  env-wide, leaving graceful/on-refresh untouched).
- **Observability/audit:** populate the burst columns in the PR 4 view; `SHOW CLUSTERS` burst
  indication; audit events for burst started / replica created / replica dropped.

**Scope (out).** Multiple burst replicas; `AUTO SCALING STRATEGY` + `ON REFRESH` (both rejected,
per design). Turning the feature on (rollout) and removing legacy paths (PR 7) come later.

**Verification.** User-visible first (gates + `enable_auto_scaling_strategy` forced on in tests): a
burst replica appears while steady is un-hydrated and tears down after its linger; the break-glass
flag disables it; rejected combinations error at plan time; `SHOW CLUSTERS`/introspection show the
burst. Platform-check for burst surviving restart. A few kernel unit tests for arm/linger/re-arm/teardown.

**Acceptance criteria.** With the gates forced on, a user can set `AUTO SCALING STRATEGY (ON HYDRATION (...))`;
a burst replica spins up when warranted and tears down after linger with no user action; the
break-glass flag disables it; rejected combinations error. With the gates off (default), nothing changes.

**Checklist.**
- [ ] SQL: AST + parser + planner for `AUTO SCALING STRATEGY`/`ON HYDRATION`; `SHOW CREATE CLUSTER`; `enable_auto_scaling_strategy` gate.
- [ ] Validations: HYDRATION SIZE == SIZE; AUTO SCALING + ON REFRESH.
- [ ] `HydrationBurstStrategy` (pure); targeted kernel tests (arm/linger/re-arm/teardown).
- [ ] `enable_hydration_burst` break-glass dyncfg.
- [ ] Burst observability columns + audit events.
- [ ] Integration tests (burst lifecycle, break-glass, rejections).
- [ ] `cargo fmt`/`check` clean; update tracker.

---

## Operational rollout — between PR 6 and PR 7 (not a code change)

With the whole feature in `main` behind gates defaulting **off**, enabling it is an **operational**
step, not a PR: flip the dyncfgs on at runtime via LaunchDarkly — `enable_cluster_controller`, then
`enable_background_alter_cluster`, then `enable_auto_scaling_strategy` (leave `enable_hydration_burst`
at its default). Roll out staging → a few prod environments → fleet, watching the introspection
view, audit log, and replica metrics. The legacy paths stay in the binary the whole time, so
rollback is a flag flip. **Let it bake in prod until we're confident.** Only then does PR 7 land.

Notes:
- The gates are independent, so enablement can be staged (master / graceful + on-refresh first,
  burst later) at the operator's discretion.
- `replication_factor` for scheduled clusters is normalized by the controller at runtime (PR 5),
  so flag-enablement is safe with no migration.
- Rollback (flag → off) is cleanest when no controller-driven reconfiguration or burst is in flight.

---

## PR 7 — Flip defaults to true & remove legacy paths (only after prod has baked)

**Status:** ⬜ Not started — **do not start until the runtime-enabled feature has baked
successfully in prod for the agreed period.**

**Goal.** Make the controller the permanent owner in code and delete the now-unused legacy
machinery. This is irreversible (it removes the fallback), so it follows a successful prod bake —
not just local parity.

**Scope (in).**
- Flip the dyncfg **defaults** to match the baked-in prod state: `enable_cluster_controller` → true,
  `enable_background_alter_cluster` → true, `enable_auto_scaling_strategy` → true
  (`enable_hydration_burst` already defaults true).
- **Remove legacy code:** the 3-stage graceful machine (`WaitForHydrated`/`Finalize`/pending
  creation + `-pending` suffix + `pending_cluster_alters`), `cluster_scheduling.rs` policy +
  `Message::CheckSchedulingPolicies`/`SchedulingDecisions` + the scheduling interval, and the
  now-obsolete `AlterClusterWhilePendingReplicas` reject. The wait-shim becomes the only foreground path.
- **Migration (drop `pending`):** remove `pending: bool` from durable `ReplicaLocation::Managed`
  (proto change + migration) — safe now that the legacy stage machine that used it is gone.
  (Scheduled-cluster `replication_factor` was already normalized to 0 by the controller at runtime
  in PR 5; add a tidy-up data migration only if any non-zero values remain.)
- Update/remove tests pinned to legacy behaviour (`enable_cluster_controller=false` pins, removed
  legacy workflows).

**Scope (out).** Nothing new — this is pure cleanup.

**Verification.** Full relevant suite green with the new defaults (sqllogictest cluster suites,
testdrive, platform-checks for restart/upgrade). Upgrade test for the `pending`-drop migration.
Confirm `mz_clusters.replication_factor` reads 0 for scheduled clusters while a replica runs
(`mz_cluster_replicas` is authoritative).

**Acceptance criteria.** Defaults are on; legacy paths are gone; `pending` dropped; full suite green.
Because the fallback is removed, this is gated on a successful prod bake, not on local parity alone.

**Checklist.**
- [ ] Confirm prod ran the runtime-enabled feature successfully for the agreed bake period.
- [ ] Flip dyncfg defaults (`enable_cluster_controller`, `enable_background_alter_cluster`, `enable_auto_scaling_strategy`) to true.
- [ ] Remove legacy graceful machine + scheduling policy + obsolete reject/messages/interval.
- [ ] Migration: drop `pending` (+ tidy rf only if needed).
- [ ] Update/remove tests pinned to legacy behaviour.
- [ ] Full suite green (CI); upgrade + platform checks.
- [ ] Update tracker.

---

## Progress log

Append dated entries as work lands. Newest first.

- _2026-06-03_ — **PR 4 follow-up: reconfiguration relation reworked into a materialized view**
  (fixup against PR 4's observability commit, same branch). The team has moved away from
  imperatively-populated builtin relations for catalog-derived data; PR 4 had introduced
  `mz_internal.mz_cluster_reconfigurations` as a catalog-populated `BuiltinTable` packed in
  `pack_cluster_update`. It is now a `BuiltinMaterializedView` over the builtin source
  `mz_internal.mz_catalog_raw`, mirroring `MZ_CLUSTER_WORKLOAD_CLASSES` (its companion
  `MZ_CLUSTER_RECONFIGURATIONS_IND` in `mz_catalog_server` is mirrored too). The MV reads the
  managed variant straight from the durable `Cluster` record: realized config from
  `data->'value'->'config'->'variant'->'Managed'` (`size`, `replication_factor`,
  `availability_zones`), the in-flight target from `…->'Managed'->'reconfiguration'->'target'`,
  the deadline from `…->'reconfiguration'->'deadline'`, and `burst_size` from
  `…->'Managed'->'burst'`. Externally-tagged variant ⇒ `->'Managed' IS NOT NULL` selects only
  managed clusters; an absent `Option` serializes to JSON `null`, so in-flight presence is
  `reconfiguration != 'null'`. `target_*` COALESCE onto the realized config when no record is in
  flight, so with the controller dark the row is identical to what the table produced
  (current==target, not-in-flight, NULL deadline/burst). AZ pools are rebuilt as ordered
  `text list`s via `jsonb_array_elements_text(...) AS az WITH ORDINALITY` + `list_agg(az.value
  ORDER BY az.ordinality)` (`jsonb_array_elements_text` is a `CallTable` table func, so the
  non-legacy `WITH ORDINALITY` path applies), with an unpinned pool COALESCEd to the empty list.
  The output `RelationDesc` (columns/types/nullability/key), `column_comments`, and the
  one-to-one `belongs_to_cluster` ontology are byte-for-byte unchanged, so `mz_show_clusters`
  (LEFT JOIN, indexed) and `show.rs` are untouched. Removed the imperative path
  (`pack_cluster_reconfiguration_row` + its `pack_cluster_update` call site and the now-unused
  `MZ_CLUSTER_RECONFIGURATIONS` / `ClusterVariantManaged` imports in
  `builtin_table_updates.rs`); rewired `BUILTINS_STATIC` (Table → MaterializedView, plus the
  Index registered next to `MZ_CLUSTERS_IND`); renamed the OID
  `TABLE_MZ_CLUSTER_RECONFIGURATIONS_OID` → `MV_MZ_CLUSTER_RECONFIGURATIONS_OID` (value 17088
  kept) and added `INDEX_MZ_CLUSTER_RECONFIGURATIONS_IND_OID = 17089`. **No
  `builtin_schema_migration` entry** — the relation is unshipped (introduced on this branch in
  PR 4, never in a released version), so it is defined correctly in place, not migrated; the
  migration machinery is only for version upgrades of already-shipped persisted builtins.
  **Cheap checks run:** `cargo fmt`; `cargo check -p mz-catalog -p mz-adapter -p
  mz-pgrepr-consts` clean; `bin/lint-cargo` clean; `mz-catalog` `builtin::tests` green (incl.
  `test_builtins_static_dependency_order`, `test_ontology_consistency`,
  `test_mz_sources_fingerprint_changes_with_new_builtin_source`). **Authored for CI (not run —
  needs environmentd):** hand-updated the catalog snapshots — `information_schema_tables.slt`
  (BASE TABLE → MATERIALIZED VIEW), `catalog.td` (moved to the materialized-views section, table
  count 54→53, index count 267→268), `oid.slt` (added the index OID row), and a new index row in
  `mz_catalog_server_index_accounting.slt` (its per-column block + the `mz_show_clusters` column
  dependencies are unchanged because the schema is identical; the `sNNN` global-IDs across that
  file shift and are authoritative only after CI live-server `--rewrite-results`). The dark
  behavior assertions in `cluster_controller.slt` / `show_clusters.slt` / `canary-clusters.td`
  need no edits — the MV yields the same rows. Did **not** run the slt/testdrive suites or a full
  optimized build.

- _2026-06-03_ — **PR 4 follow-up review addressed** (fixup on `cluster-autoscaling`). Adversarial
  review of the Table → MaterializedView conversion returned **approve, no must-fix**. Re-verified
  the high-risk dimensions against the live tree: the JSON paths match the catalog-protos
  serialization (`StateUpdateKind` internally tagged on `kind`; `ClusterVariant` externally tagged
  ⇒ `…->'variant'->'Managed'`; `ManagedCluster` / `ReconfigurationState` / `ReconfigurationTarget` /
  `BurstState` field names all match), `jsonb_array_elements_text` yields a text column named
  `value` so the `list_agg(... ORDER BY ordinality)` rebuilds a `text list`, and the per-column
  block in `autogenerated/mz_internal.slt` needs no edit (schema identical; indexes do not appear
  in its `objects` listing). The two review findings are **deferred, not fixed**, with reasoning:
  (1) the `sNNN` system-item global IDs in `mz_catalog_server_index_accounting.slt` shift because
  registering the new index in `BUILTINS_STATIC` bumps every item allocated after it (e.g.
  `mz_recent_storage_usage` s806→s807) and the new index's relation reference is a placeholder —
  these IDs are positional and authoritative only after CI live-server `--rewrite-results`; a
  partial hand-edit cannot make the file self-consistent (the relation's true global ID is not
  knowable without booting `environmentd`, which the operating rules say not to block on), so the
  whole file is left for CI regen. (2) the sibling template's companion index
  `MZ_CLUSTER_WORKLOAD_CLASSES_IND` is defined but unregistered in `BUILTINS_STATIC` (inert) — a
  pre-existing condition out of scope for this correction; whether to register it (and add it to
  the index-accounting snapshot) is tracked separately.

- _2026-06-03_ — **PR 4 implemented** (observability: introspection table + `SHOW CLUSTERS` +
  reconfiguration audit lifecycle, all dark). **Introspection table:** new catalog-populated builtin
  `mz_internal.mz_cluster_reconfigurations` (OID 17088), one row per managed cluster, packed in
  `pack_cluster_update` from the realized config plus the durable `reconfiguration`/`burst` records —
  `current_*` (size / rf / AZ list), `target_*` (the record's target when in flight, else the realized
  config), `reconfiguration_in_flight`, `reconfiguration_deadline` (`mz_timestamp`, NULL when none),
  and a placeholder `burst_size` for PR 6. New builtin → **no migration**; with the controller dark
  the records stay `None`, so `current == target` and nothing is in flight. **`SHOW CLUSTERS`:**
  `mz_show_clusters` now LEFT JOINs the new table and exposes `current_size`, `target_size`,
  `reconfiguration_in_flight`; `show.rs` projects them. The view is **indexed**, so it stays
  non-temporal (no `mz_now()`); the timed-out-vs-in-progress split is read from the table's deadline.
  **Audit lifecycle:** new `mz_audit_log::EventDetails::AlterClusterReconfigurationV1`
  (`started`/`finalized`/`timed-out` transition + target size/rf + optional deadline), with the proto
  variant added in place to the unshipped v86 (`objects.rs` + `objects_v86.rs`, both
  `objects_hashes.json` entries regenerated to `11a6859ce52f01809ed4a317ec1735aa`) and `RustType`
  conversions in `catalog-protos/audit_log.rs`. Emitted from the `Op::UpdateClusterConfig` durable
  write site via `Catalog::classify_reconfiguration_transition`, classified purely from the
  before/after `reconfiguration` record and the write timestamp vs. the deadline: record write /
  re-target → `started` (with deadline), success cut-over → `finalized`, `COMMIT`-on-timeout cut-over
  → `timed-out` (with deadline). The replica create/drop attribution (`Reconfiguration`) already lands
  from PR 3. Dark by construction — a record only ever moves under the master gate. **Tests:**
  `cluster_controller.slt` gained an observability section (steady-state introspection row =
  current==target/not-in-flight, `SHOW CLUSTERS` new columns, post-reconfiguration current==target,
  and the `started`+deadline / `finalized` audit rows) and `show_clusters.slt` asserts the new
  gate-off columns are benign (current==target, NULL for unmanaged) — both authored for CI. A
  `classify_reconfiguration_transition` unit test (`mz-adapter`) covers started/finalized/timed-out/
  re-target/no-op deterministically and passes. Updated the hand-maintained catalog snapshots for the
  new table/columns + bumped system-table count (`autogenerated/mz_internal.slt` query block + the
  DISTINCT-object list, `mz_catalog_server_index_accounting.slt` column blocks,
  `information_schema_tables.slt`, `oid.slt`, `catalog.td`) and added the `mz_cluster_reconfigurations`
  doc block to `doc/.../system-catalog/mz_internal.md`. **Cheap checks:** `cargo fmt` + `cargo
  check`/`clippy` clean on `mz-adapter`/`mz-catalog`/`mz-catalog-protos`/`mz-audit-log`/`mz-sql`/
  `mz-pgrepr-consts`; `bin/lint-cargo` clean; `Cargo.lock` unchanged (no new deps); catalog
  `test_proto_serialization_stability`, `durable::upgrade::tests`, and `builtin::` (incl.
  `test_builtins_static_dependency_order`, `test_ontology_consistency`) green; catalog-protos +
  audit-log unit tests green. Did **not** run the slt/testdrive suites or the live-server catalog-doc
  regen (needs a full environmentd). **Deferred (see the PR-4 section):** the dedicated
  `ROLLBACK`-timeout audit event (no config write to hang it off; needs a seam signal or a durable
  tombstone-stamp — larger than PR 4's scope; the rollback is already visible via the tombstoned
  record + none-desired drops); a live `mz_now()` timed-out indicator in `SHOW CLUSTERS` (the view is
  indexed); and the live-server regen of the autogenerated catalog snapshots (hand-updated here to
  match the generators).

- _2026-06-02_ — **PR 3 review follow-up (on-timeout knob; fixup commit, same branch)** — addresses
  the adversarial review of the durable, controller-honored `ON TIMEOUT` work. **Doc correctness
  (major):** rewrote the stale `**Timeout action.**` paragraph on `reshape_alter_cluster_managed`,
  which still claimed `COMMIT` was "collapsed into the tombstone-revert model". It now states the
  current contract — the record carries `on_timeout` (default `ROLLBACK`) and the controller applies
  it at the deadline only if the target has not hydrated (`ROLLBACK` drops the target set + retains the
  tombstone, `COMMIT` cuts the realized config over to the un-hydrated target and clears the record;
  success always takes precedence). **Seam coverage (minor):** added two `FakeCtx`-driven controller
  tests that force `now` past the deadline with an un-hydrated target and drive `reconcile` end-to-end:
  `graceful_rollback_at_timeout_drops_target_through_seam` (the in-flight target replica is dropped via
  `apply`, the realized set reverts, the record is retained as a tombstone, second tick is a no-op) and
  `graceful_commit_at_timeout_cuts_over_through_seam` (phase 1 advances `size` to the target and clears
  the record, phase 2 drops the old replica, second tick converges). 21 controller tests total, all
  pass. **Fold note (nit):** sharpened the `reshape_alter_cluster_managed` comment to state explicitly
  that — unlike the target, which folds per-dimension — the deadline and `on_timeout` are resolved
  fresh and replaced wholesale by the latest `ALTER`'s `WAIT` clause. **Cheap checks:** `cargo
  fmt --check` + `cargo check`/`clippy` clean on `mz-cluster-controller` and `mz-adapter`; `Cargo.lock`
  unchanged. No scope change; stays dark behind the master gate.
- _2026-06-02_ — **PR 3 completed: durable, controller-honored `ON TIMEOUT` (default `ROLLBACK`)**
  — the three reopened checklist items, in two commits on `cluster-autoscaling`. **(1) PR-1
  `fixup!` commit** (against the v85→v86 catalog commit): `on_timeout: OnTimeoutAction` added to
  the durable `ReconfigurationState`. v86 is unshipped, so the proto is revised in place —
  `ReconfigurationState` gains the field and a mirrored `OnTimeoutAction` proto enum
  (`objects.rs` + the `objects_v86.rs` snapshot, both hashes updated in `objects_hashes.json`),
  with the `RustType` conversion alongside the other autoscaling types in catalog-protos
  `serialization.rs`. The durable serialization, the in-memory mirror and its `From` conversions,
  and the adapter↔controller record conversions all thread the field; `mz_sql::plan::OnTimeoutAction`
  gained `Copy`/serde/ordering derives so both the durable and memory layers can reference it without
  conversion, like `ClusterSchedule`. The v86 encoding snapshot (`objects_v86.txt`) was regenerated
  (decoding the old snapshot into the new struct fails on the missing field — expected for an
  unshipped version). The implicit default stays `COMMIT` in this commit (behavior-preserving). **(2)
  PR-3 commit:** `GracefulReconfigurationStrategy::update_state` now honors `on_timeout` — past the
  deadline un-hydrated, `Rollback` parks (drops the target set, tombstones the record; the existing
  behavior) and `Commit` cuts over the un-hydrated target and clears the record; `desired_replicas`
  keeps the target desired under `Commit` (it becomes the realized set at cut-over) and drops it under
  `Rollback`. Success precedence is unchanged (a hydrated target cuts over regardless of the action).
  The controller carries its own `OnTimeout` enum (no `mz_sql` dep in the pure crate); the adapter
  driver maps `OnTimeoutAction`↔`OnTimeout`. The planner's `OnTimeoutAction::default()` flipped
  `COMMIT`→`ROLLBACK` globally (also the legacy foreground default, which reads the same `default()` —
  no test relied on the implicit default, all `WAIT UNTIL READY` tests pass `ON TIMEOUT` explicitly).
  The reshape threads the action from the `WITH (WAIT ...)` plan into the record (`WAIT FOR` →
  `COMMIT`; no `WAIT` → default `ROLLBACK`; a fold overwrites it with the latest `ALTER`'s).
  **Tests:** two new graceful kernel tests (`graceful_commit_on_timeout_cuts_over_unhydrated`,
  `graceful_rollback_on_timeout_before_deadline_still_overlaps`; 19 controller tests total, all pass)
  plus the existing `graceful_timeout_parks_and_drops_target` as the `ROLLBACK`-at-timeout case;
  `cluster_controller.slt` extended with an `ON TIMEOUT` section (omitted default + both explicit
  actions + `WAIT FOR`, authored for CI). **Cheap checks:** `cargo fmt` + `cargo check`/`clippy` clean
  on `mz-cluster-controller`/`mz-adapter`/`mz-sql`/`mz-catalog`/`mz-catalog-protos`; `bin/lint-cargo`
  clean; `Cargo.lock` unchanged; the catalog `durable::` suite (163 passed) and the adapter cluster
  fold unit tests pass. Did **not** run the slt suite or the full optimized build. **Deferred (see the
  PR-3 section):** the dedicated deadline-carrying *timeout-fired* audit event (PR 4's reconfiguration
  audit lifecycle owns the event family); the action is already attributable through the existing
  reason-carrying ops. A deterministic ROLLBACK-vs-COMMIT-*at-timeout* slt is not feasible (an empty
  slt cluster hydrates before any deadline) — that behavior is covered by the kernel tests.
  **Fixup note:** the `on_timeout` durable field is a `fixup!` against the PR-1 catalog commit and
  should be squashed into it when PR 1 is finalized.

- _2026-06-02_ — **PR 3 design refinement (docs only): keep the existing `WITH (WAIT ...)` syntax;
  `ON TIMEOUT` becomes a durable, controller-honored knob defaulting to `ROLLBACK`.** Settled the two
  timeout-related PR-3 review items in the design rather than carrying them as deferrals.
  (1) **No new token:** we do *not* add a top-level `WITH (TIMEOUT = ...)`; the existing
  `WITH (WAIT FOR '<dur>')` / `WITH (WAIT UNTIL READY (TIMEOUT '<dur>', ON TIMEOUT COMMIT|ROLLBACK))`
  surface is the permanent spelling, with `WAIT FOR` defined as sugar for
  `UNTIL READY (TIMEOUT '<dur>', ON TIMEOUT COMMIT)` (it loses only the now-meaningless "wait the full
  duration even when ready early" property). (2) **Honor `on_timeout`:** instead of always reverting,
  the controller now applies the action — added as a durable field on `ReconfigurationState`, written
  by the reshape from the `WITH (WAIT ...)` plan, and applied in `update_state` (`ROLLBACK` = drop the
  whole target set + tombstone; `COMMIT` = cut over un-hydrated and clear), with an audit event for the
  action taken. Success precedence is unchanged. (3) **Default flip:** the implicit `ON TIMEOUT` default
  goes `COMMIT`→`ROLLBACK` **globally** (planner `OnTimeoutAction::default()`) — a deliberate
  safe-default change that also affects the **legacy foreground path** (chosen so the same syntax never
  means different things across the gate, and so a timeout never silently induces downtime by cutting
  over to an unhydrated target). Design doc updated (*Per-`ALTER` timeout*, *Stuck reconfiguration*,
  *Notable user-facing changes*, success criteria, dyncfgs). PR 3 reopened to 🚧; remaining code work
  (durable field as a PR-1 `fixup!`, strategy honoring + audit, the default flip + `on_timeout`
  threading) is tracked in the PR 3 checklist. **No code in this entry — docs only.**

- _2026-06-02_ — **PR 3 review follow-up** (fixup commit, same branch; addresses the adversarial
  review). **Fold correctness:** a fold of an `ALTER` issued while a `reconfiguration` is in flight
  no longer reverts dimensions the `ALTER` did not mention. `reshape_alter_cluster_managed` now
  overlays the new `ALTER` onto the **in-flight target** — a dimension left `Unchanged` keeps the
  in-flight target's value rather than the realized (pre-reconfiguration) value `new_config` carried —
  via the new pure helper `fold_reconfiguration_target` (unit-tested: no-record passthrough, rf-only
  keeps the in-flight shape, all-set overwrite, all-unchanged ALTER-back). Previously an rf-only or
  workload_class-only `ALTER` mid-flight silently discarded the in-flight size/AZ/logging. **Wait-shim
  vs. success-precedence:** the foreground shim no longer reports a spurious timeout on a
  reconfiguration the controller is about to (or just did) complete. Past the deadline with the record
  still present it grants **one grace re-poll** (`past_deadline_grace_used`) before erroring, so the
  controller's success-precedence cut-over (hydrated-past-deadline still cuts over) can clear the
  record first; the strategy's tombstone is stable, so a record still present after the grace re-poll
  is a genuine timeout. **`ON TIMEOUT COMMIT`:** documented as intentionally collapsed into the
  tombstone-revert (`ROLLBACK`) model on `reshape_alter_cluster_managed` and the Deferred list —
  committing a not-fully-hydrated target would break the HA guarantee the overlap exists for. **slt:**
  the second graceful `ALTER` is now a genuine reshape to a third size (was an identical no-op that
  early-returned before the reshape branch, proving nothing about record-clearing). **Hydration probe:**
  documented why `Coordinator::hydrated_replicas` must probe per-replica (the compute/storage APIs
  collapse a replica list to a single "hydrated on any" bool, so a batched call would lose the
  per-replica granularity the cut-over needs); phase-2 already reuses the phase-1 hydration when phase
  1 wrote nothing. Dropped a redundant `.clone()` on the `Copy` `cluster_alter_check_ready_interval()`.
  Cheap checks re-run: `cargo fmt --check` + `cargo check`/`clippy` clean on `mz-cluster-controller`
  and `mz-adapter`; new fold unit tests + the 17 controller tests pass.
- _2026-06-02_ — **PR 3 implemented** (graceful reconfiguration strategy + `ALTER` reshape +
  wait-shim, all dark behind the master gate). **Strategy:** new pure `GracefulReconfigurationStrategy`
  in `mz-cluster-controller` — engaged whenever the durable `reconfiguration` record is present;
  `desired_replicas` contributes `target.replication_factor` replicas at the target shape (size +
  logging + AZ list, all diffable thanks to PR 1) on top of the baseline's realized set (the
  hydrate-overlap), and stops contributing them on timeout (`now > deadline` && target not fully
  hydrated → park, the controller drops the in-flight target set, the record is retained as a
  tombstone); `update_state` cuts over (`cluster.{size,rf,az,logging} := target`, clears the record)
  once the target replicas are **all** present and hydrated — success takes precedence over the
  deadline. Registered in `ClusterController::new()` alongside the baseline. **Hydration seam:** the
  PR-2 stub `collections_hydrated_on_replicas(cluster, replicas, collections) -> bool` (whose
  `collections` include-list the underlying controller APIs cannot express) was reshaped into
  `hydrated_replicas(cluster, replicas) -> BTreeSet<ReplicaId>` — "of these replicas, which have all
  current non-transient collections on the cluster hydrated". The controller pulls it
  **on demand**: `enrich_hydration` probes a cluster's replicas only while a `reconfiguration` is in
  flight (steady clusters are never probed), and threads the result into the new live-signal field
  `ClusterState::hydrated_replicas` (excluded from the CaA `expected` witness — it is a signal, not
  durable state). The adapter driver backs it per-replica against
  `ComputeController::collections_hydrated_for_replicas` + `StorageController::collections_hydrated_on_replicas`
  with an empty exclude set (a replica counts as hydrated iff both report it). **`ALTER` reshape:**
  when `enable_cluster_controller` is on and a managed→managed `ALTER` touches a replica's config
  shape (SIZE / logging / AVAILABILITY ZONES) — or any `ALTER` while a record is already in flight —
  it is routed to `reshape_alter_cluster_managed`, which writes/folds the `reconfiguration` record
  (full target shape + deadline) onto the **realized** config in one txn (only `workload_class`, which
  needs no overlap, is applied immediately) and leaves the realized shape in place; the legacy
  3-stage machine still runs for gate-off and for non-shape changes (rf-only, workload_class) with no
  record in flight. Deadline = `now + timeout`, timeout from the existing `WITH (WAIT ...)` strategy
  or the new `default_cluster_reconfiguration_timeout` dyncfg. **Wait-shim:** new
  `ClusterStage::AwaitReconfiguration` polls the durable record at `cluster_alter_check_ready_interval`
  — cleared = done, past-deadline tombstone = `AlterClusterTimeout`; with the new
  `enable_background_alter_cluster` dyncfg on, `ALTER` returns immediately instead. Session disconnect
  no longer aborts the reconfiguration (the record is durable; the controller carries on). **Audit:**
  new `ReplicaCreateDropReason::GracefulReconfiguration` → `CreateOrDropClusterReplicaReasonV1::Reconfiguration`
  (rust enum + `objects.rs`/`objects_v86.rs` proto enum revised in place since v86 is unshipped, hashes
  updated, `RustType` conversion threaded); the controller maps graceful-desired creates to it via
  per-decision strategy attribution. Added dyncfgs `enable_background_alter_cluster` (false) and
  `default_cluster_reconfiguration_timeout` (24h), registered in `all_dyncfgs`. **Tests:** 8 new
  graceful kernel/flow tests (17 total in `mz-cluster-controller`, all pass) covering in-flight desire,
  cut-over on full hydration, partial-hydration no-cutover, timeout-vs-hydrated precedence, timeout
  park+drop, AZ-only shape change, full overlap→cut-over→old-set-drop with attribution, and ALTER-back
  no-churn; the fake ctx now drives hydration. Extended `cluster_controller.slt` (gate + background
  forced on) to assert a background `ALTER SIZE` cuts the realized `mz_clusters.size` over and settles
  the replica set — authored for CI, not run locally. **Cheap checks:** `cargo fmt` + `cargo
  check`/`clippy` clean on `mz-cluster-controller`/`mz-adapter`/`mz-adapter-types`/`mz-catalog-protos`/
  `mz-audit-log`/`mz-catalog`; `bin/lint-cargo` clean; `mz-catalog-protos` snapshot + `mz-catalog`
  `durable::` suite (163 passed) green; `Cargo.lock` unchanged (no new deps). Did **not** run the slt
  suite, platform-checks, or the full optimized build. **Deferred (see PR-3 section):** the bare
  `WITH (TIMEOUT = ...)` parser token (timeout sourced from the existing `WAIT` plumbing instead) and
  the restart-survival platform-check (would flip the master gate env-wide in the shared dark suite).
  Old-set drops at cut-over carry `Manual` (none-desired); richer reconfiguration drop-lifecycle audit
  attribution lands in PR 4.
- _2026-06-02_ — **PR 2 implemented** (controller scaffolding + baseline strategy + coordinator
  wiring, all dark). New pure crate `mz-cluster-controller` (deps: `mz-controller-types`,
  `mz-compute-types`, `mz-repr`, `mz-ore`, `async-trait`; no adapter/catalog dep, no new
  third-party license). It defines the **`ClusterControllerCtx`** seam (batched pulls:
  `managed_cluster_ids`, `cluster_states` with latched `now`, `collections_hydrated_on_replicas`
  stubbed for PR 3; one CaA-guarded `apply`), the pure **`Strategy`** trait
  (`update_state`/`desired_replicas`), the **`BaselineStrategy`** (desires `replication_factor`
  replicas at the realized shape), and the **reconcile kernel**: phase 1 unions every strategy's
  `update_state` into one `UpdateClusterState` per cluster and applies under CaA (rejected clusters
  are skipped this tick); phase 2 re-reads, unions `desired_replicas` (multiset union = max per
  shape, not sum), matches by `ReplicaShape` against actual, and emits creates (fresh names that
  avoid in-use `rNN`) / drops with strategy attribution (phase 2 reuses the phase-1 read when no
  state was written). **Compare-and-append:** every decision — the `UpdateClusterState` writes and
  the create/drop batch alike — carries the `ExpectedClusterState` it was diffed against
  (`ClusterState::expected()`); the apply path re-reads each cluster's durable config + records and
  rejects the whole batch on any mismatch, so a stale create/drop can never reshape the replica set
  against a config a concurrent `ALTER` established (recomputed next tick). Decision 1 = **separate
  task**: the adapter driver `coord/cluster_controller.rs` runs the controller on its own
  `mz_ore::task::spawn`, and its `CoordCtx` marshals each ctx call to the coordinator loop via
  `Message::ClusterControllerRequest(..)` + oneshot; on a held guard it builds
  `Op::UpdateClusterConfig` (cut-over/record-write from the `StateWrite` deltas),
  `Op::CreateClusterReplica` (reusing `concretize_replica_location`), and `Op::DropObjects`,
  transacted via `catalog_transact`. Added dyncfgs `enable_cluster_controller` (default **false**)
  and `cluster_controller_tick_interval` (5s), both re-read each tick — a gate flip needs no restart
  and the cadence is a live operational knob; with the gate off, reads return no clusters and
  applies reject, keeping the controller fully inert and all legacy paths unchanged. Create/drop
  audit currently uses `ReplicaCreateDropReason::Manual` as an interim tag — PR 3 introduces the
  controller-specific, attribution-carrying reason (the controller is dark, so the tag is immaterial
  until then). The frontier/read-ts ctx reads land with their first consumer (PR 3/5), not as
  speculative stubs (their shape depends on that consumer and would pull an unused frontier dep into
  the pure crate). Tests: 9 boundary/kernel tests against a fake ctx (steady no-op,
  under/over-provision, wrong-shape drop+create, union max-not-sum, distinct-shape attribution, CaA
  reject-and-recover against a state a concurrent `ALTER` changed, and a direct phase-2 create/drop
  guard test) all pass; an slt (`cluster_controller.slt`, gate forced on) drives the interval to
  5ms, waits across hundreds of ticks, and asserts a steady managed cluster's replica ids+names are
  unchanged — not satisfiable by gate-off behaviour that never runs the loop; authored for CI, not
  run locally. Cheap checks: `cargo fmt` + `cargo check`/`cargo clippy` clean on
  `mz-cluster-controller`/`mz-adapter`/`mz-adapter-types`; `bin/lint-cargo` clean; `Cargo.lock`
  gained only the new-crate entry (no version bumps). Did **not** run the full optimized build, the
  slt suite, or generate `doc/developer/generated/` crate docs (regen deferred, as in PR 1).
- _2026-06-01_ — **PR 1 AZ modeling reworked (in-memory cleanup; commit 2 of 2).** Removed the
  in-memory `ManagedReplicaAvailabilityZones` enum, replacing the `ManagedReplicaLocation`
  availability-zone field with a bare `Vec<String>` (empty = unconstrained). With the durable field
  now storing the list unconditionally (commit 1), the in-memory→durable `From` is a passthrough,
  concretize fills the list directly (managed pool re-derived from the cluster, durable list as the
  pin for unmanaged), the orchestrator maps empty→no-constraint, and the convert-to-managed check
  iterates the pin(s) — all behaviour-preserving. Per decision, the stable public
  `mz_cluster_replicas.availability_zone` column is left unchanged (still the unmanaged replica's
  single pin, now derived from the list + the cluster's managed-ness); the `guswynn` TODO is replaced
  with a comment recording the future plural-`text list` rename and its consumers (console types, dbt
  macros, docs, slt). Cheap checks: `cargo fmt` + `cargo check -p mz-controller -p mz-catalog
  -p mz-adapter` clean; `durable::` suite (163 passed).
- _2026-06-01_ — **PR 1 AZ modeling reworked (durable collapse; commit 1 of 2).** Reworked the
  durable availability-zone change rather than carrying forward the two-field shape from the entries
  below: the managed `ReplicaLocation`'s single `availability_zone` user-pin is **collapsed** into one
  `availability_zones: Vec<String>` (the zones the replica was provisioned under — a managed cluster's
  `AVAILABILITY ZONES` pool, or an unmanaged replica's pin as a 0/1-element list), replacing the
  separate `provisioned_availability_zones` field. The two were mutually exclusive (concretize even
  errored if both were set) and both collapse to a list at the orchestrator, so the split was
  redundant. v86 is unshipped, so it is revised in place — one migration, the collapsed shape. The
  controller→durable `From` maps both `ManagedReplicaAvailabilityZones` variants to the list;
  concretize stays inert (re-derives the managed pool from the cluster, reads the list as the pin for
  unmanaged clusters), so PR 1 stays dark. The in-memory `ManagedReplicaAvailabilityZones` enum is
  kept in this commit and removed in commit 2 — it is the persistence discriminator until the durable
  field stores the list unconditionally, so it cannot become a bare `Vec` any earlier. Per decision,
  the stable public `mz_cluster_replicas.availability_zone` column is left unchanged; commit 2 adds a
  comment documenting the future plural-`text list` rename (an outward-facing break held out of this
  dark PR). Cheap checks: `cargo fmt` + `cargo check -p mz-catalog -p mz-catalog-protos -p mz-adapter`
  clean; full `durable::` suite (163 passed, incl. `test_proto_serialization_stability`,
  `proptest_state_update_kind_roundtrip`, v85↔v86 json-compat round-trips, migration tests);
  `objects_v86.txt` regenerated.
- _2026-06-01_ — **PR 1 boundary refactor.** Removed the deviation that the in-memory
  `ClusterVariantManaged` embedded durable-only autoscaling types (which forced serde derives on
  those durable types, unlike every other durable type). Re-homed each type per the existing
  convention, by kind: (1) the *user-configured policy* `AutoScalingStrategy`/`OnHydration` moved to
  `mz_sql::plan` and is now referenced by **both** layers with no conversion, exactly like
  `ClusterSchedule` (its `RustType<proto::…>` impls live in `mz-catalog-protos`'s `serialization.rs`
  alongside `ClusterSchedule`'s, per Rust's orphan rules); (2) the *in-flight runtime* records
  `ReconfigurationState`/`ReconfigurationTarget`/`BurstState` stay durable-only and lose their serde
  derives, with new in-memory mirror types in `memory::objects` carrying serde and `From` conversions
  in both directions, mirroring the `optimizer_feature_overrides` pattern (runtime controller state
  does not belong in the SQL planning crate, and `mz-cluster-controller` does not exist until PR 2).
  Purely a relocation of Rust type definitions/derives and conversion plumbing: the proto message
  types, `objects_v86.rs`/`objects_v86.txt`/`objects_hashes.json`, and the v85→v86 migration are
  untouched, so the on-disk encoding and migration semantics are unchanged and PR 1 stays dark (no
  new field is read). Cheap checks: `cargo fmt --check` + `cargo check` + `cargo clippy` on
  `mz-sql`/`mz-catalog`/`mz-catalog-protos`/`mz-adapter` clean; `mz-catalog-protos` snapshot tests
  and the full `mz-catalog durable::` suite (163 passed, incl. `test_proto_serialization_stability`,
  `proptest_state_update_kind_roundtrip`, the json-compat round-trips, and the v85→v86 migration
  tests) pass.
- _2026-06-01_ — **PR 1 review follow-up.** Addressed the review of the implementation commit:
  (1) clarified the contract of `provisioned_availability_zones` and made the two write paths
  agree — it is now the cluster-imposed AZ pool on both, so `FromReplica` (a non-managed cluster's
  managed location) contributes only the single `availability_zone` user-pin and an empty list,
  matching the migration's empty backfill for unmanaged-cluster replicas; (2) rewrote `v85_to_v86.rs`
  to use `JsonCompatible::convert` for unchanged sub-structures (per the directory convention),
  reconstructing only the managed `Cluster` variant and managed `ReplicaLocation`, and to skip
  byte-identical unmanaged records rather than emit no-op retract+add pairs; (3) added a `nota bene`
  documenting why the durable autoscaling types carry serde derives (the in-memory mirror backing
  `dump()` requires them) — the review's suggestion to drop them was declined because removing them
  breaks compilation. Cheap checks: `cargo fmt --check` + `cargo check -p mz-catalog
  -p mz-catalog-protos -p mz-adapter` clean; full `durable::` test suite (163 passed, incl. the 18
  new generated json-compat round-trip proptests and `test_proto_serialization_stability`) and the
  catalog-protos snapshot tests pass.
- _2026-06-01_ — **PR 1 implemented** (durable state model + migration v85→v86, additive/inert).
  Added durable + in-memory + proto types `AutoScalingStrategy`/`OnHydration`,
  `ReconfigurationState`/`ReconfigurationTarget`, `BurstState` as `Option` fields on
  `ClusterVariantManaged` (decision 3), with target shape `{ size, replication_factor,
  availability_zones, logging }` (decision 4). Added `provisioned_availability_zones: Vec<String>`
  to durable `ReplicaLocation::Managed`; new managed replicas derive it from
  `ManagedReplicaAvailabilityZones` in the controller→durable `From`, and the migration backfills
  existing managed replicas from their cluster's `availability_zones`. Bumped `CATALOG_VERSION` to 86,
  snapshotted `objects_v86.rs` (+ `objects_hashes.json`), wrote `v85_to_v86.rs` (rewrites every
  managed `Cluster`/`ClusterReplica` record), and generated `objects_v86.txt`. No new fields are read
  yet → no behaviour change; the `pending` drop and rf-normalization stay deferred to later PRs
  (decision 7). Cheap checks: `cargo fmt` clean; `cargo check -p mz-catalog -p mz-catalog-protos
  -p mz-adapter` clean. Tests run: catalog-protos snapshot assertions + all 144 catalog durable tests
  (incl. four new migration unit tests and the proto round-trip proptest). Did **not** run the full
  optimized/release build or `cargo test` over the whole workspace.
- _2026-06-01_ — Reordered the rollout: burst is the last *feature* PR (PR 6), so all of PRs 1–6
  land **dark**. Enabling is an operational LaunchDarkly flip + prod bake (not a code change);
  the final PR 7 (flip defaults to true + remove legacy paths + drop `pending`) lands **only after**
  the bake — we never enable and remove-the-fallback in one step. Scheduled-cluster rf is normalized
  by the controller at runtime, so no rf migration.
- _2026-06-01_ — Plan created against the realized-fact design (no implementation in the tree
  yet; PR 1 is next). The controller↔coordinator boundary is an agnostic **pull** interface
  (`ClusterControllerCtx`), not a coordinator-pushed snapshot; testing focuses on user-visible
  behavior + the interface seam with minimal kernel unit tests; burst replicas are ordinary
  replicas in the reconciler (no tag/marker).
