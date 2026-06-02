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
| `default_cluster_reconfiguration_timeout` | Duration | (system default) | PR 3 | Deadline written when an `ALTER` omits `WITH (TIMEOUT = ...)`. |
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
  - `ReconfigurationState { target: ReconfigurationTarget, deadline: mz_repr::Timestamp }`, where `ReconfigurationTarget { size, replication_factor, availability_zones, logging }` (decision 4).
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

**Status:** ⬜ Not started

**Goal.** Move graceful (zero-downtime) reconfiguration into the controller as a strategy,
driven by the durable `reconfiguration` record, with hydration-aware cut-over and timeout —
all dark behind the master gate; legacy 3-stage machine still runs when the gate is off.

**Scope (in).**
- **Implement the hydration/frontier ctx methods** (first hydration-dependent strategy): back `ClusterControllerCtx`'s hydration query with `collections_hydrated_for_replicas` / `collections_hydrated_on_replicas` (and frontiers as needed), pulled on demand — the controller asks only about the replicas a tick examines. This is the seam stubbed in PR 2.
- `GracefulReconfigurationStrategy` (pure):
  - Engaged when `reconfiguration` record is present.
  - `desired_replicas`: `target.replication_factor` replicas at `target` shape (size + logging + AZ list).
  - `update_state`: target replicas present **and** hydrated → cut over (`cluster.size := target.{size,...}`, clear record). Reads `deadline`: `now > deadline` and not yet hydrated → record timeout, **retain record as tombstone**, stop contributing (parked). Hydrated **and** past deadline → cut over (success precedence).
  - Diff on size + logging + **AZ list** (now diffable thanks to PR 1) + count.
- **`ALTER CLUSTER` reshape (gated):** when the master gate is on and the change is a config-shape change (size/logging/AZ), write the `reconfiguration` record (target + deadline) in one txn and return; leave realized config. Once a record is present, further `ALTER`s **fold into it** (overwrite target + deadline). Non-shape changes (rf-only, workload_class) with no record in flight update realized config directly (as today). `WITH (TIMEOUT = ...)` → `deadline = txn_ts + timeout`; omitted → `default_cluster_reconfiguration_timeout`. **Keep the legacy 3-stage path for gate-off** (don't delete yet — PR 7).
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
- [ ] Implement the hydration/frontier methods of `ClusterControllerCtx` (pulled on demand).
- [ ] Implement `GracefulReconfigurationStrategy` (pure); targeted kernel tests for the tricky cases.
- [ ] Reshape `ALTER CLUSTER` (gated): write/fold `reconfiguration` record; keep legacy path for gate-off; timeout/deadline resolution + dyncfg default.
- [ ] Wait-shim polling the record; `enable_background_alter_cluster` dyncfg.
- [ ] Audit reason variant (`Reconfiguration`) on create/drop.
- [ ] Integration + platform-check tests (authored; run in CI).
- [ ] `cargo fmt`/`check` clean; update tracker.

---

## PR 4 — Observability: introspection view + `SHOW CLUSTERS` + audit

**Status:** ⬜ Not started

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
- [ ] Add the introspection table/view; catalog-populate from the durable records.
- [ ] Extend `MZ_SHOW_CLUSTERS` SQL; keep `show.rs` template pointing at the view.
- [ ] Reconfiguration audit lifecycle events (with deadline on start/timeout).
- [ ] Testdrive + audit assertions.
- [ ] `cargo fmt`/`check` clean; update tracker.

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
