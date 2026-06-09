# Scoped feature flags: per-cluster and per-replica-size LaunchDarkly overrides

- Associated:
  - `doc/developer/design/20240205_cluster_specific_optimization.md` (prior art, rejected the per-cluster LD approach "for now")

## The Problem

We distribute feature flags / system parameters through LaunchDarkly (LD). The
finest granularity we can currently target is the **environment**: every flag is
evaluated exactly once, against a single fixed `ld::Context`, and the resulting
value is applied globally to the environment's `SystemVars`. There is no way to
say "this flag has value X on cluster A but value Y everywhere else", or "this
flag is on for replicas of size family D but off for the legacy t-shirt sizes".

Two concrete use cases motivate finer granularity:

1. **Per-cluster overrides.** We want the `mz_catalog_server` cluster (the
   builtin cluster known internally as `s2`) to run with a different set of
   flags than the rest of the environment, independent of the environment-wide
   values. For example, we may want to turn an optimizer feature or a persist
   read tuning on for the catalog server without affecting user clusters (or
   vice-versa).

2. **Per-replica-size-family overrides.** We are introducing replica *size
   families* — `cc`, the legacy t-shirt sizes, `M.1`, and soon `D.1` — and
   different families should ship with different flags. Concretely: the legacy
   t-shirt sizes should keep using `lgalloc`, while `D.1` might enable the
   persist pager and LZ4 compression. The override is keyed by the replica's
   *size*, not by the cluster it belongs to.

Today, the only knob we have that resembles this is the manually-set, optimizer-
only `CREATE CLUSTER ... FEATURES (...)` override
(`src/sql/src/plan/statement/ddl.rs`, stored as `OptimizerFeatureOverrides` on
the cluster's `ClusterConfig`). It is not wired to LaunchDarkly, and it only
covers optimizer features.

## Success Criteria

- A flag can be assigned a value scoped to (a) a specific cluster and (b) a
  specific replica size family, with a well-defined precedence relative to the
  environment-wide value.
- Those scoped values are driven from LaunchDarkly using LD's existing targeting
  primitives, so they can be rolled out / rolled back without a deploy, exactly
  like environment-wide flags today.
- The consumers that already know which cluster / replica they are acting on
  (the optimizer, the compute & storage controllers, `clusterd`) observe the
  correctly resolved value.
- Adding scoped targeting does not blow up LaunchDarkly context cardinality
  (LD bills per context), and does not regress the environment-wide path.
- Behaviour is well-defined when LaunchDarkly is unreachable (same fallback
  story as today's global flags).

## Out of Scope

- Per-session, per-database, or per-role flag scoping (PostgreSQL-style
  `system < database < role < session`). We only add `cluster` and
  `replica size family` scopes here, though the mechanism is designed so a third
  scope could be added later.
- Per-individual-replica overrides (i.e. two replicas of the *same* size in the
  same cluster getting different flags). The scope is the size family, not the
  replica.
- Arbitrary user-defined targeting in the product surface (e.g. exposing this to
  customers). This is an internal operability mechanism, driven by LD on the
  Materialize side.
- Switching feature-flag vendors. LaunchDarkly already supports everything we
  need on the distribution side (see "LaunchDarkly capabilities"); the work is
  almost entirely on the Materialize side.

## LaunchDarkly capabilities (why this is mostly a Materialize-side change)

The environment-only granularity is **not** an LD limitation. LD already
supports:

- **Custom context kinds.** Beyond `user`, you can define arbitrary kinds such
  as `cluster` and `replica_size`, each with custom attributes. We already use
  this: `ld_ctx` in `src/adapter/src/config/frontend.rs` builds a multi-context
  with kinds `environment`, `organization`, and `build`.
- **Multi-contexts.** A single evaluation can carry `environment` +
  `organization` + `cluster` + `replica_size` at once, and targeting rules can
  combine conditions across kinds.
- **Per-evaluation context with one SDK client.** `client.variation(ctx, flag,
  default)` takes the context per call, so we can evaluate the same flag against
  different contexts from the same `ld::Client` — no extra clients needed.
- **Rule-based segments and percentage rollouts** on any attribute, e.g.
  `replica_size_family in {legacy, cc}` or `cluster_name = "mz_catalog_server"`.

So the LD-facing change is: add `cluster` / `replica_size` context kinds to the
evaluation, and author targeting rules against them. The hard part is on our
side: **where do scoped values live, and how does each consumer resolve the
value for the cluster/replica it is operating on.**

## Solution Proposal

High-level: introduce **scoped system parameters**. A synced parameter can carry,
in addition to its environment-wide value, a set of overrides keyed by a *scope*:

```
scope := Global | Cluster(cluster) | ReplicaSizeFamily(family)
```

with resolution precedence, from lowest to highest:

```
Global  <  ReplicaSizeFamily  <  Cluster
```

(Cluster wins over size family because cluster targeting is the more specific,
operator-driven decision; see Open Questions for whether this ordering is right
for every parameter.)

The design has three parts: **distribution** (LD), **storage** (catalog), and
**resolution** (consumers). We keep the existing data flow shape —
`LaunchDarkly -> sync loop -> ALTER SYSTEM -> catalog -> consumers` — and extend
each stage to be scope-aware, rather than inventing a parallel path.

### 1. Distribution: evaluate LaunchDarkly per scope

Today `SystemParameterFrontend::pull` (`frontend.rs`) loops over parameters and
calls `client.variation(&self.ctx, flag, default)` with one fixed context. The
sync loop (`sync.rs`) calls `pull` once per tick.

Changes:

- **Extend `ld_ctx`** to accept optional scope attributes and emit additional
  context kinds:
  - `cluster` — `key = <cluster name>`, attrs: `cluster_name`,
    `is_builtin` (bool), `cluster_id`.
  - `replica_size` — `key = <size family>`, attrs: `size_family`
    (`cc` / `legacy` / `M` / `D`), `size_name` (e.g. `D.1`).
- **Evaluate per scope.** The frontend gains
  `pull_scoped(scope, params)` which builds the appropriate multi-context
  (always including the existing `environment` / `organization` / `build`
  contexts, plus the scope's context) and evaluates the synced flags against it.
  The sync loop now does, per tick:
  1. `pull` with the base context → environment-wide values (unchanged).
  2. For each *scope of interest*, `pull_scoped` → that scope's values.
  The diff between a scope's evaluation and the base evaluation is the scope's
  *override set* (we only store a scoped value when it actually differs from the
  environment-wide value, to keep overrides sparse).

- **Bounding context cardinality (important — LD bills per context).** We do
  **not** create an LD context per user cluster or per replica. Instead:
  - For clusters, the sync loop only evaluates the **builtin system clusters**
    (`mz_catalog_server`, `mz_system`, `mz_probe`, …) by their stable names.
    These are a fixed, tiny, low-cardinality set, and use case 1 only needs
    `mz_catalog_server`. (Targeting arbitrary user clusters is left as future
    work precisely because of cardinality; see Open Questions.)
  - For replica sizes, the loop evaluates one context per **distinct size
    family currently in use** (~4 today), discovered from the cluster replica
    size configuration. Keying by family (not by individual replica) keeps this
    at a handful of contexts regardless of replica count.
  - All scope contexts can be marked `anonymous` so they don't pollute the LD
    Contexts dashboard, mirroring the existing `Local` dev handling in
    `ld_ctx`.

The sync loop already holds a `SystemParameterBackend` with a `SessionClient`,
so it can query the catalog for the list of builtin clusters and the in-use size
families to decide which scopes to evaluate.

### 2. Storage: scoped `ALTER SYSTEM` in the catalog

Today the backend (`backend.rs`) pushes each modified value via
`set_system_vars`, i.e. `ALTER SYSTEM SET <name> = <value>`, and the catalog
persists it. We keep this and add scoped variants:

- New SQL surface (system-only, like `FEATURES`):
  - `ALTER SYSTEM SET <name> = <value> FOR CLUSTER <name>`
  - `ALTER SYSTEM SET <name> = <value> FOR REPLICA SIZE FAMILY <family>`
  - plus `RESET` counterparts.
- New catalog state: a scoped-overrides table/collection mapping
  `(scope, parameter_name) -> value`, durably stored next to the existing system
  configuration. The backend's `push` writes the override set computed by the
  sync loop into this collection (and resets entries that no longer differ).

Persisting (rather than keeping overrides only in environmentd memory) buys us
the same properties the global path already has: values survive an environmentd
restart and an LD outage, and they are introspectable. We expose the resolved
and the raw scoped values through new `mz_internal` relations (e.g.
`mz_cluster_system_parameters`) for debugging.

This is the concrete realization of the extension the cluster-specific
optimization design doc hinted at: "use the `CLUSTER`-specific `ALTER SYSTEM`
extensions in `SystemVars` in the `backend.push(...)` call."

### 3. Resolution: consumers read the scoped value

This is the most invasive part, but it factors into two existing per-scope
boundaries, so we do **not** need a universal rewrite of `SystemVars`.

`SystemVars` (`mz_sql::session::vars`) gains a layered lookup:

```rust
// pseudocode
fn get_for(&self, name: &str, scope: ResolveScope) -> &VarValue {
    // ReplicaSizeFamily and Cluster overrides shadow the global value,
    // Cluster winning over ReplicaSizeFamily.
}
```

where `ResolveScope { cluster: Option<ClusterId>, size_family: Option<Family> }`.
The default `get` (no scope) returns the global value, so every existing call
site is unchanged.

The two boundaries that need to pass a scope:

**(a) The compute & storage controllers' per-replica config push — handles use
case 2 and the cluster-scoped dyncfg part of use case 1.**

The flags in use case 2 (`lgalloc`, persist pager, LZ4 compression) are
`mz_dyncfg` parameters consumed inside `clusterd`, and environmentd already
pushes a `ConfigSet` / `ConfigUpdates` to each replica through the controllers.
environmentd knows each replica's cluster *and* size at push time. So we resolve
there: when building the `ConfigUpdates` for a replica, layer
`global ⊕ size_family_override ⊕ cluster_override` and send the effective value.

Crucially, **`clusterd` needs no changes** — it keeps reading its dyncfg
`ConfigSet`; it just receives size/cluster-appropriate values. This makes
use case 2 almost entirely a matter of resolving at the push site.

**(b) The optimizer's per-cluster feature set — handles the optimizer part of
use case 1.**

The optimizer already takes an `OptimizerFeatures` derived from `SystemVars`
*plus* the per-cluster `OptimizerFeatureOverrides` stored on `ClusterConfig`.
We generalize the source of those overrides: instead of only the manually-set
`CREATE CLUSTER ... FEATURES`, the cluster's resolved layer (from §2) feeds the
overrides. Any place that already plumbs a "which cluster am I optimizing for"
context (peek/index/MV sequencing in
`src/adapter/src/coord/sequencer/inner/*`) resolves the cluster scope.

For SystemVars consumed in environmentd that are neither dyncfg nor optimizer
features but are still cluster-dependent, the same `get_for(name, scope)` lookup
is used at the (already cluster-aware) call site. We expect this set to be small;
each such parameter is opted in explicitly (see Open Questions on opt-in).

### Worked examples

**Use case 1 — `mz_catalog_server` gets a different optimizer feature.**

1. In LD, add a targeting rule on the synced flag: `cluster_name =
   "mz_catalog_server"` → variation X.
2. Sync loop evaluates the base context (→ env-wide value) and a
   `cluster:mz_catalog_server` context (→ X). They differ, so it writes
   `Cluster(mz_catalog_server) -> X` via `ALTER SYSTEM SET ... FOR CLUSTER
   mz_catalog_server`.
3. When the coordinator optimizes a dataflow on `mz_catalog_server`, the cluster
   layer feeds `OptimizerFeatureOverrides`, so X is used. Other clusters keep the
   env-wide value.

**Use case 2 — legacy sizes keep `lgalloc`, `D.1` gets pager + LZ4.**

1. In LD, target the `lgalloc` dyncfg flag with `size_family = "legacy"` → on,
   and the pager / LZ4 flags with `size_family = "D"` → on.
2. Sync loop evaluates one context per in-use family (`cc`, `legacy`, `M`, `D`)
   and records the per-family override sets.
3. When the controller pushes dyncfg to a replica, it resolves by that replica's
   size family: a `D.1` replica receives pager+LZ4 enabled; a legacy-sized
   replica receives `lgalloc` enabled. No `clusterd` change required.

## Minimal Viable Prototype

Smallest end-to-end slice that de-risks the design, ordered:

1. **Replica-size dyncfg push (use case 2 first — it's the cleaner boundary).**
   - Add the `replica_size` context kind to `ld_ctx` and `pull_scoped`.
   - Compute per-size-family override sets in the sync loop (held in
     environmentd memory for the prototype, no catalog persistence yet).
   - Resolve at the controller's per-replica dyncfg push.
   - Demonstrate: LD rule `size_family = "legacy"` toggles `lgalloc` only on
     legacy replicas, verified via the replica's effective config.
   This proves the LD-per-scope evaluation and the push-site resolution with
   zero catalog/SQL/`clusterd` changes.

2. **Cluster scope for `mz_catalog_server` (use case 1).**
   - Add the `cluster` context kind (builtin clusters only).
   - Feed the resolved cluster layer into `OptimizerFeatureOverrides`.
   - Demonstrate a flag that differs on `mz_catalog_server` vs. user clusters.

3. **Catalog persistence + `ALTER SYSTEM ... FOR ...` SQL** and the
   `mz_internal` introspection relations, once the in-memory prototype validates
   the resolution model.

Test surface to extend: `test/launchdarkly/mzcompose.py` already targets by
`contextKind`; add cases for `contextKind = "cluster"` and `"replica_size"`.

## Alternatives

- **Universal `SystemVars` layering (`system < cluster < session`).** Teach
  `SystemVars` to be fully scope-aware for *every* read site, as the prior design
  doc sketched. More general, but a large, risky change touching every consumer
  and the session-var resolution path. Rejected as the starting point in favour
  of resolving at the two existing per-scope boundaries (controller push,
  optimizer overrides), which covers both use cases with far less blast radius.
  The `get_for` lookup proposed here is a stepping stone toward this if we later
  need broader scoping.

- **In-memory-only overrides (no catalog persistence).** Keep scoped values only
  in environmentd, re-derived from LD each tick and pushed to consumers. Simpler
  (no SQL/catalog work), and fine for the prototype, but loses the
  survive-restart / survive-LD-outage / introspectable properties the global
  path has today. We adopt it for the MVP but recommend persistence for GA.

- **Per-individual-replica targeting (LD context per replica).** Maximum
  flexibility, but explodes LD context cardinality (billing) and gives no
  benefit over size-family targeting for the stated use cases. Rejected.

- **Switch / add a different feature-flag tool.** Surveyed Unleash, Flagsmith,
  Statsig, and the OpenFeature standard. None removes the Materialize-side
  storage/resolution work, and several (Unleash, Flagsmith) are *less* expressive
  than LD's multi-context model — they would force flattening cluster/size into
  context fields. The only structural reason to consider OpenFeature is backend
  portability (wrap the integration behind its API and keep LD as the provider).
  Rejected as a prerequisite; orthogonal to this design.

## Open questions

- **Precedence.** Is `Global < ReplicaSizeFamily < Cluster` always right? A
  cluster-level override would shadow a size-family override even for a replica
  whose size family was explicitly targeted. Do we need per-parameter precedence,
  or is a single global ordering acceptable?
- **Opt-in surface.** Should *every* synced parameter be scopable, or should
  parameters opt into being cluster-/size-scopable (e.g. a flag on the `Var`
  definition)? Opting in keeps the resolution surface and the LD evaluation cost
  bounded and avoids surprising interactions for params that are meaningless
  per-cluster.
- **User clusters.** Use case 1 only needs builtin clusters, which keeps LD
  context cardinality trivial. Is there a near-term need to target arbitrary user
  clusters, and if so, how do we bound the context count (e.g. only clusters that
  have an active scoped rule, anonymous contexts, or a server-side evaluation
  proxy)?
- **Size family taxonomy.** Where is the authoritative `size name -> size
  family` mapping (`D.1 -> D`, t-shirt sizes -> `legacy`)? It must be available
  both to the sync loop (to know which families to evaluate) and to the
  controller (to resolve per replica). Likely the cluster replica size
  configuration; confirm.
- **Interaction with `CREATE CLUSTER ... FEATURES`.** A cluster can already carry
  manual `OptimizerFeatureOverrides`. When both a manual override and an
  LD-driven cluster override exist for the same feature, which wins? Proposed:
  manual DDL override wins over LD (operator intent is explicit), but this needs
  confirmation.
- **Sync cost.** Per-tick evaluation count grows from 1 to `1 + |builtin
  clusters| + |size families|`. With the bounds above this is ~10 evaluations;
  confirm this is acceptable for the LD SDK and tick interval, and whether scoped
  evaluation should run on a slower cadence than the base pull.
