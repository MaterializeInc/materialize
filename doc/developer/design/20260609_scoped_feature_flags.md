# Scoped feature flags: per-cluster and per-replica LaunchDarkly overrides

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

1. **Per-cluster overrides (cluster-coherent flags).** We want the
   `mz_catalog_server` cluster (the builtin cluster known internally as `s2`) to
   run with a different set of flags than the rest of the environment,
   independent of the environment-wide values. The motivating class here is
   **optimizer flags**: there is broad interest in being able to flip optimizer
   features per cluster from LD, e.g. enabling a feature on the catalog server
   (or a specific user cluster) without affecting everyone else.

2. **Per-replica overrides (replica-local flags), keyed by size family.** We are
   introducing replica *size families* — `cc`, the legacy t-shirt sizes, `M.1`,
   and soon `D.1` — and different families should ship with different flags.
   Concretely: the legacy t-shirt sizes should keep using `lgalloc`, while `D.1`
   might enable the persist pager and LZ4 compression. The override is keyed by
   the replica's *size*, not by the cluster it belongs to.

Today the only knob that resembles this is the manually-set, optimizer-only
`CREATE CLUSTER ... FEATURES (...)` override (`src/sql/src/plan/statement/ddl.rs`,
stored as `OptimizerFeatureOverrides` on the cluster's `ClusterConfig`). It is
not wired to LaunchDarkly.

## Success Criteria

- A flag can be assigned a value scoped to (a) a specific cluster and (b) a
  specific replica / replica size family, with a well-defined precedence
  relative to the environment-wide value.
- Every synced parameter **declares its scope class** (environment-only,
  cluster-coherent, or replica-local) at its definition. The declaration is
  required, is surfaced in documentation / introspection, and is the single
  source of truth for which contexts the sync loop evaluates and where the value
  may be overridden.
- Those scoped values are driven from LaunchDarkly using LD's existing targeting
  primitives, so they can be rolled out / rolled back without a deploy, exactly
  like environment-wide flags today.
- The consumers that already know which cluster / replica they are acting on
  (the optimizer, the compute & storage controllers, `clusterd`) observe the
  correctly resolved value.
- Recreating an object (drop + recreate, or a replica resize) does **not**
  silently inherit an override that was meant for the previous incarnation,
  unless the override was authored as a durable predicate (e.g. by name or size
  family) that intentionally applies to the new object too.
- Scoped values are **durable**: they survive an `environmentd` restart and
  remain in effect while LaunchDarkly is slow to sync or unavailable. Resolution
  falls back to the environment-wide value only on a *cold cache* — i.e. an
  object we have never successfully evaluated against LD. This matches how
  environment-wide flags already persist and survive an LD outage.

## Out of Scope

- Per-session, per-database, or per-role flag scoping (PostgreSQL-style
  `system < database < role < session`). We only add `cluster` and `replica`
  scopes here, though the mechanism is designed so a third scope could be added.
- Exposing this targeting to customers as a product surface. This is an internal
  operability mechanism, driven by LD on the Materialize side.
- Switching feature-flag vendors. LaunchDarkly already supports everything we
  need on the distribution side (see "LaunchDarkly capabilities"); the work is
  almost entirely on the Materialize side.

## LaunchDarkly capabilities (why this is mostly a Materialize-side change)

The environment-only granularity is **not** an LD limitation. LD already
supports custom context kinds, multi-contexts (one evaluation carrying several
kinds at once), per-evaluation contexts on a single SDK client
(`client.variation(ctx, flag, default)`), and rule-based segments / percentage
rollouts on any attribute. We already use a multi-context of kinds `environment`,
`organization`, and `build` in `ld_ctx` (`src/adapter/src/config/frontend.rs`).

So the LD-facing change is small: add `cluster` and `replica` context kinds to
the evaluation and author rules against them. The substantive work is on our
side — where scoped values live and how each consumer resolves the value for the
cluster/replica it is operating on.

### Billing note: server-side SDKs are billed by connections, not contexts

A natural worry is that minting a context per cluster/replica explodes LD
billing. It does not, because of how LD meters the two SDK families:

- **Client-side / edge SDKs** are billed by **MAU** = unique context keys per
  month. This is what the Materialize **Console** consumes (the JS client-side
  SDK in `console/src/hooks/useLaunchDarklyClient.ts`, keyed by user/org).
- **Server-side SDKs** are billed by **service connections** = one SDK instance
  connected to one environment, measured in connection-minutes. **Context count
  is irrelevant to this metric.**

`environmentd` uses the **server-side** SDK (`launchdarkly_server_sdk`). Whether
it evaluates 4 contexts or 4,000 from its single client, the billing is the same
— it is driven by how many `environmentd` / `balancerd` instances connect, not
by how many cluster/replica contexts we mint. The MAU bucket visible on the
usage page is therefore not the relevant constraint for this work.

The only residual cost of many distinct context keys is **cosmetic**: they
populate the LD *Contexts list* in the dashboard. (That, not billing, is why the
existing dev-mode path uses `anonymous(true)` + fixed keys.) We mark the
cluster/replica contexts `anonymous` for the same dashboard-cleanliness reason.

> **Pre-launch checkbox (must be confirmed before enabling the gate):** confirm
> with the LD account owner that we are on the standard service-connections model
> and not a custom contract with per-context metering.

## Solution Proposal

Introduce **scoped system parameters**: a synced parameter can carry, in
addition to its environment-wide value, overrides resolved per cluster and per
replica. We keep the existing data-flow shape and extend it to be scope-aware.

### Two context kinds, because there are two scopes of coherence

Not every flag can be resolved at the same granularity. There are two distinct
classes:

- **Replica-local flags** — realized inside a `clusterd` process (e.g.
  `lgalloc`, the allocator, persist client tuning, the persist pager, LZ4
  compression). Each replica applies its own value independently, so the
  **replica is the unit of control**, and the value may legitimately differ per
  replica (e.g. by size family). This is use case 2.

- **Cluster-coherent flags** — change a **shared artifact across the cluster's
  replicas**, chiefly the **optimized dataflow** (optimizer features). Replicas
  of a managed cluster are interchangeable and run the *same* installed plan; the
  feature set is consumed once, in `environmentd`, at plan time, and stored on
  the cluster (`OptimizerFeatureOverrides` on `ClusterConfig`). There is **no
  replica in scope** when that value is read. This is use case 1.

A cluster-coherent flag must be **replica-independent**: you cannot have two
replicas of one cluster running plans optimized under different feature sets. To
*enforce* that, cluster-coherent flags are evaluated against a context that
deliberately carries no replica/size attributes — which is exactly what a
separate `cluster` context kind provides. Folding cluster identity into replica
attributes only would make it possible to write a rule that resolves a
plan-level flag differently per replica (incoherent, or nondeterministic if we
picked a "representative" replica). Hence two kinds:

```
context kind = "cluster"
  key:  <cluster id>
  attrs: cluster_id, cluster_name, is_builtin

context kind = "replica"
  key:  <replica id>
  attrs: replica_id, replica_name, is_builtin,
         replica_size, replica_size_family,
         cluster_id, cluster_name        # the owning cluster, for combined rules
```

Evaluation composes these into the existing multi-context per pass:

- **Cluster-coherent resolution** (optimizer features): evaluate with
  `environment + organization + build + cluster`. Replica-free, so the value
  cannot vary by replica.
- **Replica-local resolution** (dyncfg to `clusterd`): evaluate with
  `environment + organization + build + cluster + replica`. Including the cluster
  lets a rule combine both axes, e.g. *"size family `D` **and** cluster `foo`."*

LD contexts in a multi-context are **siblings, not a hierarchy** — there is no
attribute inheritance between kinds, but a targeting rule can reference
attributes from *any* kind present in the evaluated multi-context (each rule
clause is a `(kind, attribute)` pair). This is why every scoped evaluation must
keep the `environment` / `organization` / `build` contexts in the bundle
*alongside* the scope context, rather than evaluating the `cluster` / `replica`
context standalone: it is what lets rules cross axes (e.g.
`environment.cloud_provider = "aws" AND cluster.cluster_name = "..."`). We do
**not** copy environment attributes onto the cluster/replica contexts — that
would be redundant and risk drift; the environment context simply rides along.
Concretely, `pull_scoped` adds the scope context to the *same*
`MultiContextBuilder` that `ld_ctx` already populates with
environment/organization/build, rather than replacing it.

The `replica` context carries the owning cluster's identity as attributes so that
replica-local flags can still be cluster-targeted without a second evaluation;
the standalone `cluster` kind exists specifically for the replica-free,
cluster-coherent case.

### Size family taxonomy

The `replica_size_family` attribute needs a `size name -> family` mapping
(`D.1 -> D`, t-shirt sizes -> `legacy`, …) that **does not exist today and must
be introduced**. It belongs in the **cluster replica size map**
(`ClusterReplicaSizeMap` in `mz_catalog::config`, parsed from the
`--cluster-replica-sizes` configuration that already defines each named size's
allocation), as a new per-size `family` field. That makes the size definitions
the single authoritative source: it is available both to the sync loop (to
enumerate the in-use families and which `replica` contexts to evaluate) and to
the controller (to resolve a given replica's family at dyncfg-push time), and it
versions and deploys with the rest of the size configuration rather than living
in a second place that could drift.

The `family` field need not be populated for every size up front:
`ReplicaAllocation::family()` falls back to deriving `cc` (via `is_cc`) or
`legacy` when no explicit family is set, so the `legacy` and `cc` families are
targetable immediately, before the new families (`M`, `D`) get explicit `family`
entries. Explicit values are required only where the fallback is wrong.

This fallback must always yield a **sensible default family string**, never fail
or leave the attribute unset, because we do not control size (or cluster) naming
everywhere. In **self-managed**, operators define their own replica sizes via
`--cluster-replica-sizes`, with names we have never seen; `family()` must still
return a well-defined string for them so the `replica` context is always
complete. In practice this is harmless there: the scoped mechanism is LD-driven,
and self-managed deployments use the file-based system-parameter frontend
(`SystemParameterFrontendClient::File`) or none — so they produce no scoped
overrides and resolve to environment-wide defaults regardless of how sizes are
named. The feature is effectively a cloud-fleet operability mechanism that
degrades gracefully to env-wide elsewhere; the only hard requirement self-managed
imposes is that the family derivation has a sensible default for unknown names.

We deliberately do **not** ask LD rule authors to derive the family from the
size string with `startsWith` / `endsWith` operators. The family is a *curated
mapping*, not a stable encoding: the new families follow `<family>.<version>`
(`M.1`, `D.1`) and would prefix-match, but `cc` and the legacy t-shirt sizes
(`xsmall`, `2xlarge`, numeric `1`/`2`/`4`, …) share no clean affix, and `legacy`
is effectively *"everything not in another family"* — which cannot be expressed
as a positive affix clause at all. Affix matching would also scatter the taxonomy
across every rule and break whenever a new size is added that does not fit the
assumed pattern. The explicit attribute, sourced from the size map, avoids all of
this. We also keep the raw `replica_size` attribute for fine-grained targeting
(exactly `D.1` vs all of `D`): family is the coarse axis, size the fine one.

This is the opposite call from `is_builtin`, and the distinction is worth stating
because the two look similar ("can't we just derive it from the id / size
string?"). `is_builtin` *is* a clean invariant we fully control —
`matches!(id, ClusterId::System(_))`, equivalently the rendered id starting with
`s` — so the attribute is semantic sugar: derivable, but kept because
`is_builtin = true` is more readable in rules and does not bake the id-rendering
convention into every rule (it must be computed from the enum variant, **not** by
string-prefixing the rendered id). The rule of thumb: derive-from-string is fine
for a clean invariant we own (`is_builtin`); it is a trap for a curated product
mapping (`replica_size_family`).

### Scope declaration is required, per parameter

Every synced parameter must declare its scope class as part of its definition.
This is a hard requirement, not an opt-in convenience: it is what lets the sync
loop know which contexts to build, lets resolution know where a value may be
overridden, gives us a basis to reject nonsensical targeting, and documents the
surface area of each flag.

```rust
enum ParameterScope {
    /// Environment-wide only; no cluster/replica overrides. The default, so all
    /// existing synced parameters are unchanged.
    Environment,
    /// Cluster-coherent: env-wide base + per-cluster overrides. Evaluated with
    /// the `cluster` context (replica-free) and resolved at plan time via
    /// `OptimizerFeatureOverrides`. e.g. optimizer features.
    Cluster,
    /// Replica-local: env-wide base + per-replica / per-size-family overrides.
    /// Evaluated with the `replica` context and resolved at the controller's
    /// per-replica dyncfg push. e.g. `lgalloc`, persist pager, LZ4.
    Replica,
}
```

The class is declared where the parameter is defined — on the `Var` builder for
system vars and on the dyncfg `Config` definition — defaulting to `Environment`.
The scope class is a property of the parameter's **consumption site / coherence**,
not of any targeting attribute, so a single class per parameter is sufficient
(`Replica` already covers both size-family and per-replica targeting).

The declaration drives three things:

1. **Documentation.** Surfaced in the generated parameter docs and in an
   introspection relation, so the scope of every flag is discoverable.
2. **Evaluation.** The sync loop only builds and evaluates a `cluster` context
   for `Cluster` parameters, and a `replica` context for `Replica` parameters.
   This bounds the per-tick evaluation work to exactly what is in use and is the
   answer to the sync-cadence/cardinality concern: an environment with no
   `Replica`-scoped flags pays nothing for replica evaluation.
3. **Validation.** Because we know a parameter's class, we can detect and warn on
   an LD rule that targets an out-of-class dimension (e.g. an `Environment` or
   `Cluster` parameter targeted by `replica_size_family`). Such a rule would
   simply never match (we never build that context for that parameter), so the
   behavior is safe-by-construction; the declaration lets us surface it as a lint
   rather than a silent no-op.

### Identity & recreate semantics: dual id/name attributes

Catalog object ids are allocated from a monotonic counter
(`USER_CLUSTER_ID_ALLOC_KEY`, `src/catalog/src/durable.rs`), so a dropped or
**resized** object's id is **never reused** — a recreated cluster, or a resized
replica, gets a fresh id. We exploit this to support two opposite intents from
the *same* mechanism, selected by which attribute an LD rule matches:

| Intent | Target attribute | On drop / recreate / resize |
|---|---|---|
| **Role / identity** ("the catalog server should always behave this way"; "the legacy size family keeps lgalloc") | `cluster_name`, `is_builtin`, `replica_size_family`, `replica_size` | predicate — re-applies to any matching object, including future ones (intended) |
| **Incarnation patch** ("this specific cluster/replica is hitting a bug right now, pin a flag for it") | `cluster_id`, `replica_id` | dies with the incarnation — the recreated/resized object has a new id and matches no rule (intended) |

Because ids are never reused, an incarnation pin keyed by `*_id` cannot
accidentally carry over; because predicates key on stable names/families, role
intent survives recreate by design. The operator picks the behavior by choosing
the attribute. This is why we expose **both** identifiers on each context.

### Storage: a durable cache, written solely by the sync loop

Scoped overrides are **persisted**, so they survive an `environmentd` restart and
stay in effect while LaunchDarkly is slow to sync or unavailable. This is
required: LD may take a while to connect/stream after a restart, or be
unreachable, and during that window we must serve the last-known scoped values
rather than reverting to environment-wide defaults. It also removes an asymmetry
— environment-wide flags already persist in the catalog (via the existing
`ALTER SYSTEM` path) and survive an LD outage; the scoped layer should too.

The scoped layer is **two** durable catalog collections — one per scope, mirroring
the existing flat `system_configurations` collection rather than a single
sum-typed key — each keyed by **object id**, with an in-memory working copy used
for resolution:

```
cluster_system_configurations:  (ClusterId, parameter_name) -> value
replica_system_configurations:  (ReplicaId, parameter_name) -> value
```

The **sync loop is the sole writer**; there is no user-facing
`ALTER SYSTEM ... FOR CLUSTER` DDL (see Alternatives). A row is written **only
when the scoped evaluation differs from the environment-wide value** (see
Resolution for why *difference* — not the raw LD evaluation reason — is the
correct signal), so the collections are sparse: only objects that LD targets to a
scope-specific value get a row.

Lifecycle:

- **Startup:** load the persisted collection into the working maps, so scoped
  values are in effect immediately — no waiting for the first LD sync.
- **LD reachable:** each successful tick reconciles the collections to LD's
  current evaluation, *including removals* — an entry is dropped once the scoped
  evaluation no longer differs from the environment-wide value (e.g. its LD rule
  was removed), so changes propagate in both directions.
- **LD slow / unavailable:** keep serving the last persisted values; do **not**
  revert to the environment-wide value. This is the case that motivates
  persistence.
- **Cold cache + LD unavailable** (first-ever startup, object never yet
  evaluated): fall back to the environment-wide value — unavoidable, since we
  have never observed LD for it.

Crucially, persisting does **not** reintroduce the recreate ambiguity that sank
the `ALTER SYSTEM ... FOR CLUSTER` DDL alternative, because of the monotonic id
allocator (`USER_CLUSTER_ID_ALLOC_KEY`): ids are **never reused**, so a persisted
entry keyed by a dropped object's id is **inert** — it can never match a future
object. A recreated same-named cluster (new id) starts with no cached overrides;
a name-targeting LD rule re-applies on the next sync under the new id, while an
incarnation pin keyed by the old id never resurfaces. GC is therefore *not* a
correctness concern, only hygiene: dead-object entries load into the in-memory
copy **inert** (they can never match a live object) and are removed on the **first
reconcile after startup**, and on each successful reconcile thereafter — there is
no separate startup-time prune pass. This is the same non-reuse property the
dual-identity scheme already relies on (§Identity & recreate semantics).

#### Feature gate

The entire scoped-parameter mechanism is gated behind a new
`enable_scoped_system_parameters` dyncfg (defined in `mz_adapter_types::dyncfgs`, default `false`).
With the gate off, behavior is exactly today's environment-wide-only resolution, so the feature is strictly opt-in.
The gate is checked at the single sync-loop chokepoint, `sync_scoped_params`.
When off, no cluster or replica contexts are evaluated, and any overrides that a previously-enabled run persisted are cleared once, so resolution falls back to the environment-wide value everywhere.
When on, cluster/replica evaluation begins with no deploy, since every dyncfg is mirrored as an LD-synced, `ALTER SYSTEM`-settable system var.
It is a dyncfg rather than a `feature_flags!` entry because the latter carries the catalog item-parsing rehydration contract for SQL/syntax features, which this runtime subsystem toggle has no part in.

### Resolution: two existing per-scope boundaries

We do **not** rewrite `SystemVars` into a universally scope-aware store. Both use
cases resolve at boundaries that already carry the right context:

**(a) The compute & storage controllers' per-replica config push — replica-local
flags (use case 2).** environmentd already pushes a dyncfg `ConfigSet` /
`ConfigUpdates` to each replica through the controllers, and knows each replica's
cluster, size, and size family at push time. We resolve there:
`effective = global ⊕ replica_local_override` for that replica. **`clusterd`
needs no changes** — it keeps reading its dyncfg set and simply receives
size/replica-appropriate values.

**(b) The optimizer's per-cluster feature set — cluster-coherent flags (use case
1).** The optimizer already takes `OptimizerFeatures` derived from `SystemVars`
plus the per-cluster `OptimizerFeatureOverrides` on `ClusterConfig`. We add a
second source of those overrides: the cluster's resolved scoped layer feeds the
overrides at plan time, at the (already cluster-aware) sequencing sites in
`src/adapter/src/coord/sequencer/inner/*`. This is the direct path to
"optimizer flags in LD per cluster."

The cluster-scoped layer **feeds** `OptimizerFeatureOverrides`; it does **not**
replace the mechanism. The overrides struct stays as the composition point and
simply gains LD as one input. It is still needed independently for per-statement,
what-if overrides — notably `EXPLAIN WITH (<flag>)`, which previews how a plan
*would* look with a flag flipped. That per-statement override is the most specific
scope of all and sits above the cluster layer (see precedence below).

The cluster-scoped working copy must live in `CatalogState` (behind
`Arc<Catalog>`), **not** on the coordinator. Cluster-coherent overrides must also
apply on the fast-path peek route (`frontend_peek`) and inside the bootstrap
re-optimization closure, and both of those hold only a catalog snapshot, not the
coordinator. Threading the working copy through the catalog is what makes it
visible at *every* cluster-aware resolution site rather than just the sequencing
path.

Precedence for replica-local flags, lowest to highest:
`Global < ReplicaSizeFamily < Replica(id)` (a specific replica pin beats a size
family rule).

For cluster-coherent flags the ordering is **specificity-then-source**, lowest
to highest:

```
env-wide LD  <  manual CREATE CLUSTER ... FEATURES  <  cluster-scoped LD  <  EXPLAIN WITH (per-statement)
```

i.e. a more specific scope wins, and at equal specificity **LD beats manual**.
The per-statement `EXPLAIN WITH (<flag>)` override is the most specific and wins
for that one statement's preview, which is why the `OptimizerFeatureOverrides`
mechanism is retained rather than replaced by the cluster-scoped layer.
This is consistent with the existing global behavior — LD already overrides a
manual `ALTER SYSTEM SET` wherever LD serves a value (the sync loop rewrites the
param every tick), and `FEATURES` is likewise a system-only operator knob, so it
gets the same treatment. It also *preserves* today's behavior that a manual
`FEATURES` override beats the env-wide defaults (confirmed by `override_from` in
`src/repr/src/optimize.rs`): env-wide LD is not cluster-specific, so a deliberate
per-cluster manual pin still beats it, while a cluster-specific *LD* rule beats
the manual pin.

Crucially, whether a cluster-scoped LD value beats a manual `FEATURES` pin is
decided by **whether LD assigns the cluster a value that differs from the
environment-wide value** — not by the raw `variation_detail` reason. The reason is
the wrong signal, in two ways:

- `FALLTHROUGH` *is* the environment-wide value (it is what every context gets
  absent a specific match). Treating it as a cluster opinion would let any
  globally-on flag silently override a deliberate per-cluster `FEATURES` pin —
  coarse scope beating fine scope, the opposite of "more specific wins" — and
  would record a row for *every* live object, making the collections dense rather
  than sparse.
- A `RULE_MATCH` does not say *which* context kind's attributes matched. An
  environment-level rollout rule (`environment.cloud_provider = "aws"`) also
  reports `RULE_MATCH`, indistinguishable from a cluster-specific rule without
  inspecting the rule's clauses — which the SDK does not expose.

Comparing the scoped evaluation to the baseline (env-only) evaluation captures
exactly *"LD has a **cluster-specific** opinion"*: the cluster context changed the
result. So `cluster-scoped LD` in the ordering above means **the cluster-scoped
evaluation differs from env-wide**; where it does not differ, there is no cluster
opinion and the manual `FEATURES` pin stands. This is the same `differs from
env-wide` test used at replica scope (which has no manual layer), so the recording
rule is **uniform across both scopes** — there is no need to special-case
`FALLTHROUGH` per scope.

The one behavior this does *not* support is reasserting the env-wide value on a
cluster purely to clear a `FEATURES` pin (LD value equal to env-wide cannot
override the pin, because it is indistinguishable from "no cluster opinion").
That is handled by removing the pin, not via LD — an acceptable trade for a simple,
introspection-free rule.

The accepted trade-off, stated plainly: an operator's manual `FEATURES` pin can be
overridden by a cluster-scoped LD rule (whenever LD targets the cluster to a
*different* value). That is the same bargain operators already accept for
`ALTER SYSTEM`, so it is consistent rather than novel.

### Worked examples

**Use case 1 — an optimizer feature on `mz_catalog_server` only.**
LD rule: `cluster_name = "mz_catalog_server"` → variation X. Each tick the sync
loop evaluates the `cluster` context for the catalog server (replica-free), sees
X differ from the env-wide value, and records `Cluster(s2) -> {feature: X}`. At
plan time the coordinator feeds that into `OptimizerFeatureOverrides` for the
catalog server; other clusters are unaffected.

**Use case 2 — legacy sizes keep `lgalloc`, `D.1` gets pager + LZ4.**
LD rules: `replica_size_family = "legacy"` → `lgalloc` on; `replica_size_family =
"D"` → pager + LZ4 on. The sync loop evaluates the `replica` context per live
replica; the controller resolves per replica at dyncfg push: a `D.1` replica
gets pager+LZ4, a legacy replica gets `lgalloc`. No `clusterd` change.

**Incident pin.** LD rule: `replica_id = "u42-replica-3"` → flag Y. Applies to
exactly that replica; if it is resized or recreated (new id) it reverts to the
env-wide value automatically.

## Minimal Viable Prototype

Ordered to de-risk the cleaner boundary first:

0. **Scope declaration.** Add the `ParameterScope` enum and the declaration hook
   on the `Var` / dyncfg `Config` definitions, defaulting to `Environment`.
   Annotate the parameters the two use cases need (the target optimizer features
   as `Cluster`; `lgalloc` / pager / LZ4 as `Replica`). This is a prerequisite
   for the evaluation steps below, since the sync loop keys off the class.
   - **Kill-switch (step 0's companion).** Add the `enable_scoped_system_parameters` dyncfg (default `false`) and gate `sync_scoped_params` on it, so the mechanism ships dark and the default behavior stays environment-wide-only until it is turned on with no deploy.
1. **Replica-local dyncfg push (use case 2), in-memory first.** Add the `replica`
   context kind and per-replica evaluation; compute per-replica/size override maps
   in `environmentd` (in-memory, not yet persisted); resolve at the controller's
   dyncfg push. Demonstrate `replica_size_family = "legacy"` toggling `lgalloc`
   only on legacy replicas. This validates the eval + push path cheaply, with no
   catalog/`clusterd` changes; persistence follows next.
2. **Persist the scoped cache.** Add the durable catalog collection keyed by
   object id, load it into the working maps on startup, and have the sync loop
   reconcile it (incl. removals + lazy prune). Demonstrate that scoped values
   survive an `environmentd` restart and an LD outage, falling back to
   environment-wide only on a cold cache.
3. **Cluster-coherent optimizer flags (use case 1).** Add the `cluster` context
   kind (replica-free evaluation); feed the resolved cluster layer into
   `OptimizerFeatureOverrides`. Demonstrate an optimizer feature differing on
   `mz_catalog_server` vs. user clusters.
4. **Introspection.** Expose resolved scoped values via `mz_internal` relations
   for debugging (e.g. `mz_cluster_system_parameters`,
   `mz_replica_system_parameters`).

Extend `test/launchdarkly/mzcompose.py` (which already targets by `contextKind`)
with `contextKind = "cluster"` and `"replica"` cases.

## Alternatives

- **Single `replica` context kind, cluster as attributes only.** Simpler, and
  sufficient *if* this mechanism only drove replica-local flags. Rejected because
  cluster-coherent flags (optimizer features — an explicit near-term goal) must be
  resolved replica-free; a replica-only model cannot express "evaluate
  independent of any replica" and would permit incoherent per-replica plan
  divergence.
- **User-facing `ALTER SYSTEM ... FOR CLUSTER` DDL.** Note that *persistence
  itself is adopted* (see Storage) — what we reject is exposing it as **user
  DDL**. A human-writable scoped setting would force a name-vs-id binding choice,
  invite a second writer the sync loop overwrites, and need eager dangling-entry
  GC. The internal, id-keyed, sync-loop-only durable cache gets the durability
  without any of these: keying by id avoids the binding choice, a sole writer
  avoids the conflict, and non-reused ids make stale entries inert (lazy GC).
- **Pure in-memory cache (no persistence).** Simpler, but loses the last-known
  values across an `environmentd` restart and during an LD outage — exactly when
  we most need them — so resolution would wrongly revert to environment-wide
  defaults until LD re-syncs. Rejected for that reason.
- **Universal `SystemVars` layering (`system < cluster < session`).** General but
  large/risky, touching every read site. Resolving at the two existing boundaries
  covers both use cases with far less blast radius; this can evolve toward general
  layering later if needed.
- **Per-individual-replica LD contexts for everything.** Unnecessary; size-family
  predicates cover the durable cases and incarnation pins cover the rest, and
  (per the billing note) cardinality is not a cost driver on the server SDK
  anyway.
- **Switch / add another flag tool (Unleash, Flagsmith, Statsig, OpenFeature).**
  None removes the Materialize-side storage/resolution work, and several are less
  expressive than LD's multi-context model. OpenFeature is the only one worth
  considering, purely for backend portability; orthogonal to this design.

## Open questions

Both remaining items are **deferrable** — they are operational tuning concerns,
not design decisions, and neither blocks the MVP. Both can be revisited once the
mechanism is in use and we have real numbers.

- **User-cluster context-list growth (deferred).** Billing is unaffected, but
  keying cluster contexts by id means resized/recreated objects accumulate in the
  dashboard Contexts list over time. `anonymous` contexts are likely enough; if
  the list becomes unwieldy we can add a periodic prune or only emit contexts for
  objects matching an active rule. Not a blocker — for the MVP (builtin clusters
  + a handful of size families) the list stays small.
- **Sync cadence (deferred).** Per-tick evaluation grows from 1 to roughly
  `1 + |Cluster-scoped objects| + |live replicas with Replica-scoped flags|`.
  The scope-declaration requirement already bounds this to parameters actually in
  use, and on the server SDK it is free billing-wise. If the per-tick cost ever
  matters, scoped evaluation can run on a slower cadence than the base pull. Defer
  until we have evaluation-cost numbers.
