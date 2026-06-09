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
- Behaviour is well-defined when LaunchDarkly is unreachable (same fallback
  story as today's global flags: fall back to the environment-wide value).

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

> Action item: confirm with the LD account owner that we are on the standard
> service-connections model and not a custom contract with per-context metering.

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

The `replica` context carries the owning cluster's identity as attributes so that
replica-local flags can still be cluster-targeted without a second evaluation;
the standalone `cluster` kind exists specifically for the replica-free,
cluster-coherent case.

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

### Storage: in-memory, reconciled from LD — not `ALTER SYSTEM ... FOR CLUSTER`

The source of truth is the LD ruleset (which targets by name/attributes), and
the sync loop re-evaluates every tick. So the scoped layer is best modeled as a
**cache of a continuous LD evaluation**, not durable user-authored state:

- The scoped layer is a `BTreeMap<ClusterId, Overrides>` and
  `BTreeMap<ReplicaId, Overrides>` held in `environmentd`, rebuilt each sync tick
  by evaluating LD (with the appropriate contexts) for each live cluster/replica
  in the catalog. We store only values that differ from the environment-wide
  value, keeping the maps sparse.
- Drop a cluster/replica → it leaves the catalog → it is no longer evaluated →
  it falls out of the map. Nothing durable lingers, so nothing can bind a stale
  override to a recreated object.
- On `environmentd` restart or LD outage, the maps are simply empty until
  re-derived, and resolution falls back to the environment-wide value — the same
  fallback story global flags have today.

We deliberately **reject** persisting scoped overrides as `ALTER SYSTEM SET ...
FOR CLUSTER <name>` DDL (see Alternatives): it would force a name-vs-id binding
decision, tempt a second (human) writer that the sync loop would overwrite, and
require explicit GC of dangling entries — reintroducing exactly the recreate
ambiguity the in-memory model avoids.

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

Precedence for replica-local flags, lowest to highest:
`Global < ReplicaSizeFamily < Replica(id)` (a specific replica pin beats a size
family rule).

For cluster-coherent flags the ordering is **specificity-then-source**, lowest
to highest:

```
env-wide LD  <  manual CREATE CLUSTER ... FEATURES  <  cluster-scoped LD
```

i.e. a more specific scope wins, and at equal specificity **LD beats manual**.
This is consistent with the existing global behavior — LD already overrides a
manual `ALTER SYSTEM SET` wherever LD serves a value (the sync loop rewrites the
param every tick), and `FEATURES` is likewise a system-only operator knob, so it
gets the same treatment. It also *preserves* today's behavior that a manual
`FEATURES` override beats the env-wide defaults (confirmed by `override_from` in
`src/repr/src/optimize.rs`): env-wide LD is not cluster-specific, so a deliberate
per-cluster manual pin still beats it, while a cluster-specific *LD* rule beats
the manual pin.

Crucially this is decided **per feature, only where LD actually serves a value**,
mirroring the global case (where a manual value survives exactly when LD is
silent). The implementation uses the LD evaluation *reason* from
`variation_detail` to distinguish "LD has an opinion" (`RULE_MATCH` /
`TARGET_MATCH` / `FALLTHROUGH`) from "LD is silent" (`FLAG_NOT_FOUND` / error);
where LD is silent, the manual `FEATURES` value stands.

The accepted trade-off, stated plainly: an operator's manual `FEATURES` pin can
be overridden by a cluster-scoped LD rule. That is the same bargain operators
already accept for `ALTER SYSTEM`, so it is consistent rather than novel.

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

1. **Replica-local dyncfg push (use case 2).** Add the `replica` context kind and
   per-replica evaluation; compute per-replica/size override maps in
   `environmentd` (in-memory); resolve at the controller's dyncfg push.
   Demonstrate `replica_size_family = "legacy"` toggling `lgalloc` only on legacy
   replicas. Zero SQL / catalog / `clusterd` changes.
2. **Cluster-coherent optimizer flags (use case 1).** Add the `cluster` context
   kind (replica-free evaluation); feed the resolved cluster layer into
   `OptimizerFeatureOverrides`. Demonstrate an optimizer feature differing on
   `mz_catalog_server` vs. user clusters.
3. **Introspection.** Expose resolved scoped values via `mz_internal` relations
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
- **Persisted `ALTER SYSTEM ... FOR CLUSTER` DDL.** Durable and introspectable,
  but forces a name-vs-id binding choice, invites a human writer the sync loop
  overwrites, and needs explicit dangling-entry GC on drop. The in-memory
  reconciled model sidesteps all three. (We can revisit durable persistence later
  if surviving an LD outage with non-default scoped values becomes a requirement.)
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

- **Opt-in surface.** Should every synced parameter be scopable, or should
  parameters declare whether they are cluster-coherent, replica-local, or neither
  (e.g. on the `Var` / dyncfg definition)? Declaring it bounds the evaluation
  surface and prevents nonsensical scoping (e.g. an optimizer flag targeted by
  size family).
- **Size family taxonomy.** Where is the authoritative `size name -> size family`
  mapping (`D.1 -> D`, t-shirt -> `legacy`)? It must be available to the sync loop
  (which families to evaluate) and the controller (resolve per replica). Likely
  the cluster replica size configuration; confirm.
- **User-cluster context-list growth.** Billing is unaffected, but keying cluster
  contexts by id means resized/recreated objects accumulate in the dashboard
  Contexts list over time. Is `anonymous` enough, or do we want a periodic prune /
  to only emit contexts for objects matching an active rule?
- **Sync cadence.** Per-tick evaluation grows from 1 to roughly
  `1 + |clusters| + |live replicas|`. With anonymous server-side contexts this is
  free billing-wise, but confirm the SDK evaluation cost is acceptable, and
  whether scoped evaluation should run on a slower cadence than the base pull.
