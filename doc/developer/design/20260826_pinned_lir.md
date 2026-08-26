# Pinning LIR plans to offer stable customer experiences

- Associated: [#30233 optimizer release engineering](https://github.com/MaterializeInc/materialize/pull/30233),
[#8768 optimizer crate](https://github.com/MaterializeInc/database-issues/issues/8768)

F/K/A "the customer trade-off problem"

## The Problem

Customers run operational workloads on Materialize.
Changes to Materialize can threaten the stability of those workloads---particularly changes in the optimizer.

To date, we have managed optimizer changes using feature flags (e.g., `enable_cast_elimination`, `enable_eager_delta_joins`).
Not every feature can be feature flagged (e.g., changing `MirRelationExpr` to hold `Repr*` types), though, and we do not have much in the way of tooling for feature flags.

It is hard for us to make changes in the optimizer that won't cause some customers to have a bad time---even if some customers have a much better time with those changes.
(Whence the name, "the customer trade-off problem".)
We need a way to change the optimizer without disrupting customer workloads.

## Success Criteria

Customers---self-hosted or cloud---will be able to qualify new optimizers before migrating to them.

Optimizer engineers will be able to develop features with confidence, namely:

  - introducing new transforms (e.g., cost-informed late materialization)
  - updating existing transforms (e.g., new join planning)
  - targeting new dataflow operators (e.g., many-to-many reduce)
  - changing AST types for HIR, MIR, or LIR (e.g., LIR many-to-many reduce, MIR window functions)

Optimizer engineers will be able to deploy hotfixes to any active optimizer using the normal weekly release.

## Out of Scope

Mztrail---testing on customer workloads---would help us predict when optimizer changes will affect customers.
(It would also help the most proactive customers, who could run tests themselves.)
While pushing in this direction is good, important work, it's a bigger bite than what's proposed here.
Moreover, it's not clear how to use mztrail in a self-managed context.

There are two closely related but not identical problems:
 1. **`our-bad`** MZ optimizer changed and it broke on redeploy.
 2. **`your-bad`** You changed something and it broke in staging.
We are addressing the **`our-bad`** case exclusively.
It is very important that we solve the "optimizer image" problem (you should be able to write SQL to get the good dataflow) and the "optimizer discontinuity" problem (you should be able to make small changes and not experience discontinuous performance, part of **`your-bad`**)---at some point, but not with this.


## Weighing Alternatives

- **`optimizer-versions`** Separate optimizer versions, settable per-cluster using a system-level privilege.
- **`feature-flags`** Feature flag everything, building tooling to support eng, field eng, and customers.
- **`plan-pinning-v1`** Offer an explicit way to fix a query plan in a per-cluster way.
- **`plan-pinning-v2`** All plans are pinned by default.
- **`query-hints`** Offer query hints or special syntax to control query plans.

What are the pros and cons of each approach?

### `optimizer-versions`

Pros:

  + Fixed, known configurations.
  + Per-cluster control.
  + Forces more unified optimizer interface.
  + Ties in neatly with related ideas of "a separate optimizer process".
  + Moderately flexible versioning: we can cut new optimizer versions as we please, and do not need to fix a support window in advance.

Cons:

  - Code duplication. (Somewhat mitigated by `git subtree`.)
  - We do not know what kind of support window we will want, and may get backed into things we end up disliking.
  - Coarse-grained offramp: you can change versions, but that's it.
  - Coarse-grained application: regressions are typically local, even within a customer. So optimizer versions may not cut it fine enough---it may be just one query on the cluster that needs a different optimizer.
  - Engineering burden of refactor.
  - Engineering burden of backporting.
  - Punts on release qualification.

### `feature-flags`

Pros:

  + The status quo (less the tooling).
  + Fine-grained control: you can offramp from old feature settings flag-by-flag. (In principle, at least.)
  + Flexible: we can create new feature flags as we plase, and we do not need to fix their support windows in advance.

Cons:

  - Difficult scaling granularity: not every feature is easy to flag. `**optimizer-versions**` is essentially a particular approach to `feature-flags`, where the flag granularity is "set of optimizer features and types."
  - Exponentially many configurations---we can't test every combination of flags, and flags interact.
  - Who flips the bits? If it's us: high support burden. If it's someone else: what if they break things?
  - Unknown support windows, and we have not historically done a good job managing feature flags.

### `plan-pinning-v1`

Pros:

  + Per-cluster control.
  + Ties in neatly with related ideas of "production clusters", guarantees, and auto-scaling.
  + Ties in neatly with related ideas of "DDIR" or some other stable, low-level interface.
  + Offers the most reliable possible experience---a fixed LIR plan would be stable even if bugfixes in MIR cause queries to change.

Cons:

  - Any changes to the plan and you lose your pin. (Mitigation: use MVs on different clusters to separate the units you care about.)
  - Any changes to other objects on the cluster.
  - LIR is a not currently stored anywhere (but is a stable interface between MIR and rendering). DDIR does not actually exist.
  - Once we are committed, may be hard to back out of. (Mitigation: deploy this is as an unstable feature with a customer partner.)
  - More durable state.
  - Need to manage migrations for LIR. (Mitigation: best effort.)

### `plan-pinning-v2`

Pros:

  + All of the positives of `plan-pinning-v1`.
  + Defaulting to pinned helps keep operational workloads operational.
  + Clean factoring of mechanism ("LIR is durable") from tools for its management (`COPY CLUSTER`, `EXPLAIN REPLAN ...`).

Cons:

  - All of the cons of `plan-pinning-v1`, _except_ it's easier to make changes to objects on the cluster.

### `query-hints`

Pros:

  + The finest-grained control.
  + Avoids/defers the need to have smart query planning.

Cons:

  - Major parser overhaul.
  - Major AST overhaul.
  - Major transform overhaul.
  - All known forms of this are brittle.
  - Hard to specify emergent properties (e.g., what to do with operators that do not syntactically appear in the query plan).
  - One-way door: once it's in, it's not going away.
  - Devolves to plan pinning.

## Solution Proposal

We propose using **`plan-pinning-v2`**.
The ability to pin plans _exactly_ solves the **`our-bad`** problem.
It's also superior to the alternatives.
We see it as superior to **`feature-flags`** because we can work more flexibly (change types!) with less uncertainy (known configs!).
(Feature flags will of course continue to exist!)
We see it as superior to **`query-hints`** because we don't want to add query hints.

A prior version of this design doc and [the prior design doc in #30233](https://github.com/MaterializeInc/materialize/pull/30233) proposed **`optimizer-versions`**.
A [prior version of this design doc in #35441](https://github.com/MaterializeInc/materialize/pull/35441) proposed **`plan-pinning-v1`**.

### Why did we switch to plan pinning?

`**optimizer-versions**` overfits to particular engineering challenges (wanting to make certain AST changes).
But recent work on repr types has shown that we can change the tires while the car is moving---we simply have to be careful.
Versioning the optimizer has a high engineering burden up front and promises a high maintenance burden in the future.
Refactoring to have a clean optimizer crate is a good idea, but versioning is a heavyweight way to achieve what could be a lightweight goal.

The balance tips further in **`plan-pinning`**'s favor when we consider that pinned plans are not merely a useful way for customers to have more confidence in Materialize, they are a way to help us identify clusters that are candidates for autoscaling and immediate incident escalation---production clusters.

### Why did we switch to `plan-pinning-v2`?

During broader conversations about the "cluster lifecycle", it became clear that it's much easier to simply treat _all_ plans as durable.
This obviates questions about freezing/unfreezing clusters and what to do when dependent objects change---we can reuse existing logic around `ALTER`, `DROP`, and `CREATE`.
During a business logic change blue/green swap, old definitions will be dropped and new plans will be written---but storing LIR plans by default means unchanged objects will keep their plans.
Treating `COPY CLUSTER` and `EXPLAIN REPLAN` as orthogonal but complementary features makes the design more compelling.

## Minimal Viable Prototype

Clusters will pin LIR plans (`DataflowDescription<LirRelationExpr>`s) by default.
These plans will be stored in persist shards, referenced in the catalog.
When a cluster starts up, it will attempt to read existing LIR plans and deploy _those_, rather than recompiling plans.

### What is the SLA?

Our pinning will start off as "best effort".
At any point, we may simply throw up our hands and replan.
Users should be notified if pinned plans are replanned, but it should not necessarily rise to the level of pinging an on-call engineer---say, an escalation rather than an incident.
A possible success metric for plan pinning (beyond e.g., overall usage/number of pinned plans) is how _few_ replans are forced to occur.

### What happens at an upgrade?

During a 0dt upgrade, the new environmentd will read the catalog and spin up dataflows for the plans recorded in persist shards.
This way, the new environment will continue to operate with the existing plans---stability!
We may need to migrate these plans if, e.g., there was a change in the LIR definition.
(We use a [schema registry](https://github.com/MaterializeInc/materialize/pull/37814) to track the need for these migrations.)

### What happens when business logic changes?

Whenever a planned object (materialized view; index) changes, it must generally be dropped and then recreated.
Here, dropping loses the old pinned plan; recreating the object stores a new one.
There is a challenge, however: what if only a few objects need to change, but other plans should stay the same?

The proposed solution is a `COPY CLUSTER foo TO bar` command that creates a cluster `bar` that is identical to `foo`, i.e., it has the same plans.
One can then alter objects on `bar`, dropping and recreating only what's needed.
When `bar` is satisfactory, a blue/green swap deploys the new configuration.
Such an interactive usage should be permissible, but will not be best practice: one should really use a tool like mzdeploy to do a "slim deployment".

We will need to adapt mzdeploy to use `COPY CLUSTER` for slim deployments.
We may want to do the same for DBT.
In either case, we will likely want `COPY CLUSTER` to create a cluster with _no_ replicas.
This way, the user can arrange objects the way they like and _then_ create replicas---simulating a full hydration, giving them confidence that their definitions fit in the cluster.
There is some footgun risk here, and we propose that bare uses of `COPY CLUSTER` create replicas, but that slim deployments should run `COPY CLUSTER ... WITH (REPLICATION FACTOR = 0)`, i.e., carefully ensure that no dataflows will actually be created at first, and later running `ALTER CLUSTER` to set a larger replication factor.

### How do users get improvements?

As the optimizer improves, existing pinned plans may lag behind our best possible performance.
How do we ensure that users can see the improvements we make?

Users changing business logic will necessarily use the latest version of the optimizer, so new objects will enjoy the benefits.
We should also support a user-facing `EXPLAIN REPLAN` for individual objects (what would my plan look like now?) and `EXPLAIN REPLAN CLUSTER` (how have my plans "drifted" from what I would get if I replanned today?).
In an ideal world, we could offer insight into the diffs of the replanned objects.

Users may also get improvements when we cannot load the LIR for some reason, e.g., a deliberate backwards incompatible migration, a bug.

### How does LIR change?

Several PRs have rearranged MIR and LIR.
- New structures
  + [#36544 Defined a stable LIR scalar expression](https://github.com/MaterializeInc/materialize/pull/36544)
  + [#37410 LIR aggregate expression](https://github.com/MaterializeInc/materialize/pull/37410)
- New abstractions
  + [#36647 Abstracted away `Eval` and `Columns` traits for working with scalars](https://github.com/MaterializeInc/materialize/pull/36647)
  + [#36759 Parameterized MFP infrastructure](https://github.com/MaterializeInc/materialize/pull/36759)
  + [#37961 Parameterized `UnaryFunc`](https://github.com/MaterializeInc/materialize/pull/37961)
- New migrations
  + [#37814 LIR schema registry to support migrations](https://github.com/MaterializeInc/materialize/pull/37814)

We cut things such that MIR and LIR share the `*Func` definitions and other infrastructure, though there is some work towards pulling them further apart (e.g., [#37409 measure how often we need to dip back into MIR to propagate literal constraints in LIR](https://github.com/MaterializeInc/materialize/pull/37409)).

A `DataflowDescription<LIR>` will be stored as JSON in a persist shard pointed to by the catalog.
Splitting up the LIR in this way means that (a) the catalog doesn't scale (as much) with object plan size and (b) we can parallelize parsing of LIR plans.

### Where does LIR live in the catalog?

It in `CatalogState` (alongside the `CatalogItem`'s entry), or as a sidecar in `Catalog` itself (like the `ExpressionCacheHandle`).
This part of the catalog seems to be in flux, but `CatalogState` seems right---pinned plans should be stored with their associated IDs.

Storing a pointer to a persist shard holding the JSON-serialized LIR plan instead of putting the full plan in the catalog itself offers several benefits.
First, we can decode and migrate plans in parallel.
Second, we don't need to send a serialized plan to clusters---we can just point them at persist.
Third, it means that the catalog doesn't scale with object _size_ (though of course it scales with object _count_).

Plans should be written eagerly.
Migration can occur when we load a plan, but that means writing a new persist shard with the migrated plan.

## Open questions

### What happens when we move to DDIR?

A nice property of plan pinning is that we're always free to go lower.
Currently, a pinned LIR plan will render directly.
If we rearchitect things to convert that LIR plan to DDIR, that will be "transparent" to the pinned plan (if we do a good job).
If we later decide to save the DDIR rather than LIR, that will be as transparent as the original move was.

### What happens to `EXPLAIN OPTIMIZED PLAN` for pinned LIR plans?

`EXPLAIN OPTIMIZED PLAN` shows MIR by... compiling to MIR.
But an LIR plan pinned six months ago may have nothing to do with the MIR we get now---and so this `EXPLAIN` is showing the explain of the _replan_, not the original plan.
(`EXPLAIN PHYSICAL PLAN` is the default, and will not have this problem.)

Right now, I believe only Gábor uses `EXPLAIN OPTIMIZED PLAN`.
But in the event someone would like to see MIR for a pinner LIR plan, we would simply not have it.
We _could_ store a cached version, either a text (with fixed options) or as some kind of structure---though our aim was to _not_ have to serialize MIR.
I think the best approach here is to improve the default LIR-based `EXPLAIN PLAN` enough so that Gábor stops using `EXPLAIN OPTIMIZED PLAN`.

### How does this interact with the expression cache?

We will likely be able to use pinned LIR to deprecate the expression cache, but there should be no interference at first---though we will want to carefully prioritize which we consult (LIR first, then fall back to the cache).
