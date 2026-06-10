# Zero-Downtime Upgrades - Milestone 1

NOTE: At the time this was merged, we were past zero-downtime upgrades,
milestone 2 already. But we merged this so that historic context is available
in our repository of design docs. Some of the descriptions in here are half
baked, but overall the implementation followed closely what we laid out here.

## Context

We want to build out the ability to do zero-downtime upgrades. Here we outline
the first of multiple incremental milestones towards "full" zero-downtime
upgrades. We spell out trade-offs and hint at future milestones at the end.

Zero-downtime upgrades can encompass many things, we therefore carefully carve
out things that we _do_ want to provide with this first milestone and things we
_don't_ want to provide. We will use two basic notions in the discussion:

- _responsiveness_: The system, a part of it, or an object responds to queries
  at some timestamp with “sufficiently low” latency.
- _freshness_: The system, a part of it, or an object responds to queries with
  data that is “sufficiently recent”.

* _sufficient to be determined by customers, for their workloads_

We use the term _availabllity_ to describe the simultaneous combination of
_responsiveness_ and _freshness_.

An object that is not responsive is unavailable, it cannot be queried at _any_
timestamp. Objects that are only responsive can be queried when downgrading the
consistency requirement to _serializable_. Only objects that are _available_
can be queried at our default _strict serializable_ with sufficiently low
latency.

Today, we provide neither responsiveness nor freshness for in-memory objects
(read: indexes) because when we upgrade we immediately cut over to a new
`environmentd` process and only then spin up clusters, which latter will start
re-hydrating those in-memory objects.

The work around zero-downtime upgrades can be largely seen as futzing with
availability and freshness of objects and if/when we provide those.

## Goals

We define the goals in terms of what of responsiveness or freshness we want to
provide for objects during or right after an upgrade.

- freshness for compute objects that don't transitively depend on sources
- responsiveness for compute objects that do transitively depend on sources (or
  don’t depend on anything)
- the above two imply: cut connections over within reasonable time from old to
  new adapter processes

## Non-Goals

Implicitly, everything not mentioned above, but explicitly:

- a consistent cut-over timestamp across all objects
- smart (idk 🤷) handling of behavior changes: changes to query plans, changes
  to builtin functions
- freshness for sources
- additional release verification (whatever that may be) during the upgrade
  process

## Discussion of Goals

Sources are always responsive, because they are ultimately backed by persist
shards, and you can always read from those. We do have "almost freshness" for
most sources, because only UPSERT sources require re-hydration, but there is a
window of non-freshness while the source machinery spins up after an upgrade.

Reducing that window for all types of sources is not a goal of this first
milestone! Getting all sources to be fresh after upgrades is sufficiently hard,
so we're punting that for the first milestone. Transitively, this means we
cannot provide freshness for compute objects that depend on sources.

Today, in-memory compute objects offer neither responsiveness nor freshness
immediately after an upgrade: they are backed by ephemeral in-memory state that
has to be re-hydrated. This is different from sources and materialized views,
which already offer responsiveness today.

The other non-goals also require non-obvious solutions and some light thinking
but we believe any kind of zero-downtime cutover already provides benefits, so
we want to deliver that first and then incrementally build on top in future
milestones.

## Current Upgrade Orchestration (simplified!)

1. Orchestration spins up `environmentd` at new version.
2. New `environmentd` checks that it can migrate over durable environment state
   (catalog), then signals readiness for taking over and sits and waits for the
   go signal. Crucially, by this time it does not spin up controllers/clusters.
3. When signaled, make migrations permanent. This fences out the old
   `environmentd` and cluster processes.
4. Commence rest of bootstrapping, bringing up clusters, which in turns starts
   re-hydrating compute objects and sources.

The fact that we only spin up new clusters in step #4 is what leads to
user-perceived downtime for compute objects and sources that need re-hydration.

## Implementation Sketch

A number of components need to learn to start up in a _read-only mode_, where
they don't affect changes to durable environment state, including persist
shards that they would normally write to.

Then we can do this when upgrading:

1. Orchestration spins up `environmentd` at new version.
2. New `environmentd` checks that it can migrate over durable environment state
   (catalog), **then spins up controllers (and therefore clusters) in read-only
   mode. Only when everything is re-hydrated do we signal readiness for taking
   over.**
3. When signaled, make migrations permanent. This fences out the old
   `environmentd` and cluster processes.
4. Commence rest of bootstrapping, **take controllers and clusters out of
   read-only mode.**

The new parts in step #2 and #4 make it so that we re-hydrate compute objects
(and later on also sources) before we cut over to the new version. Compute
objects are immediately fresh when we cut over.

### Read-only mode

These parts need to learn to start in read-only mode:

**Compute Controller (including the things it controls: clusters/compute
objects):**

I (aljoscha) don't think it's too hard. We need to thread a read-only mode
through the controllers to the cluster. The only thing doing writes in compute
is `persist_sink`, and that's also not too hard to wire up.

**Storage Controller (including the things it controls: sources, sinks, writing
to collections, etc.):**

"Real" read-only mode for sources is hard but I (aljoscha) think we can start
with a pseudo read-only mode for milestone 1: while the storage controller is
in read-only mode it simply doesn't send any commands to the cluster and
doesn't attempt any writes. This has the effect that we don't re-hydrate
sources while in read-only mode, but that is okay for milestone 1. Later on, we
can teach STORAGE to have a real read-only mode and do proper re-hydration.

**StorageCollections:**

This is a recently added component that encapsulates persist `SinceHandles` for
collections and hands out read holds. The fact that this needs a read-only mode
is perhaps surprising, but: each collection has one logical `SinceHandle` and
when a new `environmentd` takes over it also takes over control of the since
handles and downgrades them from then on. This is a destructive/write
operation. While a compute controller (and it's clusters) are re-hydrating,
they need read holds.

I (aljoscha) believe that a StorageCollections that is starting in read-only
mode should acquire leased read handles for collections and use those to back
read holds it hands out to the compute controller et al. Only when it is told
to go out of read-only mode will it take over the actual SinceHandles.
Relatedly, a StorageCollections in read-only mode is not allowed to do any
changes to durable environment state.

Here's a sketch of the reasoning for a) the correctness, and b) the efficacy of
this approach.

Correctness: meaning we don't validate the somewhat nebulous correctness
guarantees of materialize. Roughly, that we uphold strict serializability,
don't lose data, don't get into a state where components can't successfully
restart.

- Even ignoring 0dt upgrades, it is the case today that we can always recover
  from both `clusterd` and `environmentd` processes restarting.
- The critical since handles are load bearing. A restarting `environmentd` will
  re-acquire them and then decide as_ofs for rendering dataflows based on the
  sinces that the critical handles lock in place. Therefore, leased read
  handles are never required for correctness.
- A read-only `environmentd` will not touch the critical since handles.
- A new `environmentd` deployment restarting in read-write mode will acquire
  the critical since handles, same as on a regular failure/restart cycle. It
  will then determine as_ofs for dataflows based on them and send them out
  again to already running clusters.

Efficacy: meaning in read-only mode the leased read handles do _something_ to
help hydrate dataflows/clusters, without interacting with the critical since
handles.

- The leased read handles provide a window of opportunity for dataflows (in the
  `clusterd` process) to install persist sources, as long as they're not
  expired. As seen in ci-failues, when we activate the
  `persist_use_critical_since_*` LD flags, the leased read handles holding back
  the shard frontier are "load bearing" for this use case.
- The goal of pre-hydration, in read-only mode, during 0dt upgrades is to
  prepare `clusterd` processes for when a restarted `environmentd`, in
  read-write mode, with critical since handles in place tries to reconcile with
  them. Ideally, none of the already-running dataflows have to be restarted due
  to incompatibilities. Dataflows would be incompatible if their as_of were
  *later* than what the new `environmentd` process requires.
- Pre-hydration is beneficial as long as we don't allow compaction (of the new
  read-only dataflows) beyond the since of the critical handle (which will be
  used as the basis when bootstrapping state on restart). We currently don't
  enforce this, but this pans out in practice because we downgrade read holds
  (which hold back the since of dataflows and ultimately persist handles) based
  on the upper of collections. And the read-only deployment is *not* actively
  advancing those uppers. It's the old deployment that is advancing the uppers
  and downgrading its read holds based on the same policies.
- The worst thing that can happen is that we have to restart a dataflow if we
  happened to have allowed its since to advance too far.

### Transitioning out of read-only mode

There are at least two ways for transitioning components out of read-only mode:

1. The component knows how to gracefully transition out of read-only mode and
   stay running. It will do so when told.
2. In order to transition a component, we shut it down and then start a new
   instance of it in read-write mode. You could call this the crash-and-restart
   approach. The shutdown-and-restart could happen at various levels: it could
   be an in-process component that can be shut down. In the extreme, it means
   killing a process and restarting.

At least for clusters, we need approach #1 because the whole idea is that they
re-hydrate and then stay running when we cut over to them and transition out of
read-only mode.

For the other components, both approaches look feasible and it his hard to tell
which one is easier/quicker to achieve for milestone 1.

I (aljoscha), think that in the long run we need to do approach #1 (graceful
transition) for all components because it will always be faster and lead to
less downtime.

If we want to gracefully transition the adapter/coordinator out of read-only
mode, we need the work around Catalog follower/subscribers and then teach the
Coordinator to juggle migrated in-memory state while it awaits taking over.
With the crash-and-burn approach, the Coordinator could get a recent snapshot
of the catalog, migrate it, and work off of that. It doesn't need to juggle
migrated state and be ready to eventually take over.

### Self-correcting storage-managed collections

This section is a bit in-the-weeds!

The way most storage-managed collections work today is that we "reset" them to
empty on startup and then assume that we are the only writer and panic when an
append fails.

This approach will not be feasible for long. At least when we want to do the
graceful transitioning approach we need a way for the storage controller to
buffer writes to storage-managed collections and reconcile the desired state
with the actual state. This is very similar to the self-correcting
`persist_sink`.

There are some storage-managed collections for which this approach doesn't
work, because they are more append-only in nature. We have to, at the very
least, audit all storage-managed collections and see how they would work in a
zero-downtime/read-only/transition world.

## Sequencing, Subtasks & Estimation

These can be done in any order:

- Read-only mode for compute controller/clusters
 - Estimation: tbd!
- Pseudo read-only mode for storage controller/clusters
 - Estimation: tbd!
- Read-only mode for StorageCollections
 - Estimation: tbd!
- Self-correcting storage-managed collections / Audit storage-managed
  collections
 - Estimation: tbd!
- (depends) Multi-subscriber catalog / In-memory Juggling of Migrations
 - Estimation: tbd!

After the above are done:

- Upgrade Orchestration
 - Start components in read-only mode
 - Await re-hydration
 - Transition out of read-only mode (either gracefully or using crash-and-burn
   at some layer)
 - Cut over
 - Estimation: tbd!
- Cloud Orchestration
 - Cloud likely wants to do things around monitoring an upgrade. Checking
   timeouts, escalating to human operators when things go wrong. Estimation:
   tbd!

## Future Milestones

**Milestone 2**:

- freshness for sources

**Milestone 3**:

- more release verification before cutting over

**Future Milestones**:

- a consistent cut-over timestamp across all objects
- smart (idk 🤷) handling of behavior changes: changes to query plans, changes
  to builtin functions

I (aljoscha) am not sure that we _ever_ need a consistent cut-over point or
smart handling of behaviour changes. It's very much a product question.

## Comments on roadmap for other teams

If we want to quickly deliver milestone 2 (freshness for sources), the storage
team should get started on thinking about read-only mode for sources and how we
can transition them during upgrades rather sooner than later.

## Alternatives

If we want to quickly deliver milestone 2 (freshness for sources), the storage
team should get started on thinking about read-only mode for sources and how we
can transition them during upgrades rather sooner than later.

## Open questions

Graceful transition out of read-only mode or crash-and-burn at some level?
