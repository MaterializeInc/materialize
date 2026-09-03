# Decoupled coordination

## Outcome and scope

The catalog is the authority for maintained storage and compute lifecycle.
Cluster-side components subscribe to catalog changes and enact the desired
state, rather than depending on an adapter to send lifecycle commands. Adapters
write catalog changes and use a separate fast protocol for query execution.

The goal is decoupling, with existing SQL behavior, consistency guarantees, and
performance preserved. Losing an adapter must not disrupt maintained dataflows
or other clients. Its own queries may fail. Transparent query or session
failover is out of scope.

Working multi-adapter operation is the destination, not this deliverable.
Boundaries must support independent adapters becoming catalog writers and query
clients without another ownership redesign. Enabling concurrent catalog writers
and deploying multiple adapters are not required here.

Query-local dataflows that do not go through the catalog, including slow-path
SELECTs, SUBSCRIBEs, and COPY TO, remain on the fast protocol. Their creation,
execution, responses, and cleanup are part of request-scoped execution, not
durable catalog lifecycle.

## Approach

Use catalog implications to derive lifecycle effects from committed catalog
state. Complete the relevant implication paths as needed for this work. Move
responsibility for enacting those effects to cluster-side components that can
recover and follow catalog state independently of the originating adapter.
Controllers can remain useful abstractions. Their exact placement and factoring
are implementation choices.

Catalog authority covers creation, changes, compaction permission, and deletion
of maintained objects. It describes desired state, not a command history that
requires the initiating adapter to replay it. A recovering subscriber must be
able to establish the required state and continue following changes.

The fast protocol supports independent query clients without giving a connecting
client ownership of the cluster's maintained lifecycle. Responses, cancellation,
and disconnect cleanup belong to the relevant client. This is a semantic split,
not simply another listener for the same controller protocol.

## Boundary contracts

### Readability and compaction

A reader must secure protection before relying on a timestamp. Observing a
frontier in a catalog snapshot is not itself a read hold. While protection is
valid, compute and persist compaction must respect it, including through
dependencies, installation, and ownership handover. One client cannot release
another's protection.

Catalog state carries the authority for advancing readability frontiers. The
representation and accounting of individual read requirements remain open.
An evolving per-object frontier is a candidate, not a prescribed schema. Keep
its meaning distinct from a dataflow's installation `as_of` and an MV's initial
storage visibility boundary unless the implementation establishes how they fit.

Propagation to persist critical since handles must respect all valid read
requirements. Those handles are the durable backstop, not a substitute for
multi-client accounting. Stale owners must not advance compaction or destroy
data based on incomplete local knowledge. Abandoned client holds must be
reclaimable without allowing that client to resume using invalid protection.

Avoid coupling ordinary query throughput to catalog writes for every hold
change. Coalescing or rate-limiting frontier advancement may retain extra
history, but must not delay protection until after it is needed. Choose cadence
and any configuration from measured catalog load and retention cost.

### Visibility and execution

Catalog commit, cluster application, and query readiness are different events.
Queries must not fail spuriously or execute against the wrong object state
because the fast protocol overtakes catalog application. Preserve existing
behavior for concurrent DDL, transactions, cancellation, and object drops.

Query-client connections must not replace one another's desired state or reset
maintained dataflows. Lifecycle ownership and permission to perform external
writes must remain safe across restarts and handover, independently of query
connection lifetime.

## Implementation and verification

Implementers choose the smallest coherent mechanism and the order of work.
This document does not prescribe catalog fields, leases, protocol messages,
planning placement, or process topology. An incremental implementation is fine,
but an intermediate step is not completion of the outcome above.

Flag implementation discoveries, tradeoffs, and scope growth to Aljoscha. Pause
affected work when guidance is needed, especially when correctness, user-visible
behavior, or an agreed boundary would change. Do not silently narrow capability
or expand the design to resolve a difficulty. Seek independent review where
the risk warrants it.

Existing CI, nightly, and performance suites are the broad acceptance signal.
Ensure coverage actually exercises the changed boundaries, particularly
independent query clients, catalog/query ordering, and read protection during
compaction and recovery. Add targeted tests or experiments where existing
coverage leaves uncertainty. Measure catalog traffic and retained-history cost
as well as query performance. Record what ran, failures, and remaining gaps.

### Starting points

- [Catalog implications](../../../src/adapter/src/coord/catalog_implications.rs)
  derive effects from committed changes. Index and MV creation also have
  sequencer-side installation paths to account for.
- [Compute protocol](../../../src/compute-client/src/protocol/command.rs) mixes
  lifecycle and query commands. [Transport](../../../src/service/src/transport.rs)
  replaces the active client on a new connection.
- [StorageCollections](../../../src/storage-client/src/storage_collections.rs)
  owns storage capability accounting and critical since handles. The fixed
  critical-reader identity and epoch fencing support handover, not independent
  owners aggregating their local holds.

## Implementation log (append-only)

Append dated entries with findings and evidence, decisions or pending questions,
validation results, and the next useful step. Clearly distinguish an
implementer's proposal from a decision reviewed with Aljoscha. Do not rewrite
earlier entries. Keep the main text concise and update its agreed boundaries
only following review.

### 2026-09-03: Scope agreed with Aljoscha

Multi-adapter operation is the end goal, not the immediate deliverable.
Query-local dataflows remain on the fast protocol. Adapter loss must not harm
other clients or maintained dataflows, but transparent failover is not required.
Completing relevant catalog implication paths is within scope. Implementation
has not started and runtime validation has not been performed.

### 2026-09-03: Sink creation from committed implications

Sink additions now install exports through catalog implications. Independent
review found no blocking issue. [CI build 133862](https://buildkite.com/materialize/test/builds/133862)
passed both Clippy jobs and formatting before a documentation push superseded it.
Latest CI and runtime validation remain pending. Local builds were blocked by
missing artifacts and tools. Aljoscha confirmed CI as the default test loop.

Controllers still live in the adapter. Direct committed-update tests and
same-batch MV/sink creation remain gaps. Next proposed slice: reuse index plan
reconstruction from bootstrap, preserving precommit optimization as a cache.
Index parsing leaves plans absent and the expression cache has no runtime read
API. No new boundary decision was made.
