# Refactor storage controller state cleanup

- Associated:
    - [Dropping a persisted table or source does not free the persisted data
      #8185](https://github.com/MaterializeInc/materialize/issues/8185)

## Context & Scope

On `main`, the storage controller does not clean up state related to sources or sinks until we
receive a `StorageResponse::DroppedIds` from `clusterd`/rehydration clients. This cleanup work
includes:

- Tombstone-ing the sources' shards (advancing the `since` and `upper` to `[]`)
- Removing the in-memory representation of a collections
- Removing the in-memory read and write handles to sources' shards
- Removing the collection from the on-disk stash (`COLLECTION_METADATA`)

However, we make no guarantee that we will ever receive a `DroppedIds` response and, in practice,
rarely receive them. This means we rarely succeed in completing the cleanup steps for a collection,
and as such:

- We never drop the state persisted for the collection in S3.
- We allow reference to collections that we know have been dropped by the coordinator. This has
  odd/worrying semantics, though is more abstract.

We have mitigated the effects of this to some degree by reconciling state after the coordinator is
done booting from its catalog. However, the way that reconciliation works, we don't perform the work
of actually tombstone-ing a shard until we receive some `DroppedIds` command.

Without an effective or repeatable means of cleaning up persist state, we will retain all data in
the persist shard indefinitely, incurring the cost of the S3 buckets indefinitely.

## Goals

- Deterministically free data from S3 once a collection is dropped.
- Make the semantics of dropping sources and sinks less byzantine.

## Non-Goals

- Retain the exact semantics we currently have in the face of protocol violations.

## Overview

Rather than cleaning up state in response to `DroppedIds` responses, we should proactively clean up
state when processing `drop_sources` and `drop_sinks` commands from the adapter.

This weakens our ability to make assertions about our protocol, but one could argue that we are
already failing our own protocol in our lack of ability to reliably generate `DroppedIds` responses.

## Detailed description

Dropping a source or a sink proceeds through some byzantine machinery that results in:

- The `since` of the collection getting advanced to `[]`.
- If the collection has no read holds on it or when the read holds released (i.e. the controller
  believes the since to be `[]`), a tuple of `(id, [])` gets placed in
  `pending_compaction_commands`.

This proposal is that:

- When we see values in `pending_compaction_commands` with empty frontiers that we perform the
  cleanup of the ID's state, i.e. the code that currently runs in response to `DroppedIds`, which
  we've outlined at the beginning of this document.
- We, in essence, remove `StorageResponse::DroppedIds`. There is more discussion of how to handle
  this below.

This proactive approach guarantees that we will tombstone persist shards effectively and
deterministically.

Of note, this design is akin to how compute manages a similar problem, [according to
Jan](https://materializeinc.slack.com/archives/C01CFKM1QRF/p1678785227462869?thread_ts=1678708964.047089&cid=C01CFKM1QRF):

> When a replica is removed, the compute controller [immediately releases the read
> holds](https://github.com/MaterializeInc/materialize/blob/5c05255bafcc565074eaf73f70aff18644ecf8da/src/compute-client/src/controller/instance.rs#L453-L454)
> installed for it. This means when dropping a replica, we don’t need to rely on the replica being
> functional/cooperative to successfully remove its state. It also is the only case I am aware of
> where “cannot serve requested as_of” panics can still happen. They should be rare because once the
> replica has initialized a persist_source for a collection, it has a read handle to hold back
> compaction, so panics can only happen if the controller drops a replica shortly after it
> instructed it to create a new dataflow. Also, if we see such a panic in Sentry (I don’t think we
> have yet) I’d hope we would also see that the replica was dropped in the breadcrumbs.

The parallel being we are also essentially releasing the controller's read hold on the shard, and if
it had no other read holds from the controller's POV, we remove its state.

### Concerns + mitigation

#### Created and immediately dropped resources will panic or halt

If we create and immediately drop a source, it is possible that the source will panic or halt when
it attempts to use its shard, which we've already closed.

One contention is that this is fine; periodic panics from resources in race conditions are
acceptable as long as the system naturally converges to a steady state.

#### We lose fidelity in making assertions about our protocol

Part of the reason we originally designed this subsystem to only cleanup state as late as possible
is to ensure that responses can be arbitrarily delayed and we can still make assertions about their
intermediate states.

If we eagerly remove state from the storage controller, we lose the ability to ensure that the lower
portions of the system (e.g. the workers) are behaving in a way that aligns with our expectations.

One contention is that we are already failing our own protocol by failing to yield `DroppedIds`
responses. Guaranteeing delivery of these messages from workers in the process of spinning down is
non-trivial.

However, if there are more ardent concerns, we could also continue making the same assertions by
providing "bizarro versions of this state" that are used solely for assertions, i.e. we remove the
collection information from code paths accessible to user control flows, and into another parallel
struct that exists solely to let us ensure the correctness of the protocol's behavior while still
letting us eagerly clean up the state we want to.

This approach seems brittle and convoluted, but if there are a small number of call sites that we
want to maintain this for, it might be tractable.

**Also** these assertions are not well-principled in the face of arbitrary delays and failures,
which is more or less the current problem. If we wait on the response from all replicas before
decommissioning resources, how do we want to handle arbitrarily delayed replicas? Should the storage
controller be in the business of explicitly killing slow replicas after a timeout if we haven't
heard back from them?

## Alternatives

[Petros had an alternative
design](https://materializeinc.slack.com/archives/C01CFKM1QRF/p1678753425337379?thread_ts=1678708964.047089&cid=C01CFKM1QRF)
that instead of using `StorageResponse::DroppedIds` introduces `StorageResponse::FrontierSinces`
that is the parallel response to `StorageCommand::AllowCompaction`.

However, the implementation for that suffers from the same faults as the current system in that, in
my read of the proposal, does not address the shortcoming that we never receive
`StorageResponse::DroppedIds`.

## Open questions

At this time there are no open questions, though we can amend that in the discussion.
