# Refactor storage controller state cleanup

- Associated:
    - [Dropping a persisted table or source does not free the persisted data
      #8185](https://github.com/MaterializeInc/materialize/issues/8185)

## Context & Scope

The "storage" component of Materialize currently has an architecture with a number of clients
mediating interactions between the user's request and the workers actually performing the work. A
sketch of  the clients are:

1. Coordinator
1. Storage controller
1. RehydratingStorageClient
1. PartitionedClient/StorageGrpcClient
1. Workers

Each of the clients receives commands from the level immediately above it (we'll call this the
Upper-Level Client or ULC), and responses from the level immediately below it (the Lower-Level
Client, a.k.a. LLC). (The coordinator receives commands from users, who answer to their own higher
power, and the worker receives responses from its threads of execution).

The _protocol_ that connects these clients is "call and response": ULCs heavily rely on responses
from LLCs before doing work--this dependence is so intense that, in some cases, the ULC may never
perform work without receiving a response from the LLC.

## Goals

- Reconsider the storage clients' protocol to "embrace the distributed nature" of the system and
  relax the dependence on receiving responses from lower levels.
- Allow the self-healing nature of the architecture to play a more active role in the protocol, i.e.
  ensure we converge to a healthy state, rather than avoiding "unhealthy" states at all costs.

Maybe these are the same goal.

## Non-Goals

- Revisit the architecture of the system.
- Avoid panics.

## Overview

Each layer should be able to act independently of its LLC, and should instead try to take actions
based on globally available state (e.g. through persist), rather than waiting to be informed of the
LLC's state.

For example, when dropping a collection from the storage controller, we can immediately enqueue that
this is a collection whose shards we want to drop/state we want to clean up. We can then monitor the
global state of the shard (its since and upper) to detect when all running dataflows have ceased
using this shard--i.e. we move toward relying on persist for information (which we believe to be
reliable), instead of simply relying on lower-level clients (which might be arbitrarily slow or fail
to respond entirely).

## Detailed description

We should undergo a process of analyzing the "protocol flow" for all
`StorageCommand`/`StorageResponse` sets, such that:

- We emphasize initiating action based off of `StorageCommand`s (or the equivalent API call in the
  storage controller), under the assumption that ULC are more responsive than LLC.
- When making decisions about which actions to perform, we aim to use consistent global state
  whenever possible, and rely on LLC responses only when the information is available in no other
  way.
- We err on the side of expedience and allow lower levels of the system to panic--as long as we take
  measures to ensure that the system will automatically correct itself eventually (i.e. not enter a
  crash loop).

### Example

In response to dropping a collection:
- The storage controller can immediately release all read holds for the collection, mark its shards
  for finalization, and remove the associated GlobalIds from its persisted collections.

  From here, the storage controller can monitor the retired shards' global state through persist and
  seal the shards from further reads and writes once it detects that there are no further read holds
  against the shards.

- The layers between the storage controller remove their local state for a source in response to an
  `AllowCompaction` command to the empty antichain. There is no global state available to make a
  better decision from and the responses from the LLC are unnecessary.

  Note that if there were some worry here, we could also reconfigure this to have the storage
  controller send along a `StorageCommand:DropIds`.

- Workers now just cleans up its local state, but doesn't bother sending a `DroppedIds` response
  back to its ULC, which cannot do anything with the message.

This design means we can refactor away the `DroppedIds` responses.

This design is akin to how compute manages a similar problem, [according to
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

#### This can create cause more panics

For example, created and immediately dropping resources can panic or halt: when the dataflow
attempts to use its shard, which we've already closed.

We contend that this is actually fine. Race conditions are acceptable as long as the system
naturally converges to a steady state; in this case, the dataflow will eventually find that its
write handle's upper is empty and the dataflow itself will not be scheduled.

#### What if we wait forever on global state?

If we want to make a decision based on global state (e.g. persist since handles), we need to ensure
they are elements of the system that are also built using distributed principles. In the case of
sealing shards, we know that dataflows only have leased read handles: if a dataflow gets partitioned
away from the rest of the cluster, its read handle will time out, and we will see the since advance
to `[]`.

If we get partitioned away from persist, a new environmentd will take over, and if we ever
re-connect, we'll receive an epoch error.

#### We lose fidelity in making assertions about our protocol

Part of the reason we originally designed this subsystem to only cleanup state as late as possible
is to ensure that responses can be arbitrarily delayed and we can still make assertions about their
intermediate states.

If we eagerly remove state from the storage controller, we lose the ability to ensure that the lower
portions of the system (e.g. the workers) are behaving in a way that aligns with our expectations.

However, this design fails to acknowledge the reality of our protocol: we're already failing the
protcol because we seldom yield `DroppedIds` responses. Guaranteeing delivery of these messages from
workers in the process of spinning down is non-trivial.

However, if there are more ardent concerns, we could mitigate these by having a "limbo state," where
we clean up state internally, but place the state we've cleaned up in "limbo" where it awaits the
LLC response.

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
for handling dropping shards that instead of using `StorageResponse::DroppedIds` introduces
`StorageResponse::FrontierSinces` that is the parallel response to
`StorageCommand::AllowCompaction`.

However, the implementation for that suffers from the same faults as the current system in that, in
my read of the proposal, does not address the shortcoming that we never receive
`StorageResponse::DroppedIds`.

## Open questions

At this time there are no open questions, though we can amend that in the discussion.
