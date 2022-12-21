# Active Replication

The COMPUTE layer employs active replication within clusters, henceforth called instances. The primary goal of active replication is to mask extended recovery times due to dataflow rehydration upon compute crashes. This goal can be achieved by adding enough replicas depending on failure probability and criticality. For example, a customer that can tolerate crash recovery by rehydration might run an instance with a single replica or even for periods of time, have no replica at all in an instance. A customer that wishes to mask compute crashes might run an instance with two or even three replicas.

Three secondary goals of active replication are: (a) to support the ability to upsize or downsize an instance dynamically, (b) to mask networking issues that either partition away or slow down access to a strict subset of an instance's replicas, and (c) to mask processing issues that increase response latency by again a strict subset of an instance's replicas. For secondary goal (a), upsizing can be achieved, e.g., by adding a larger replica and removing a smaller one, while downsizing, e.g., by adding a smaller replica and removing a larger one. This capability allows customers to adjust their compute costs to their needs over time. For secondary goals (b) and (c), since compute commands are processed deterministically by all replicas, it is only necessary to receive responses from a single replica to make progress. So customers can leverage replication to privilege latency in the presence of certain classes of [gray failures](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/06/paper-1.pdf).

High availability is not in scope of the mechanism, so single points of failure may exist.

## Overview of the Mechanism

The active replication mechanism works at a high level as follows. The compute controller maintains a history of compute commands per instance, which it continuously applies to every replica of the instance. Since the replica state resulting from fully applying a command history is deterministic, each replica will arrive at the same state assuming all commands in the compute controller history for that instance are executed in order. A replica is free to maintain a copy of the history or not, depending on its implementation.

As described in the [Compute Command Model](compute-command-model.md), a history can be reduced to an equivalent history. Since applying either a history or its reduction will bring a replica to the same state, the compute controller can apply history reduction to bound the growth of its compute command history. History reduction can also be of use to facilitate dynamic changes in the set of replicas, since it is expected that applying a shorter, reduced history will more efficiently bring a new replica up to speed than applying an unreduced original history.

The existence of the history in the compute controller is independent from the existence of replicas. Since the mechanism allows for dynamic changes in the set of replicas, it is valid to drop all replicas. When a new replica is added from such an empty set, the compute controller must know how to bring the replica to the most recent state. Additionally, the compute controller must retain its knowledge of existing compute collections, given that their lifetime is controlled by the ADAPTER layer. For example, an index created by the user cannot cease to logically exist simply because all replicas in the COMPUTE layer were temporarily dropped.

### Interaction with the ADAPTER and STORAGE Layers

A critically important point for the mechanism to work is that data dependencies for each command, e.g., availability of storage collections at particular logical times, must be satisfied whenever a replica needs to execute a command. So the compute controller needs to interface with the ADAPTER and STORAGE layers to receive and provide information regarding [read (since) and write (upper) frontiers](read-write-frontier-management.md).

Given the reliance on other components for persistent state, the mechanism is currently not coupled with a checkpointing facility. Checkpointing by itself does not address the [cost of downtime](https://www.usenix.org/legacy/events/lisa2002/tech/full_papers/patterson/patterson.pdf), which tends to be significant in applications relying on low-latency processing of data streams. When additionally considering the concerns of economy of mechanism for the [intended goals](active-replication.md#active-replication) and manageable complexity, active replication without checkpointing becomes an attractive choice.

### History Reduction

Over time, the compute command history of an instance can grow large. To avoid unbounded growth, the history is reduced, following a length-doubling policy, to an equivalent, hopefully shorter history. The defining property of a reduced history is that its full application by a new replica would bring the replica to the same state as other replicas that have fully applied the unreduced version of the instance's history. More details about history equivalence and reduction are discussed in the [Compute Command Model](compute-command-model.md).

### Failure Detection and Rehydration

Since durability is provided by the ADAPTER and STORAGE layers, the mechanism is intended to tolerate crashes of any number of replicas in an instance. However, masking of recovery delay can only be guaranteed when the compute controller can reach at least one non-faulty replica.

The compute controller assumes detection of crashes by breakdown of the network communication with a replica, namely when sending commands or receiving responses from the replica. Upon such an assumed detection of a crash, the compute controller adds the replica to the instance's set of presumably failed replicas. The latter makes the replica a target for rehydration, i.e., the process of removing the old replica metadata, re-adding the replica, and sending to the replica the entire reduced history of the instance for replay. Note that when re-adding the replica here, an attempt is made to find an existing incarnation of the replica's service, in which case the [Reconciliation](reconciliation.md) optimization can be triggered; if such an incarnation cannot be found, a new replica service is spawned.

Note that a failure of the compute controller will result in unavailability and — at present — loss of the authoritative compute command history. However, the ADAPTER layer can execute a recovery process from the information recorded in the catalog that will endow the compute controller with a history that is equivalent to the history prior to the crash. Additionally, all replicas will be re-added to the compute controller and undergo rehydration.

## Core Components in Source Code

### In [`src/compute-client/src`](/src/compute-client/src/)

- [`controller.rs`](/src/compute-client/src/controller.rs): Management of addition / removal of instances as well as core interaction with the ADAPTER layer through the `process` function. Otherwise, all command handling is forwarded to `Instance`s.
- [`controller/instance.rs`](/src/compute-client/src/controller/instance.rs): Here you find the heart of the replication scheme. The code is very delicate to edit due to the heavy use of asynchrony in `ActiveInstance`; however, a style is used in which nested asynchrony is avoided and each function gets a chance to make a consistent change to the instance's state. Two important data structures are the `Instance`'s `collections` metadata in the form of a map of `CollectionState` and the `Instance`'s compute command `history`. Most API calls will trigger execution of some of the functions `update_write_frontiers`, `remove_write_frontiers`, and `update_read_capabilities` to transition the state of the read and write frontiers kept per collection and replica in response to external triggering of `ActiveInstance` operations. More details about read and write frontiers are described in [Read and Write Frontier Management for Compute Collections](read-write-frontier-management.md).
- [`controller/replica.rs`](/src/compute-client/src/controller/replica.rs): Lower-level communication code to send commands to and receive responses from replicas.

### In [`src/compute/src`](/src/compute/src/)

- [`server.rs`](/src/compute/src/server.rs): Main command processing loop and reconciliation code.
- [`compute_state.rs`](/src/compute/src/compute_state.rs): Handling of commands by a single replica along with management of compute command history for that replica.
