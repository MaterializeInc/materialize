# Materialize Platform: Overview

The intended experience with Materialize Platform is that many independent users can establish changing data sets, create and interrogate views over these data, and share the data, views, and results with each other in a consistent fashion.
The intent is that in each dimension we remove limits to scaling:
* We aim to support unbounded numbers of concurrent users and sessions
* We aim to support unbounded numbers of data sources, and with unbounded rates
* We aim to support unbounded numbers of views over these data

Here "unbounded" does not mean "infinite", only "can be increased by spending more resources".

The above are goals, and the path to Materialize Platform starts with none being the case yet.

The approach to remove scaling limits is "decoupling".
Specifically, we want to remove as much artificial coupling as possible from the design of Materialize Platform.
We will discuss the decoupling of both
- the "logical architecture" (which modules are responsible for what) and
- the "physical architecture" (where and how does code run to implement the above).

What follows is the initial skeleton of work that needs to be done in support of this decoupling.

## Common context

Decoupling is enabled primarily through the idea of a "definite collection".
A definite collection is a durable, explicitly timestamped changelog of a data collection.
Any two independent users of a definite collection can be sure they are seeing the same contents at any specific timestamp.

Definite collections, and explicit timestamping generally, allow us to remove many instances of explicitly sequenced behavior.
If reads and writes (and other potential actions) have explicit timestamps, these actions do not require sequencing to ensure their correct behavior.
The removal of explicit sequencing allows us to decouple the physical behaviors of various components.

This concept is analogous to "multiversioning" in traditional databases, in which multiple versions of rows are maintained concurrently.
Multiversioning similarly decouples data updates and query execution in its setting.
We are doing the same thing, at a substantially larger scale than is traditional.

## User experience design

The intended user experience drives some of the concepts that we'll want to reflect in our code.
The intended user experience in turn reflects the practical realities of the tools we have access to.

The coarsest level of granularity of Materialize Platform is the ACCOUNT.
Within an ACCOUNT there may be multiple ENVIRONMENTs.
Each ENVIRONMENT is bound to a region of a specific cloud provider, e.g. AWS us-east-1.
An ENVIRONMENT is the unit within which it is reasonable to transfer data, and across which that expectation doesn't exist.

Within each ENVIRONMENT there may be multiple CLUSTERs, which correspond roughly to timely dataflow instances.
A CLUSTER contains indexes and is the unit within which it is possible to share indexes.

Within each ENVIRONMENT there may be multiple TIMELINEs, which correspond roughly to coordinator timelines.
A TIMELINE contains collections, and is the unit within which it is possible to query across multiple collections

The main restrictions we plan to impose on users is that their access to data is always associated with a ENVIRONMENT, CLUSTER, and TIMELINE, and attempts to access data across these may result in poorer performance, or explicit query rejection.

There is some work to do to present these concepts to users, but prior work exists with e.g. SQL `database`s for TIMELINE and Snowflake's `USE WAREHOUSE` command to pilot CLUSTER.

## Logical architecture design

Materialize Platform is broken into three logical components.

* STORAGE records and when asked produces definite collections.
* COMPUTE executes and maintains views over definite collections.
* CONTROL interprets user commands and instructs the STORAGE and COMPUTE layers.

The partitioning into logical components assists us in designing their implementations, as they clarify who are responsible for which properties, and which components can be developed independently.
It is meant to provide more autonomy, agency, and responsibility.

One part of the design is that these components are actually *layers*.
* STORAGE is the lowest layer, and makes no assumptions about the other layers (e.g. determinism, correctness).
* COMPUTE is the next layer, and relies on STORAGE but makes no assumptions about CONTROL.
* CONTROL relies on the lower layers.

This moves the onus of some behaviors on to higher layers. STORAGE is not expected to understand CONTROL, and so must be explicitly instructed if it has goals. STORAGE should not believe that COMPUTE is deterministic, and should treat its output with suspicion.

### STORAGE

The STORAGE layer is tasked with creating and maintaining definite collections.

It relies on no other layers of the stack, and has great lattitude in defining what it requires of others.

Its primary requirements include
1. define and respond to `CreateSource` commands (likely from CONTROL) with the identifier of definite collections.
2. define and respond to `Subscribe` commands (CONTROL or COMPUTE) with a snapshot and stream of updates for a specified collection.
3. define and respond to `WriteBack` commands (CONTROL or COMPUTE) by recording updates to a specified collection.

The layer gets to determine how it exposes these commands and what information must be provided with each of them.
For example, STORAGE may reasonably conclude that it cannot rely on determinism of CONTROL or COMPUTE, and therefore require `Subscribe` commands must come with a "rendition" identifier, where STORAGE is then allowed to reconcile the renditions of a collection (perhaps taking the most recent information).

There are any number of secondary requirements and additional commands that can be exposed (for example, to drop sources, manage timeouts of subscriptions, advance compaction of collections, set and modify rendition reconciliation policies, etc).

STORAGE can be sharded into ENVIRONMENTs.

### COMPUTE

The COMPUTE layer is tasked with creating and maintaining views over definite collections.

It relies on STORAGE to provide definite collections (sources) and receive updates to be written back (sinks).

Its primary requirements include (all from CONTROL)
1. define and respond to commands from CONTROL to spin up or shut down a CLUSTER (the atom of COMPUTE).
2. define and respond to commands from CONTROL to install and modify maintained views over data.
3. define and respond to commands from CONTROL to inspect the contents of maintained data (peeks).
4. define and respond to commands from CONTROL to inspect the metadata of maintained data (frontiers).

Each CLUSTER is by design stateless, and should be explicitly instructed in what is required of it.

COMPUTE can be sharded into CLUSTERs.
Each CLUSTER is bound to one ENVIRONMENT.
Views installed in one CLUSTER can be used in that same CLUSTER, but cannot be used by others without a round-trip through STORAGE.

### CONTROL

The CONTROL layer translates user input into commands for the STORAGE and COMPUTE layers.

It relies on STORAGE to make sources definite, and on COMPUTE to compute and maintain views as specified.

CONTROL has no requirements imposed on it by other layers, and has great lattitude in defining what it asks others to do.

Although at the moment CONTROL is "SQL", there is no reason this layer needs to provide exactly this interface.
It could also provide more direct access to STORAGE and COMPUTE, for other frameworks, applications, languages.

CONTROL is where Materialize Platform provides the experience of consistency.
Users that write to STORAGE and then view COMPUTE may expect to see results reflecting their writes.
Users that view COMPUTE then tell their coworker may expect them to see compatible results.

CONTROL can be sharded into TIMELINEs.
Users on the same TIMELINE can be provided with consistency guarantees, whereas users on independently timelines cannot.

## Physical architecture design

The physical architecture tracks the logical architecture somewhat, in that each layer is intended to manage its own resources.
* The STORAGE layer is expected to spin up threads, processes, containers for each maintained collection.
* The COMPUTE layer is expected to spin up threads, processes, containers for each indepedent cluster.
* The CONTROL layer is expected to spin up threads, processes, containers for each user session, timeline, frontend, etc.

Each of these layers needs to orchestrate its resources: spinning up, instructing, spinning down, etc.
However, this orchestration is not required to by physically isolated.
Until scaling requires, we can imagine each of these orchestrators co-located in the same process.

### Roadmap to Platform

Our current codebase has a monolithic implementation of STORAGE, COMPUTE, and CONTROL in the form of `materialized`.

However, we already have hints of scalable architecture:
* The `dataflow` module spins up new timely dataflow worker threads,
* The `persist` module farms work out to independent worker threads.

We are not yet in a position where we can go much further than this in these modules.

#### Step 1: Abstraction

Without modifying the behavior of Materialize Binary, introduce abstraction boundaries for STORAGE and COMPUTE.
This is presently `dataflow::{ Command, Response }`, which contains variants that instruct both STORAGE and COMPUTE.

We partition the above to be the commands for STORAGE and COMPUTE separately.
We introduce a boundary between STORAGE and COMPUTE (roughly `create_source` and `create_sink`).
We reorganize modules to follow these boundaries (e.g. sources and sinks into a `storage` crate).

The partitioned commands move at least source creation and table manipulation to STORAGE from `dataflow::Command`.
This requires us to mock up a STORAGE layer that initially could be as simple as
* a `HashMap` from identifiers to source descriptions, which it then uses to respond to source subscriptions for `dataflow` by running the existing `create_source` logic.
* a `Vec<Update>` for table updates, from which new source instances can read. (the `persist` team likely has something more sophisticated already, which could be used instead, but I wanted to scribble down what was the simplest thing).

A goal of mocking up the abstraction is determining which concepts need to be expressed across the boundary.
For example, we currently conflate an input table with its index, and rely on the index to retain updates for other uses.
This seems wrong, but does mean that if we do not use indexes as the mechanism, we need to determine the vocabulary to communicate (e.g. `since` and `upper`, compaction, etc).

#### Step 2: Layer-local work

Should we reach a point where we like the boundaries, each layer can iteratively improve its implementation.
For example:
* STORAGE can investigate pivoting off of timely dataflow where appropriate (moving logic out of the "fat client").
* COMPUTE can investigate spinning up independent clusters, vs the single cluster it currently uses.
* CONTROL can continue to divest itself of concepts that other layers should be managing. Perhaps spin up workers for e.g. optimization.

The intended result of this work is a core binary that largely *orchestrates* the work in STORAGE and COMPUTE.
It is not arbitrarily scalable (the single-threaded `determine_timestamp` logic lies at the core if nothing else), but a solid start.

#### Step 3: Too soon to tell

Work can continue beyond this point, for example sharding the orchestration work.
However, it seems premature to plan this work at this point.

### Operational goals

We expect Materialize Platform to evolve and improve, which means that we will need to turn it off and on again, repeatedly.

Each of the components that layers spin up should be able to be started, stopped, and restarted correctly.
This need not be efficient at first, e.g. a CLUSTER could simply be turned off and then restarted and reissued commands.
However, a long-running Kafka ingestion worker in STORAGE should be able to stop and restart correctly, without re-reading the entire Kafka topic.

We also want the orchestration to be able to stop and restart correctly, which means that the orchestration of each layer must have similar properties.
For example, we might expect:
* STORAGE durably records the definitions of sources it is maintaining.
* COMPUTE durably records the CLUSTERs that should be running and the views maintained on each.
* CONTROL durably records sufficient state to reconstruct whatever users expect (e.g. catalog contents, timeline timestamp).

Ideally, the orchestration could stop and restart without forcing the same of its orchestrated resources.
* STORAGE could continue to read sources and serve subscriptions.
* COMPUTE could continue to read subscriptions and produce outputs.
* CONTROL could continue to handle user requests, optimize things, assign timestamps.
