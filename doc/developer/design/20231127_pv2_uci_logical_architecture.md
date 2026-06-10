# Platform v2: Logical Architecture of a Fast/Scalable/Isolated Query/Control Layer

## Context

As part of the platform v2 work (specifically use-case isolation) we want to
develop a scalable and isolated query/control layer that is made up of multiple
processes that interact with distributed primitives at the moments where
coordination is required.

The query/control layer is the point where different parts come together. For
our purposes, it comprises the Adapter, which is responsible for translating
user queries into commands for other parts and the Controllers, which are
responsible for driving around clusters and what runs on them. A central
component here is the Coordinator, which mediates between the other components
and owns the durable state of an environment and ensures _correctness_
(roughly, _strict serializable_).

With our current architecture, ensuring correctness requires that the
Coordinator employ a "single-threaded" event loop to sequentialize, among other
things, processing of all user queries and their responses. This is a blocker
for having a scalable and isolated query/control layer: all workloads are
competing for execution time on the single event loop.

We talk about threads, tasks, and loops below, but those should be understood
more as logical concepts, rather than physical implementation details. Though
there is of course a connection and we talk about the current physical
implementation. We _will_ propose a separate design doc that lays our the
post-platform-v2 physical architecture, having to do with processes and threads
and how they interact. The logical architecture described here is what will
enable that physical architecture and we focus on the arguments for why it does
provide correctness.

## Goals

- describe a logical design that allows for a more decoupled query-control layer

## Non-Goals

- specify the physical design of the post-platform-v2 architecture
- implement the full vision of use-case isolation: a multi-process serving
  layer

## Overview

In order to understand the proposal below we first need to understand what the
Coordinator (a central component of the query/control layer) is doing and why
it currently requires a singleton event loop for correctness. I will therefore
first explore the Coordinator in Background. Then I will give my proposed
logical architecture design. To close things out, I will describe a roadmap,
highlighting interesting incremental value that we can deliver along the way.

The choice of splitting the design into a logical and a physical architecture
is meant to mirror [architecture-db.md](./../platform/architecture-db.md), but
we are focusing on the query/control layer, also called ADAPTER in those earlier
design docs.

It is interesting to note that the proposed design is very much in line with
the design principles laid out a while ago in
[architecture-db.md](./../platform/architecture-db.md):

> The approach to remove scaling limits is "decoupling".
> Specifically, we want to remove as much artificial coupling as possible from the design of Materialize Platform.
> We will discuss the decoupling of both
> - the "logical architecture" (which modules are responsible for what) and
> - the "physical architecture" (where and how does code run to implement the above).
>
> [...]
>
> Decoupling is enabled primarily through the idea of a "definite collection".
> A definite collection is a durable, explicitly timestamped changelog of a data collection.
> Any two independent users of a definite collection can be sure they are seeing the same contents at any specific timestamp.
>
> Definite collections, and explicit timestamping generally, allow us to remove many instances of explicitly sequenced behavior.
> If reads and writes (and other potential actions) have explicit timestamps, these actions do not require sequencing to ensure their correct behavior.
> The removal of explicit sequencing allows us to decouple the physical behaviors of various components.

The value I provide here is that we take the design one step further, to double
down on those design ideas for a scalable and isolated serving layer, aka.
ADAPTER.

## Background: What is a Coordinator?

At a high level, the Coordinator is the component that glues all the other
components together. It is the sole holder of interesting resources, such as:
the controllers(s), which are used to drive around what COMPUTE and STORAGE do,
the Catalog (which I use as a proxy for "all persistent environment state"
here), and the Timestamp Oracle. The Coordinator mediates access to those
resources, by only allowing operations that flow through it's event loop to
access and modify those resources. Among other things, handling of client
queries is sequentialized by the main loop, as well as handling
responses/status updates from the controllers/clusters.

The messages/operations that pass through the event-loop fall into these
categories:

1. Internal Commands: These are enqueued as part of processing external
   commands, responses from controllers, or periodic "ticks". This includes
   Group Commit and batched linearization/delay of peeks.
2. Controller Responses: Controllers don't process responses (from clusters)
   when they get them. Instead they signal that they are ready to process them
   and wait "their turn" on the event loop to do the actual processing.
3. External Commands: Largely, queries that come in from a client, through the
   adapter layer. These will often cause one or more internal commands to be
   enqueued.

As we will see later, an important part of the current design is that these
messages have priorities. The coordinator has to work off all Internal Commands
(1.) before processing Controller Responses (2.), and so on for 2. and 3.

### Controller Commands

When we consider the commands that the Coordinator issues to the Controller(s),
we can differentiate two categories:

1. Deterministic Controller Commands: commands that derive from the current
   contents of the Catalog, or changes to it. These are command where the
   Coordinator first writes down changes to the Catalog and then synthesizes
   commands to the Controller that let it know of things that it has to do
   based on the Catalog changes. An example of this is `CREATE INDEX` where the
   fact that the index exists is first written down in the Catalog and then a
   corresponding `CreateDataflow` command is sent to the controller.
2. Ephemeral Controller Commands: Ephemeral commands that arise from external
   commands. This includes Peeks (`SELECT`), for COMPUTE, and writes to tables,
   for STORAGE. These are ephemeral because they don't derive from the Catalog
   and therefore would not survive a restart of `environmentd`.

For the first category, the Coordinator acts as a deterministic translator of
Catalog contents or changes to the Catalog to Controller commands.

### External Commands

We can differentiate external commands using similar categories:

1. DDL: Commands that modify the Catalog, which potentially can cause Controller
   Commands to be synthesized.
2. DML: Commands that modify non-Catalog state. Think largely `INSERT`-style
   queries that insert data into user collections.
3. DQL: Commands that only query data. Either from the Catalog or user
   collections/tables.

The mapping from `DDL`, etc. to the types of commands does not fit 100% but
it's a useful shorthand so we'll keep using it.

### Controller Responses

Clusters send these types of responses back to the controller:

1. Upper information: reports the write frontier for a collection (identified
   by a `GlobalId`).
2. Peek Responses
3. Subscribe Responses
4. Internal messages that are only interesting to the controller

Processing of all responses has to wait its turn on the single event loop. The
Controller will signal that it is ready to process responses and then wait. The
(compute) Controller will enqueue peek/subscribe responses to be processed on
the even loop, which will eventually send results back over pgwire.

### Sequencing for Correctness

The current design uses the sequencing of the single event loop, along with the
priorities of which messages to process, to uphold our correctness guarantees:
external commands and the internal commands their processing spawns are brought
into a total order, and Coordinator/Controller state can only be modified by one
operation at a time. This "trivially" makes for a linearizable system.

For example, think of a scenario where multiple concurrent clients/connections
create collections (`DDL`) and query or write to those collections (`DQL` and
`DML`). Say we have two clients, `c1` and `c2`, and they run these operations,
in order, that is they have a side-channel by which they know which earlier
operations succeeded and only start subsequent operations once the previous one
has been reported as successful:

1. `c1`: `CREATE TABLE foo [..]`
2. `c2`: `INSERT INTO foo VALUES [..]`
3. `c1`: `SELECT * FROM foo`

At a high level, processing of these queries goes like this (abridged):

1. External command `CREATE TABLE foo [..]` comes in
2. Command is processed: update Catalog, send commands to Controller
3. External Insert command comes in, this writes inserts into a queue
4. An internal GroupCommmit message is synthesized
5. Internal GroupCommit message is processed, writing to tables, at the end we
   spawn an internal GroupCommitApply message
6. The internal GroupCommitApply is processed, which spawns an Internal
   AdvanceTimelines message
7. The internal AdvanceTimelines is processed, which updates oracle timestamps
8. External SELECT/Peek command comes in
9. Peek is processed, looking at the Catalog state and timestamp oracle to
   figure out if the peek is valid and at what timestamp to peek
10. Processing a Peek spawns internal messages (for peek stages) that get
    worked of on the event loop
11. Eventually, a Peek Controller message is sent to the Controller
12. At some point the Controller receives a PeekResponse and signals that it is
    ready for processing
13. The Controller is allowed to process the PeekResponse, which enqueues a
    PeekResponse (for sending back over pgwire)
14. That response from the Controller is processed, causing results to be sent
    back to the client

Notice how working off commands in sequence (again, on the single event loop)
_and_ the rules about message priority make it so that users don't observe
anomalies: External message #8 cannot be processed before the internal messages
spawned by the `INSERT` are worked off. And the fact that Catalog state (and
other state, including talking to Controller(s)) can only be modified from one
"thread" make it so that the Peek will observe the correct state when it is
being processed.

Potential anomalies here would be:

- The `INSERT` statement returns an error saying `foo` doesn't exist.
- The `SELECT` statement returns an error saying `foo` doesn't exist.
- The `SELECT` statement returns but doesn't contain the previously inserted
  values.
- (Not from the example above) An index is queried (because it exists in the
  catalog), but the (compute) Controller doesn't (yet) know about it and
  returns an error.

Any design that aims at replacing the single event loop has to take these
things into consideration and prevent anomalies!

## Observations and Assumptions

These are the assumptions that went into the design proposed below. Some of
these derive from Background, above.

For now, this is a mostly un-ordered list but I will add more structure based
on feedback.

Observations, from the above section on Coordinator Background:

- Changes to the Catalog need sequencing. But an alternative mechanism, say
  compare-and-append would work for that.
- Currently, Controllers are not shareable or shared. The Coordinator holds a
  handle to them and operations that want to interact with them have to await
  their turn on the event loop. This doesn't have to be this way and it is safe
  for multiple actors to interact with Controller(s) if we use other means
  (other than the sequencing of the event loop) of ensuring consistency.
- Deterministic Controller Commands don't _have_ to be delivered by the
  Coordinator. Controller(s) could learn about them directly from listening to
  Catalog changes.
- Peeks and Inserts (and related DQL/DML queries) need to be
  consistent/linearizable with Catalog changes. _And_ they need to be
  consistent with Deterministic Controller Commands.
- Responses from controllers don't need sequencing. As soon as Peek/Subscribe
  responses are ready we can report them back to the client over pgwire.

Assumptions:

- Upper responses are _not_ important for correctness. Them advancing is what
  drives forward the `since`, in the absence of other read holds. They _are_
  used when trying to Peek at the "freshest possible" timestamp. But there are
  no guarantees about when they are a) sent back from the cluster to the
  controller, and b) when they are received by the controller. Meaning they are
  _not_ a concern in our consistency/linearizability story.

## Logical Architecture Design

We use terms and ideas from
[architecture-db.md](./../platform/architecture-db.md) and
[formalism.md](./../platform/formalism.md) so if you haven't read those recently
now's a good time to brush up on them.

This section can be seen as an extension of
[architecture-db.md](./../platform/architecture-db.md) but we drill down into
the innards of ADAPTER and slightly change how we drive around COMPUTE AND
STORAGE. At least we deviate from how they're driven around in the current
physical design of Coordinator.

> [!NOTE]
> Please keep in mind that this section is about the _logical_ architecture. It
> might seem verbose or over/under specified but is not necessarily indicative
> of a performant implementation of the design.

We introduce (or make explicit) two new components:

- CATALOG maintains the persistent state of an environment and allows changing
  and reading thereof. It is explicitly a pTVC that carries incremental changes
  to the catalog!
- TIMESTAMP ORACLE is a linearizable object that maintains read/write
  timestamps for timelines.

And we also change how some of the existing components work.

### TIMESTAMP ORACLE

The TIMESTAMP ORACLE can be considered a service that can be called from any
component that needs it, and it provides linearizable timestamps and/or records
writes as applied.

- `ReadTs(timeline) -> Timestamp`: returns the current read timestamp.

  This timestamp will be greater or equal to all prior values of
  `ApplyWrite()`, and strictly less than all subsequent values of `WriteTs()`.

- `WriteTs(timeline) -> Timestamp`: allocates and returns monotonically
  increasing write timestamps.

  This timestamp will be strictly greater than all prior values of `ReadTs()`
  and `WriteTs`.

- `ApplyWrite(timeline, timestamp)`: marks a write at time `timestamp` as
  completed.

  All subsequent values of `ReadTs()` will be greater or equal to the given
  write `timestamp`.

### CATALOG

CATALOG can also be considered a shared service whose methods can be called
from any component that needs it. And multiple actors can append data to it.

Changes to the catalog are versioned in the `"catalog"` timeline, which is
tracked separately from other timelines in TIMESTAMP ORACLE. Any successful
append of updates to the catalog implicitly does an `ApplyWrite("catalog",
new_upper)` on TIMESTAMP ORACLE.

- `CompareAndAppend(expected_upper, new_upper, updates) -> Result<(),
  AppendError>`: appends the given updates to the catalog _iff_ the expected
  upper matches its current upper.

- `SubscribeAt(timestamp) -> Subscribe`: returns a snapshot of the catalog at
  `timestamp`, followed by an ongoing stream of subsequent updates.

- `SnapshotAt(timestamp)`: returns an in-memory snapshot of the catalog at
  `timestamp`.

  A snapshot corresponds to our current in-memory representation of the catalog
  that the Coordinator holds. This can be (and will be, in practice!)
  implemented using `SubscribeAt` to incrementally keep an in-memory
  representation of the catalog up to date with changes. However, it is
  logically a different operation.

### The controllers, aka. STORAGE and COMPUTE

The controllers remain largely as they have been previously designed, but we
introduce a distinction between deterministic controller command and
ephemeral/query-like controller commands. See [Controller
Commands](#controller-commands) above.

The controllers can be seen as consuming a stream of deterministic controller
commands, in the newly introduced `"catalog"` timeline, and
`AdvanceCatalogFrontier` is downgrading its frontier.

Ephemeral/query-like commands gain a new `catalog_ts` parameter which tells the
controller that it can only process them once the input frontier (of catalog
updates) has sufficiently advanced.

These changes make it so that the controller knows up to which CATALOG
timestamp it has received commands and to ensure that peeks (or other
query-like operations) are only served once we know that controller state is
_at least_ up to date with the catalog state as of which the peek was
processed. Note that we say the Controller has to be _at least_ up to date with
the `catalog_ts`, it is okay for its catalog frontier to be beyond that. This
has the effect that in-flight peeks can end up in a situation where a
collection that we are trying to query doesn't exist anymore, and we therefore
have to report back an error. This is in line with current Materialize
behavior.

As we see below in the section on ADAPTER. We replace explicit sequencing by
decoupling using pTVCs and timestamps.

- `Peek(global_id, catalog_ts, ..)`: performs the peek, but _only_ once the catalog
  frontier has advanced far enough.

  It is important to note that the new `catalog_ts` parameter is different from
  an `as_of`/`ts`, which latter refers to the timeline/timestamp of the object
  being queried.

- `AdvanceCatalogFrontier(timestamp): tells the controller that it has seen
  commands corresponding to catalog changes up to `timestamp`.

(Most existing commands have been omitted for brevity!)

### ADAPTER

ADAPTER spawns tasks for handling client connections as they show up.

Additionally, it spawns one task per controller that runs a loop that
synthesizes commands for the controller based on differential CATALOG changes.
To do this, the task subscribes to CATALOG, filters on changes that are
relevant for the given controller, sends commands to the controller while
preserving the order in which they are observed in the stream of changes, and
synthesizes `AdvanceCatalogFrontier` commands when the catalog timestamp
advances. These commands are the commands that deterministically derive from
CATALOG state (see Background section, above).

When processing client requests, ADAPTER uses TIMESTAMP ORACLE to get the latest
read timestamp for CATALOG, then fetches a CATALOG snapshot as of at least that
timestamp, and continues to process the request using the catalog snapshot as
the source of truth. It's important to note that the catalog timestamp is not
correlated with the timestamp of data collections that are being queried. Most
of the time, the catalog read timestamp will not advance, it will only advance
when DDL cause the catalog to change.

When handling DDL-style client queries, ADAPTER uses TIMESTAMP ORACLE to get a
write timestamp and tries to append the required changes to the CATALOG. If
there is a conflict it has to get a new timestamp, get the latest CATALOG
snapshot, and try again. ADAPTER does _not_ explicitly instruct the controllers
to do anything that is required by these changes and it does _not_ wait for
controllers to act on those changes before returning to the client. Controllers
are expected to learn about these changes and act upon them from their
"private" command loop.

Client queries that spawn ephemeral controller commands (mostly DQL and
DML-style queries) are timestamped with the timestamp (in the catalog timeline)
as of which the query was processed. This ensure that these ephemeral
controller commands are consistent with what the controller must know as of
that timestamp.

## Physical Architecture Design

Previous design documents describe physical implementations of the newly required components:

 - [Differential CATALOG state](./20230806_durable_catalog_state.md)
 - [TIMESTAMP ORACLE as a service](./20230921_distributed_ts_oracle.md)

As a follow-up, we will propose a design doc for the post-platform-v2 physical,
distributed architecture.

## Roadmap for Use-Case Isolation and Platform v2 in General

These milestones gradually move us from our current architecture to a design
where the query/control layer (aka. the adapter, coordinator, and controllers)
is less and less tightly coupled and therefore supports better scalability and
isolation.

### Milestone 1: Laying the Foundations

This milestone is about developing the components that we will need to decouple
the components of the query/control layer. The current abstractions around the
catalog and timestamp oracle are not shareable between multiple actors.
Similarly, table writes cannot be done concurrently by multiple actors.

Epics:

- persist-txn: an abstraction that makes tables isolated and scalable
  - https://github.com/MaterializeInc/materialize/issues/22173
- Differential Catalog State (aka. Catalog pTVC): an abstraction that will allow multi-writer, multi-subscriber access to durable catalog state
  - https://github.com/MaterializeInc/materialize/issues/20953
  - https://github.com/MaterializeInc/materialize/issues/22392
- Distributed TimestampOracle: making the timetamp oracle distributed/shareable
  - https://github.com/MaterializeInc/materialize/issues/22029)

### Milestone 2: A Vertically Scalable Query/Control Layer

Once we have the new components developed in Milestone 1 we can start
incrementally pulling responsibilities out of the "single-threaded" Coordinator
event loop. Individual benefits of this are:

- Isolated Controller Control Loops: A single cluster (and it's controller),
  that is, for example, sending back a lot of responses no longer negatively
  interferes with the rest of the Coordinator responsibilities.
- Isolated Peeks: Processing of Peeks will no longer interfere with other
  responsibilities of the Coordinator, for example processing of controller
  responses or other (potentially blocking things). Controllers will send Peek
  Responses back to `pgwire` proactively, bypassing the event loop. And, most
  importantly other responsibilities will not negatively aspect processing
  Peeks.
- Isolated Writes: What we have above for Peeks, but for Writes!

At the end of this milestone, we will have an isolated and vertically scalable
query/control layer. The current implementation can not scale up by adding more
resources to the single process/machine because the single event loop is a
bottleneck. Once responsibilities are decouple we _can_ scale up vertically.

### Milestone 3: A Horizontally Scalable Query/Control Layer, aka. Full Physical Use-Case Isolation

Once the controllers are decoupled from the Coordinator via the Catalog pTVC, we
can start moving them out into their own processes or move them along-side the
cluster/replica processes. Similarly, we can start moving query processing to
other processes.

## Alternatives

TBD!

## Open questions

### Are those observations/assumptions around uppers and freshness correct?
