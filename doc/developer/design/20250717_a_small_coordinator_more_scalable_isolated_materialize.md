# A Small Coordinator for A More Scalable and Isolated Materialize

As part of the platform v2 working group, we were working towards making
Materialize more scalable and provide (physical) use-case isolation. We managed
to implement the required basic building blocks, a [distributed timestamp
oracle](20230921_distributed_ts_oracle.md), [a decoupled storage
controller](20240117_decoupled_storage_controller.md), and the [txn
system](20230705_v2_txn_management.md), but never got to assembling the whole
vision because priorities shifted.

This is a concrete proposal for working towards a "smaller" Coordinator, that
is, a Coordinator that is less involved in processing user requests. This will
lead to a more scalable _and_ more isolated system, without grander ambitions
of implementing full horizontal scalability and isolation of Materialize.

## The Problem

The Coordinator is a component of the ADAPTER layer that is sequentializing
most interactions and goings-on in Materialize through a single-threaded
command-processing loop (aka. the coordinator main loop or simply main loop).
Crucially, this includes processing user queries and controller (STORAGE and
COMPUTE) updates. There are two important consequences of that:

1. A limit to scalability: It is generally accepted that you can only do a
   second's worth of work in a second, so this puts an upper bound on the
   number of interactions we can process. For example, if processing a SELECT
   needed 10ms of time on the main loop that would impose a strict upper limit
   of 100 SELECTs/s, regardless of how much capacity other parts of the system
   have for processing.
2. Lack of use-case isolation: When an operation takes longer than expected on
   the main loop -- due to bugs, unexpected behavior, or other reasons -- it
   "steals" time from other operations that need time on the loop. One use-case
   that is using a lot of Materialize will make Materialize worse for other use
   cases.

We can see the first of these when running benchmarks. We have metrics about
how many commands the main loop is processing and how much time they're taking
(a histogram). And so we can see that when running a SELECT throughput
benchmark, the time spent processing messages is close to 1 second, so at the
theoretical limit of the architecture. More on this in a detailed section
below.

We can see the second consequence in action most acutely when there are bugs in
operations that are thought to be fast but take a long time. In such cases,
Materialize as a whole becomes unresponsive or "sluggish" for a customer.
Another way of seeing this in action is of course when one use-case is using
the environment a lot, say high SELECT throughput: this will also make the
system less responsive for other use cases/users of the system.

Both of these _might_ not obviously be problematic today, but we think we will
have customers soon for which our scalability limits become a blocker for
expansion and we do see a trickle of bugs where the lack of isolation makes it
so that Materialize becomes unresponsive for customers, for multiple seconds at
a time. This can shake confidence in Materialize, which would also be a blocker
for adoption or expansion. There is some recency bias to thinking how urgent
these bugs are, but here's the latest example, where we had to disable a
feature because it can block the coordinator main loop for 10s of seconds or
more: https://github.com/MaterializeInc/materialize/pull/33070.

## Success Criteria

We want to address both problems mentioned above, but scalability is easier to
quantify. Isolation is covered by the qualitative goals.

- When running throughput benchmarks, the metric showing time spent processing
  messages on the main loop must be nowhere near 1 second. The main loop must
  not be the bottleneck.
- The Coordinator is so small -- in terms of the types of commands and the
  complexity of each command -- that it is feasible to audit all commands and
  conclude that nothing can block the main loop unreasonably long.

## Out of Scope

- We want to do this work within the current singleton `environmentd` process.
  That is, we don't yet address horizontal scalability of the query/control
  layer.
- We don't want to improve throughput benchmark numbers, only remove the
  Coordinator as a bottleneck. Our work might increase throughput numbers or
  reveal similar bottlenecks in other parts of the system.

## Background

The Coordinator has had its current design since the early days of the
codebase. A single control loop that sequentializes all modifications of
durable state was put in place to provide Materialize's correctness guarantees,
consistency, strict serializability.

It had to sequence DDL, regular queries, the storage/compute controllers
changing state, everything. Putting it in Rust terms, all of the persistent or
ephemeral state was a `&mut`, that is it was not shareable and required that a
single loop mediate access to it. Timestamp selection (important for
consistency) was using in-memory state (backed by periodically synced disk
state), that was not shareable, not the distributed timestamp oracle we have
now.

Today, the distributed timestamp oracle provides correctness, and DDL has to
modify the catalog, which is backed by a persist shard. And that machinery
notices and handles concurrent modifications (with some caveats).

The platform v2 design doc on [a logical architecture for a scalable and
isolated Materialize](20231127_pv2_uci_logical_architecture.md) goes into more
detail and argues how the primitives that we have now suffice to provide
correctness. The arguments there serve as the basis for the proposed changes to
the Coordinator below.

> [!NOTE]
> The coordinator main loop runs inside an `async` task, but throughout this
> doc we use the term single-threaded. Whether it's an async task or thread is
> not important for this discussion.

### Interesting Components

We are interested in ADAPTER components and how they interact:

- _adapter frontend_: this is the code that terminates a `pgwire` connection
  from the user/client. Each connection is being run by an `async` task that
  sends commands to the Coordinator and sends responses from the Coordinator
  back to the client.
- Coordinator: the component that sits in between the frontend and the
  controllers and is responsible for durable environment state, including the
  catalog, and mediating access to it. The Coordinator talks to the controllers
  to affect things.
- Controller(s): the storage and compute controllers. For the purposes of this
  document the interesting facets are that we need to talk to the controllers
  to acquire read holds for a SELECT and that we need to talk to the compute
  controller for executing a SELECT. But anything else that is affecting
  clusters or collections also has to go through the controller(s).

### Small vs. Big Coordinator

We argue for a _Small Coordinator_ and retroactively call what we have now a
_Big Coordinator_. The Coordinator is big in terms of the types of commands it
supports, how complex ("big") those commands are, and how much time the
Coordinator has to spend on its main loop for processing those commands.
Currently, the frontend sends commands of the shape EXECUTE SELECT or EXECUTE
DDL. These are higher-level, complex commands. The alternative is a Small
Coordinator that supports a much reduced set of simpler commands: most of the
work would have to happen in other parts of the system and the Coordinator only
has too be involved when absolutely necessary. A good analogy might be CISC vs
RISC instruction sets, where CISC has fewer, more complex opcodes and RISC has
possibly more, but simpler opcodes.

### Staged Processing

To work around limited time on the coordinator main loop, the concept of
_staged processing_ was introduced. First for peeks (SELECT) but then for other
things. The Coordinator gathers required pieces for processing a peek on the
main loop, then fires off an async task to do the actual work. The task sends a
command back to the Coordinator when ready, which resumes processing on the
main loop, potentially firing off more stages.

This is a band aid because it doesn't fix the ultimate scalability problems
_and_ it makes the code more complicated and harder to understand. It makes it
harder to audit code running in the main loop.

## Benchmarks

We did not run comprehensive benchmarks; the purpose is to show how the
Coordinator behaves under sustained workload and how command processing becomes
the bottleneck.

The benchmark runs `SELECT` statements with concurrent clients (128
connections). On my machine, I get about 5000 qps. We see these metrics on the
rate of commands being processed (also known as "messages" in the code and
metrics):

<img src="./static/a_small_coordinator/select-metrics-message-count.png" alt="SELECT benchmark - message counts" width="50%">

Around 5000 we see interesting message types that correspond with our 5000 qps:

- `command-execute`: starts execution of the SELECT.
- `command-catalog_snapshot`: gets a snapshot of the catalog, needed for
  processing SELECT.
- `command-commit`: finalizes a single SELECT execution.
- `controller_ready(compute)`: the compute controller signaling that a peek
  result is ready and the Coordinator needs to act.

The top line is `peek_stage_ready`, which originates in the staged processing
machinery explained above. There are two async stages fired off for processing
each SELECT.

When we look at the metric showing total time spent processing commands on the
main loop, we are very near our 1 second theoretical maximum. When accounting
for overhead of the loop machinery, message channels, etc., we can see that
this is currently the bottleneck:

<img src="./static/a_small_coordinator/select-metrics-message-time-total.png" alt="SELECT benchmark - total time spend processing commands" width="50%">

## Proposal

We propose to work towards a Small Coordinator. We should start this work _now_
because of urgency, but do it incrementally to arrive at the goal of a Small
Coordinator. We will not outline a comprehensive step-by-step implementation
plan but instead provide examples of workflows and how we can move from big to
small, then provide a rough implementation plan for immediate next steps.

We should use a data-driven approach: use message-processing metrics to find
where we spend time on the Coordinator main loop, both at steady state and when
processing important or representative workloads. Then we tackle those usages
of the loop and associated commands. Additionally, we can let bugs or
observations of lack of isolation guide what other parts we need to address.

### Processing SELECTs

For SELECT, the bulk of the work is currently driven by the main loop. The
frontend sends an EXECUTE SELECT message and the Coordinator does sequencing,
fires off staged tasks, and talks to controllers. This diagram visualizes the
workflow for the current Big Coordinator. A lot of time is spent on the main
loop:

<img src="./static/a_small_coordinator/big-coord-select.png" alt="Big Coordinator - processing SELECT" width="50%">

We propose this approach for moving towards a Small Coordinator:

- Determine what bundles of data and access is needed for processing SELECT.
- Introduce interfaces/clients (or re-use existing ones) and small commands
  that allow the frontend to get them from the Coordinator.
- Move main driver code for executing SELECT to the frontend, using the new
  interfaces and commands to retrieve what it needs.

For SELECT, the required interfaces and commands are:

- `get_catalog_snapshot` -> `Catalog` (exists)
- `get_timestamp_oracle` -> `TimestampOracle` (exists)
- `get_controller_clients` -> `ComputeControllerClient` (roughly exists already
  after [PR: decoupled compute
  controller](https://github.com/MaterializeInc/materialize/pull/29559)),
  `StorageCollections` (exists, from [design: decoupled storage
  controller](20240117_decoupled_storage_controller.md))

We need the `Catalog` for resolving objects in the query, the `TimestampOracle`
for selecting a query timestamp, and the controllers for acquiring read holds
and actually setting off the peek.

The workflow after those changes looks like this. The work is "pushed out" from
the main loop to the frontend (which already has a task/thread per connection)
and the controller. Much less time is spent on the coordinator main loop:

<img src="./static/a_small_coordinator/small-coord-select.png" alt="Small Coordinator - processing SELECT" width="50%">

### Controller Processing

Before more recent changes motivated by [a logical architecture for a scalable
and isolated Materialize](20231127_pv2_uci_logical_architecture.md), every time
the controller(s) wanted to affect environment state (ephemeral or durable)
they needed to do so on the coordinator main loop. This has partially changed
for the storage controller with the implementation of [decoupled storage
controller](20240117_decoupled_storage_controller.md) where we factored out a
`StorageCollections`, which does its work off the main loop. And for the
compute controller with [PR: decoupled compute
controller](https://github.com/MaterializeInc/materialize/pull/29559).

But as we can see from the metrics during SELECT processing, there is still
_some_ work that the controller(s) need to do on the main loop and we should
aim at removing as much of that as possible.

The pattern before recent changes (and still now), is that the controllers
report as `ready`, when they need to do processing on the main loop and then
the Coordinator will invoke them. This is visualized here:

<img src="./static/a_small_coordinator/big-coord-controller.png" alt="Big Coordinator - controller processing" width="50%">

To move to a Small Coordinator, we need to change the storage controller to do
its work in its own task, similar to how we did for the compute controller in
[PR: decoupled compute
controller](https://github.com/MaterializeInc/materialize/pull/29559). With
that work fully realized, both for the storage controller and the remaining
compute controller moments a visualization of the workflow would look like
this:

<img src="./static/a_small_coordinator/small-coord-controller.png" alt="Small Coordinator - controller processing" width="50%">

### Implementation Plan

This will be a longer-running project, but as immediate next steps we suggest
changes that address the problems mentioned in the introduction. We start with
SELECT processing as the workload where we want to reduce command processing
time/count. Additionally, because they are low-hanging fruit, we should take
the rest of controller processing off the Coordinator main loop.

We propose these tasks which can be worked on concurrently:

1. Move SELECT processing to the frontend.
2. Remove remaining places where the compute controller needs processing on the
   main loop (follow-ups to [PR: decoupled compute
   controller](https://github.com/MaterializeInc/materialize/pull/29559)).
3. Move storage controller processing to a background task, similar to the
   compute controller (again, see [PR: decoupled compute
   controller](https://github.com/MaterializeInc/materialize/pull/29559)).

Then we reassess pressing issues and continue shrinking the Coordinator.

## A Note on Horizontal Scalability and Use-Case Isolation

This proposal does not aim at working on horizontal scalability of
`environmentd` but we think it is an incremental step towards that. Pushing
processing out of the Coordinator and into the frontend will make us put in
place the interfaces that are needed for processing queries and will make the
code more amenable to running in concurrent processes.

Taking the example of processing SELECTs from above, together with a good dose
of hand waving, we argue that none of the required clients/interfaces will need
a central coordinator: anyone with a persist client can read and write the
Catalog, the distributed timestamp oracle is a "thin client" of CRDB right now
and so can also be invoked from anywhere.

As a strawman sketch, we could now put one cluster-specific `environmentd` in
front of every cluster. The controllers running within are only responsible for
replicas of its cluster and it could serve peeks from its cluster. It could
also execute DDL and any other command that don't involve clusters, but not
queries that would require talking to another `environmentd`'s cluster
replicas.

One wrinkle with the above is that the Catalog Client/Coordinator code cannot
currently act on Catalog changes that it discovers from other writers, but that
is achievable with a good amount of elbow grease.

There are more thorough arguments for this in the already mentioned [a logical
architecture for a scalable and isolated
Materialize](20231127_pv2_uci_logical_architecture.md).


## Alternatives

An alternative is keeping the Big Coordinator and investing more into staged
command processing. This doesn't help because we cannot easily audit what is
and isn't blocking for a long time, and ultimately a single loop that
sequentializes will remain a bottleneck.

## Open questions

None, so far.
