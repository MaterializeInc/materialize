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

We want to address both of the problems mentioned above, but scalability is the
one where its easier to give more quantitative goals. Isolation is covered by
the more qualitative goals.

- When running throughput benchmarks, the metric showing time spend processing
  messages on the main loop must be nowhere near 1 second. That is the main
  loop must not be the bottleneck.
- The Coordinator is so small, in terms of types of commands and the complexity
  of each command, that it is feasible to audit all of them and conclude that
  nothing can block the main loop unreasonably long.

## Out of Scope

- We want to do this work within the current singleton `environmentd` process.
  No aspirations beyond that. So no horizontal scalability of the Coordinator.
- We don't want to improve numbers in throughput benchmarks. Only remove the
  Coordinator as a bottleneck. Our work _might_ increase throughput numbers, or
  it might show that there are similar bottlenecks in other parts of the
  system.

## Background

The Coordinator has had its current design since the early days of the code
base. The fact that it is a single control loop that sequentializes all
modifications of durable state was necessary for providing the Materialize
correctness experience, our consistency guarantees, strict serializability.

It had to sequence DDL, regular queries, the storage/compute controllers
changing state, everything. Putting it in Rust terms, all of the persistent or
ephemeral state was a `&mut`, that is it was not shareable and required that a
single loop mediate access to it. Timestamp selection (important for
consistency) was using in-memory state (backed by periodically synced disk
state), that was not shareable, not the distributed timestamp oracle we have
now.

Today, the distributed timestamp oracle provides correctness, and DDL goes
through catalog modifications, which is backed by a persist shard. And the
machinery would notice and handle concurrent modifications.

The platform v2 design doc on [a logical architecture for a scalable and
isolated Materialize](20231127_pv2_uci_logical_architecture.md) goes into more
detail and argues how the primitives that we have now suffice to provide
correctness. The arguments there serve as the basis for the proposed changes to
the Coordinator below.

> [!NOTE]
> The coordinator main loop runs inside an `async` task, but throughout this
> doc we will also use the term single-threaded, or single thread. For the
> discussion it is not important whether it's an async task or a thread. It's
> an implementation detail.

### Interesting Components

For this design doc, we are interested in ADAPTER components and how they
interact:

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

In this document, we are arguing for a _Small Coordinator_ and we retroactively
call what we have right now a _Big Coordinator_. The Coordinator is big in
terms of the types of commands it supports, how complex ("big") those commands
are, and how much time the Coordinator has to spend on its main loop for
processing those commands. Currently, the frontend sends commands of the shape
EXECUTE SELECT or EXECUTE DDL. These are higher-level, complex commands. The
alternative is a Small Coordinator that supports a much reduced set of simpler
commands: most of the work would have to happen in other parts of the system
and the Coordinator only has too be involved when absolutely necessary. A good
analogy might be CISC vs RISC instruction sets, where CISC has fewer, more
complex opcodes and RISC has possibly more, but simpler opcodes.

### Staged Processing

To work around the limitation that time on the coordinator main loop is limited
the concept of _stages_, or _staged processing_ was introduced. First for peeks
(aka SELECT) but then also for other things. The idea is that the Coordinator
gathers the pieces that are required for a certain step in processing a peek,
on the main loop, then fires of an async task to do the actual work. The task
will then send a command back to the Coordinator when ready, which will then
resume processing on the main loop, potentially firing off more stages.

This is a band aid because it doesn't fix the ultimate scalability problems
_and_ it makes the code more complicated and harder to understand. It makes it
harder to audit code running in the main loop.

## Benchmarks

We did not run comprehensive benchmarks, the purpose here is to show how the
Coordinator behaves when there is a sustained workload and how command
processing becomes the bottleneck.

The benchmark runs `SELECT` statements with concurrent clients (128
connections). On my machine, I get about 5000 qps. And we see these metrics on
the rate of commands being processed (also known as "messages" in the code and
elsewhere):

<img src="./static/a_small_coordinator/select-metrics-message-count.png" alt="SELECT benchmark - message counts" width="50%">

Around 5000 we see a number of interesting message types, these correspond with
our 5000 qps. They are:

- `command-execute`: this is the command that starts execution of the SELECT.
- `command-catalog_snapshot`: this is a command for getting a snapshot of the
  catalog, needed for processing SELECT.
- `command-commit`: this is the command that will be executed for finalizing a
  single SELECT execution.
- `controller_ready(compute)`: this is the compute controller signaling that a
  peek result is ready, and the the Coordinator needs to do something about it.

The top line is `peek_stage_ready`, which originate in the staged processing
machinery explained above. We can see that there are two async stages that are
fired off for processing each SELECT.

When we look at the metric that shows the total time spend processing commands
on the main loop, we see that we are very near our 1 second theoretical
maximum. Especially when accounting for overhead of the loop machinery, message
channels, etc., we can say that this is currently the bottleneck:

<img src="./static/a_small_coordinator/select-metrics-message-time-total.png" alt="SELECT benchmark - total time spend processing commands" width="50%">

## Proposal

As is likely clear by now, we propose to work towards a Small Coordinator. We
propose to start that work _now_ because of the urgency, but do it
incrementally, so that over time we will arrive at the goal of a Small
Coordinator. We will not outline a comprehensive step-by-step implementation
plan but instead we will provide examples of workflows and how we can move from
big to small and then provide a rough implementation plan for immediate next
steps.

Overall, we should use a data-driven approach: we can use the
message-processing metrics to find where we spend time on the Coordinator main
loop, both at steady state and when processing certain important or
representative workloads. And then we tackle those usages of the loop and the
associated commands. Additionally, we can lean into recency bias and let bugs
or observations of lack of isolation guide what other parts we need to address.

### Processing SELECTs

For SELECT, the bulk of the work is currently driven by the main loop. The
frontend sends an EXECUTE SELECT message and the Coordinator then does the
sequencing, firing off of staged tasks, and talking to the controller(s). This
diagram visualizes the workflow for the current Big Coordinator. We can clearly
see that a lot of time is spent on the main loop:

<img src="./static/a_small_coordinator/big-coord-select.png" alt="Big Coordinator - processing SELECT" width="50%">

We propose this approach for moving towards a Small Coordinator:

- Determine what bundles of data and access is needed for processing SELECT.
- Introduce interfaces/clients (or re-use existing ones) and small commands
  that allow the frontend to get them from the Coordinator.
- Move main driver code for executing SELECT to the frontend. And it uses the
  new interfaces and commands to retrieve what it needs.

Concretely, we think for SELECT the required interfaces and commands are:

- `get_catalog_snapshot` -> `Catalog` (exists)
- `get_timestamp_oracle` -> `TimestampOracle` (exists)
- `get_controller_clients` -> `ComputeControllerClient` (roughly exists already
  after [PR: decoupled compute
  controller](https://github.com/MaterializeInc/materialize/pull/29559)),
  `StorageCollections` (exists, from [design: decoupled storage
  controller](20240117_decoupled_storage_controller.md))

The workflow after those changes will look like this. The work is "pushed out"
from the main loop to the frontend (which already has a task/thread per
connection), and the controller. Much less time is spent on the coordinator
main loop:

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

We think this will be a longer-running project, but as immediate next steps we
suggest changes that address the problems mentioned in the introduction. We
start with SELECT processing as the workload where we want to reduce command
processing time/count. Additionally, because they are low-hanging fruits, we
should take the rest of controller processing off the Coordinator main loop.

That is, we initially have these small-ish tasks which can be worked off
concurrently:

1. Move SELECT processing to the frontend.
2. Remove remaining places where the compute controller needs processing on the
   main loop. So follow-ups to [PR: decoupled compute
   controller](https://github.com/MaterializeInc/materialize/pull/29559).
3. Move storage controller processing to a background task, similar to how we
   did it already for the compute controller. Again, see [PR: decoupled compute
   controller](https://github.com/MaterializeInc/materialize/pull/29559).

And then we retake stock of what are the pressing issues and continue shrinking
the Coordinator.

## Alternatives

An alternative is that we keep the Big Coordinator and invest more into staged
command processing. I don't think this helps because we cannot audit easily
what is and isn't blocking for a long time, and ultimately a single loop that
sequentializes will keep being a bottleneck.

## Open questions

None, so far.
