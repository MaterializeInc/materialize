# A Small Coordinator for A More Scalable and Isolated Materialize

As part of the platform v2 working group, we were working towards making
Materialize more scalable and provide (physical) use-case isolation. We managed
to implement the required basic building blocks, a [distributed timestamp
oracle](doc/developer/design/20230921_distributed_ts_oracle.md), [a decoupled
storage
controller](doc/developer/design/20240117_decoupled_storage_controller.md), and
the [txn system](doc/developer/design/20230705_v2_txn_management.md), but never
got to assembling the whole vision because priorities shifted.

This is a concrete proposal for working towards a "smaller" Coordinator, that
is, a Coordinator that is less involved in processing user requests. This will
lead to a more scalable _and_ more isolated system, without grander ambitions
of implementing full horizontal scalability and isolation of Materialize.


logical pv2 design: [pv2 logical design](doc/developer/design/20231127_pv2_uci_logical_architecture.md)
Decoupled Compute Controller: https://github.com/MaterializeInc/materialize/pull/29559
decoupled storage controller: [decoupled storage controller](doc/developer/design/20240117_decoupled_storage_controller.md)

## The Problem

The Coordinator is a component of the ADAPTER layer that is sequentializing
most interactions and goings-on in Materialize through a single-threaded
command-processing loop (aka. the coordinator main loop or simply main loop).
Crucially, this includes processing user queries and controller (STORAGE and
COMPUTE) updates. There are two important consequences of that:

1. A limit to scalability: You can only do a second's worth of work in a
   second, so this puts an upper bound on the number of interactions we can
   process. For example, if processing a SELECT needed 10ms of time on the main
   loop that would impose a strict upper limit of 100 SELECTs/s, regardless of
   how much capacity other parts of the system have for processing.
2. Lack of use-case isolation: When, because of bugs or because of unexpected
   behavior or just because, an operation takes more time on the main loop than
   expected, this will "steal" time from other operations that are vying for
   time on the loop. One use-case that is using a lot of Materialize will make
   Materialize worse for other use cases.

We can see the first of these when running benchmarks. We have metrics about
how many commands the main loop is processing and how much time they're taking
(a histogram). And so we can see that when running a SELECT throughput
benchmark, the time spent processing messages is close to 1 second, so at the
theoretical limit of the architecture. More on this in a detailed section
below.

We can see the second consequence in action most acutely when there are bugs in
operations that are thought to be fast but take a long time. In such cases,
Materialize as a whole becomes unresponsive or "sluggish" for a customer.

We will explain this further below, but introduce the _Big Coordinator_ and
_Small Coordinator_ distinction here. What we currently have is a big
Coordinator: the frontend sends it commands of the shape PROCESS SELECT and the
Coordinator needs to spend a lot of time on it on the main loop. These are
higher-level, complex commands. The alternative is a Small Coordinator that
supports a much reduced set of simpler commands: most of the work must happen
in other parts of the system and the Coordinator only has too be involved when
absolutely necessary. A good analogy might be CISC vs RISC instruction sets,
where CISC has fewer, more complex opcodes and RISC has possibly more, but
simpler opcodes.

## Success Criteria

We want to address both of the problems mentioned above, but scalability is the
one where its easier to give more quantitative goals. Isolation is covered by
the more qualitative goals below.

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
  Coordinator as a bottleneck. Our work might increase throughput numbers, or
  it might show that there are similar bottlenecks in other parts of the
  system.

## Background

Explain how current Coordinator was like this for a long time. Was necessary
for providing the Materialize correctness experience: strict serializability.
It had to sequence DDL, user queries, everything. Timestamp selection was an
in-memory thing, not the distributed timestamp oracle we have now. Today, the
timestamp oracle provides correctness, and DDL goes through catalog
modifications which would realize concurrent modifications.

The platform v2 design doc on [a logical architecture for a scalable and
isolated
Materialize](doc/developer/design/20231127_pv2_uci_logical_architecture.md)
goes into more detail here and argues how the primitives that we have now
suffice to provide correctness.

### Interesting Components

For this doc, were are interested in ADAPTER components and how they interact:

- _adapter frontend_: this is the code that terminates a `pgwire` connection.
  Each connection is being run by an `async` task that sends commands to the
  Coordinator and sends responses from the Coordinator back to the client.
- Coordinator: the component that sits in between the frontend and the
  controllers and is responsible for durable environment state, including the
  catalog, and mediating access to it. The Coordinator talks to the controllers
  to affect things.
- Controller(s): the storage and compute controllers. For the purposes of this
  document the interesting facets are that we need to talk to the controllers
  to acquire read holds for a SELECT and that we need to talk to the compute
  controller for executing a SELECT.

### Staged Processing

To work around the limitation that time on the coordinator main loop is limited
the concept of _stages_, or _staged processing_ was introduced. First for peeks
(aka SELECT) but then also for other things. The idea is that the Coordinator
gathers the pieces that are required for a certain step in processing a peek,
on the main loop, then fires of an async task to do the actual work. The task
will then send a command back to the Coordinator when ready, which will then
resume processing on the main loop, potentially firing off more stages.

Ultimately, this is a band aid because it doesn't fix the ultimate scalability
problems _and_ it makes the code more complicated and harder to understand. It
makes it harder to audit code running in the tight main loop.

## "Benchmarks"

We did not run comprehensive benchmarks, the purpose here is to show how the
Coordinator behaves when there is a sustained workload and how command
processing becomes the bottleneck.

The benchmark runs simple `SELECT` statements with concurrent clients (128
connections). On my machine, I get about 5000 tps. And we see these metrics on
the rate of commands (also known as "messages" in the code and elsewhere):

<img src="./static/a_small_coordinator/select-metrics-message-count.png" alt="SELECT benchmark - message counts" width="50%">

Around 5000 we see a number of interesting message types, these correspond with
our 5000 tps. They are `controller:

- `command-execute`: this is the command that starts execution of the SELECT.
- `catalog-snapshot_snapshot`: this is a command for getting a snapshot of the
  catalog, needed for processing SELECT.
- `command-commit`: this is the command that will be executed for finalizing a
  single SELECT execution.
- `controller_ready(compute)`: this is the compute controller signaling that a
  peek result is ready, and the the Coordinator needs to do something about it.

The top line is `peek_stage_ready`, which emanate from the staged processing
machinery explained above. We can see that there are two async stages that are
fired off for processing each SELECT.

When we look at the metric that shows the total time spend processing commands
on the main loop, we see that we are very near our 1 second theoretical
maximum. Especially when accounting for overhead of the loop machiner, message
channels, etc. we can say that this is currently the bottleneck:

<img src="./static/a_small_coordinator/select-metrics-message-time-total.png" alt="SELECT benchmark - total time spend processing commands" width="50%">

## Proposal

<img src="./static/a_small_coordinator/big-coord-select.png" alt="Big Coordinator - processing SELECT" width="50%">
<img src="./static/a_small_coordinator/small-coord-select.png" alt="Small Coordinator - processing SELECT" width="50%">

<img src="./static/a_small_coordinator/big-coord-controller.png" alt="Big Coordinator - controller processing" width="50%">
<img src="./static/a_small_coordinator/small-coord-controller.png" alt="Small Coordinator - controller processing" width="50%">

## Alternatives

An alternative is that we keep the big Coordinator and invest more into
"staging" all command processing. I don't think this helps because we cannot
audit easily what is and isn't blocking for a long time, and ultimately a
single loop that sequentializes will keep being a bottleneck.

## Open questions
