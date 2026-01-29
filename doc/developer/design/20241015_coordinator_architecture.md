# Designing a faster coordinator

- Associated: (Insert list of associated epics, issues, or PRs)

<!--
The goal of a design document is to thoroughly discover problems and
examine potential solutions before moving into the delivery phase of
a project. In order to be ready to share, a design document must address
the questions in each of the following sections. Any additional content
is at the discretion of the author.

Note: Feel free to add or remove sections as needed. However, most design
docs should at least keep the suggested sections.
-->

## Problem

<!--
What is the user problem we want to solve?

The answer to this question should link to at least one open GitHub
issue describing the problem.
-->

The coordinator architecture shows its limits for QPS-bound workloads. We
serialize most interactions with Materialize on a single-threaded coordinator
main thread, which performs most steps from accepting a statement to
fulfillment, with only some parts offloaded to worker threads. This
architecture is a bottleneck for high QPS workloads, as the coordinator main
thread can become a bottleneck.

We observe both high average latency and long tail latency. We need to address
both, the first to achieve a meaningful QPS target, and the latter to offer QPS
guarantees.

In this document, we analyze what functions we need to perform on the
coordinator main thread to uphold the guarantees of the system, and in the
process determine parts that can be handled asynchronously by different
threads.

The entry point for queries into Materialize is the `pgwire` protocol. Each
client is handled by its own async task, which interacts with the coordinator
to fulfill its requests. Here, we're mostly interested on how Materialize
fulfills `SELECT` queries.

1. A session starts by sending a `Startup` message. This initializes state on
   the coordinator, including updating the connections table.
2. The task receives messages from the frontend, including a `Query` message.
   The task parses the raw query string and executes each contained statement
   sequentially (`one_query`).
   1. It declares a query, which binds it to a portal. Do do this, it needs a
      snapshot of the catalog to describe the statement, which involves a call
      to the coordinator.
   2. It executes the portal bound in the previous step, which sends a
      `Execute` instruction to the coordinator.
   3. Sends results to the client, and cleans up the portal.
3. Eventually, the client shuts down, and the task notifies the coordinator
   using a `Terminate` message. Similarly to the startup message, this requires
   a catalog transaction.

The coordinator performs the following planning and sequencing steps on select
queries, the entry point is `handle_execute`:
* It validates the portal, sets up logging, and passes control to
  `handle_execute_inner`.
* Checks that the transaction mode matches the statement. For selects, this is
  trivially true as selects can be implicit and explicit transactions.
* Next, it checks that the query is not DDL, which is true for selects.
* It resolves the statement, which requires immutable access to the catalog.
* It checks if the statement requires purification, which is false for selects.
* Finally, it plans the statement and hands the plan to `sequence_plan`.

For selects, none of the steps require exclusive access to the coordinator.

During sequencing, the coordinator performs roughly the following steps:
* Validate the plan, check for sufficient RBAC permissions.
* Jump to `sequence_peek`, which validates the query (`peek_validate`) and then
  passes control to `sequence_staged` using a `PeekStage` object.

From this point on, the processing advances in steps with the option of
spawning tasks to advance. Of the steps, only some require coordinator access:
* `LinearizeTimestamps` spawns a task to query the time stamp oracle if
  required by the isolation level. For serializable isolation, this should
  directly advance to `RealTimeRecency`.
* `RealTimeRecency` determines a query time stamp, which requires spawning a
  task if in strict serializable isolation.
* Next, `TimstampReadHold` acquires the read holds to fulfill the current
  request, if the transaction doesn't already have them. This step requires
  mutable access to the system state.
* The next step is `PeekStageOptimize`, spawning a task to optimize the query.
* The last step is `Finish`, which handles various cases on how to fulfill a
  peek. It can install a dataflow if needed, and can instruct the compute
  controller to peek a target. It provides a channel to send results directly
  to the client task. It requires mutable access as we store information about
  the peek in various places.


The problem is the execute portion of handling queries. The coordinator is
responsible for planning, sequencing, and executing a query, and while
individual parts can be offloaded, it needs to advance the state machine for
handling queries.

This is inefficient for several reasons:
* Not all statements require exclusive access to the coordinator state, but by
  forcing a common interface, we cannot separate DML statements and such that
  don't require transactions from more complicated ones.
* Tracing requires somewhere 5-20us per invocation, e.g., constructing spans or
  instrumenting function calls and closures. The coordinator creates a span for
  each message it processes.
* For some of its tasks, the coordinator blocks on I/O, specifically to
  communicate with durable catalog. This can block the coordinator for tens of
  milliseconds at a time, spiking to hundreds of milliseconds. During this
  time, no other request can be processed.
* We're limited by the lack of CPU. Offloading parts of the query processing
  pipeline only improves overall latency as long as there are space CPU
  resources to handle the offload.

## Success criteria

<!--
What does a solution to this problem need to accomplish in order to
be successful?

The criteria should help us verify that a proposed solution would solve
our problem without naming a specific solution. Instead, focus on the
outcomes we hope result from this work. Feel free to list both qualitative
and quantitative measurements.
-->

Success should be scoped narrowly: We're able to handle a `SELECT`-only
workload with little interaction with the coordinator, which frees the
coordinator to do what it absolutely needs to do. It should result in a lower
average latency by improving the hot code path, and a lower tail latency by
reducing interference with other tasks handled by the coordinator.

A straw man's proposal would be to handle 10k QPS at a 10^-3 latency (p99.9) of
50ms and 10^-4 latency (p99.99) of 200ms. I made these numbers up, and they
should merely be used as a guiding principle.

This leaves us with an average blocking request processing time of less than
100us per request. For the system to be stable, it should probably be less than
50us to leave headroom for long-running operations and temporary workload
variations (noise).

## Out of scope

<!--
What does a solution to this problem not need to address in order to be
successful?

It's important to be clear about what parts of a problem we won't be solving
and why. This leads to crisper designs, and it aids in focusing the reviewer.
-->

## Solution proposal

<!--
What is your preferred solution, and why have you chosen it over the
alternatives? Start this section with a brief, high-level summary.

This is your opportunity to clearly communicate your chosen design. For any
design document, the appropriate level of technical details depends both on
the target reviewers and the nature of the design that is being proposed.
A good rule of thumb is that you should strive for the minimum level of
detail that fully communicates the proposal to your reviewers. If you're
unsure, reach out to your manager for help.

Remember to document any dependencies that may need to break or change as a
result of this work.
-->

We're running out of options to achieve the target outlined in [Success
criteria] by just optimizing the current architecture. Instead, we need to
adjust the architecture to be more cautiously separating parts of request
processing that need to be serialized by the coordinator versus these parts
that can be processed on any thread.

Here, we propose approaches that could yield a reduction in average and tail
latency.

### Planning and sequencing on the client task

Instead of sequencing queries on the coordinator thread, we use the local copy
of the catalog to plan and sequence the query locally. We need to:
* Obtain a read time stamp and acquire read holds,
* Update supporting state around peeks.

This looks like a significant task that requires intimate understanding of how
query processing works, but has the chance to yield significant improvements.

### Efficient use of prepared statements

Materializes simply handles prepared statements as a safe form of string
interpolation, but doesn't use the opportunity to avoid re-doing some work. We
could reduce the average query latency by a split optimizer that first
optimizes the prepared statements without bound values, and then only does a
fast pass optimization once values are bound.

This might have obvious limits where different values lead to differently
optimized plans, so some care needs to be taken here.

### Caching optimized plans

Some workloads might show repeated submissions of the same query, or one that
is very similar to a past query. (Determining similarity is probably a too hard
problem to solve.) We could maintain a cache of recent queries, invalidated
with catalog changes, to avoid re-optimizing the same query if the state of the
world didn't sufficiently change to make a different optimization decision.

### Caching results

A similar approach would be to cache results of queries. The benefit here would
be to avoid rendering a dataflow for slow-path queries, or a network round-trip
to the replica for fast-path queries.

### Batching of requests

When interacting with expensive parts of the query processing pipeline, it
might make sense to batch several requests together and work on them
concurrently. For example, the timestamp oracle could fulfill many requests in
one operation versus one-by-one.

## Minimal viable prototype

<!--
Build and share the minimal viable version of your project to validate the
design, value, and user experience. Depending on the project, your prototype
might look like:

- A Figma wireframe, or fuller prototype
- SQL syntax that isn't actually attached to anything on the backend
- A hacky but working live demo of a solution running on your laptop or in a
  staging environment

The best prototypes will be validated by Materialize team members as well
as prospects and customers. If you want help getting your prototype in front
of external folks, reach out to the Product team in #product.

This step is crucial for de-risking the design as early as possible and a
prototype is required in most cases. In _some_ cases it can be beneficial to
get eyes on the initial proposal without a prototype. If you think that
there is a good reason for skipping or delaying the prototype, please
explicitly mention it in this section and provide details on why you'd
like to skip or delay it.
-->

## Alternatives

<!--
What other solutions were considered, and why weren't they chosen?

This is your chance to demonstrate that you've fully discovered the problem.
Alternative solutions can come from many places, like: you or your Materialize
team members, our customers, our prospects, academic research, prior art, or
competitive research. One of our company values is to "do the reading" and
to "write things down." This is your opportunity to demonstrate both!
-->

## Open questions

<!--
What is left unaddressed by this design document that needs to be
closed out?

When a design document is authored and shared, there might still be
open questions that need to be explored. Through the design document
process, you are responsible for getting answers to these open
questions. All open questions should be answered by the time a design
document is merged.
-->
